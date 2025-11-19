package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Order represents an order in the system
type Order struct {
	OrderID   string      `json:"orderId"`
	UserID    string      `json:"userId"`
	UserName  string      `json:"username"`
	Email     string      `json:"email"`
	Phone     string      `json:"phone"`
	Items     []OrderItem `json:"items"`
	Total     float64     `json:"total"`
	Status    string      `json:"status"`
	Timestamp string      `json:"timestamp"`
}

// OrderItem represents an item in an order
type OrderItem struct {
	ItemID   string  `json:"itemId"`
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// KafkaEvent represents a generic Kafka event
type KafkaEvent struct {
	EventType string      `json:"eventType"`
	Data      interface{} `json:"data"`
}

// PaymentEvent represents a payment event from payment service
type PaymentEvent struct {
	OrderID   string `json:"orderId"`
	Amount    float64 `json:"amount"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

var (
	orders      = make(map[string]*Order)
	ordersMutex sync.RWMutex
	kafkaBroker string
	writer      *kafka.Writer
	reader      *kafka.Reader
)

func init() {
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
}

func main() {
	// Initialize Kafka producer
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "order-events",
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Initialize Kafka consumer
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBroker},
		Topic:     "payment-events",
		GroupID:   "order-group",
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		MaxWait:   1 * time.Second,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	// Start consuming payment events
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumePaymentEvents(ctx)

	// Setup HTTP routes
	http.HandleFunc("/orders", handleOrders)
	http.HandleFunc("/health", handleHealth)

	// Start HTTP server
	srv := &http.Server{
		Addr:    ":3003",
		Handler: http.DefaultServeMux,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down order service...")
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error during shutdown: %v\n", err)
		}
	}()

	log.Printf("Order Service running on port 3003")
	log.Printf("Connected to Kafka broker: %s", kafkaBroker)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func handleOrders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		createOrder(w, r)
	case http.MethodGet:
		getOrders(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID   string      `json:"userId"`
		UserName string      `json:"username"`
		Email    string      `json:"email"`
		Phone    string      `json:"phone"`
		Items    []OrderItem `json:"items"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.UserID == "" || len(req.Items) == 0 {
		http.Error(w, "userId and items are required", http.StatusBadRequest)
		return
	}

	if req.Email == "" || req.UserName == "" || req.Phone == "" {
		http.Error(w, "username, email and phone are required", http.StatusBadRequest)
		return
	}

	// Calculate total
	var total float64
	for _, item := range req.Items {
		total += item.Price * float64(item.Quantity)
	}

	// Create order
	order := &Order{
		OrderID:   fmt.Sprintf("ORD-%s", uuid.New().String()),
		UserID:    req.UserID,
		UserName:  req.UserName,
		Email:     req.Email,
		Phone:     req.Phone,
		Items:     req.Items,
		Total:     total,
		Status:    "pending",
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// Store order
	ordersMutex.Lock()
	orders[order.OrderID] = order
	ordersMutex.Unlock()

	// Publish order.created event to Kafka
	event := KafkaEvent{
		EventType: "ORDER_CREATED",
		Data:      order,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("Error marshaling event: %v", err)
		http.Error(w, "Failed to create order", http.StatusInternalServerError)
		return
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(order.OrderID),
		Value: eventJSON,
	})

	if err != nil {
		log.Printf("Error publishing to Kafka: %v", err)
		http.Error(w, "Failed to publish order event", http.StatusInternalServerError)
		return
	}

	log.Printf("Order created: %s", order.OrderID)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Order created",
		"order":   order,
	})
}

func getOrders(w http.ResponseWriter, r *http.Request) {
	ordersMutex.RLock()
	defer ordersMutex.RUnlock()

	orderList := make([]*Order, 0, len(orders))
	for _, order := range orders {
		orderList = append(orderList, order)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "orders endpoint",
		"service": "order-service",
		"orders":  orderList,
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "healthy",
		"service": "order-service",
	})
}

func consumePaymentEvents(ctx context.Context) {
	log.Println("Started consuming payment events...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping payment event consumer...")
			return
		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Printf("Error reading message: %v", err)
				continue
			}

			var event KafkaEvent
			if err := json.Unmarshal(m.Value, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}

			if event.EventType == "PAYMENT_SUCCESS" {
				handlePaymentSuccess(event.Data)
			}
		}
	}
}

func handlePaymentSuccess(data interface{}) {
	// Convert data to PaymentEvent
	dataJSON, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling payment data: %v", err)
		return
	}

	var paymentEvent PaymentEvent
	if err := json.Unmarshal(dataJSON, &paymentEvent); err != nil {
		log.Printf("Error unmarshaling payment event: %v", err)
		return
	}

	log.Printf("Processing payment success for order: %s", paymentEvent.OrderID)

	// Update order status
	ordersMutex.Lock()
	order, exists := orders[paymentEvent.OrderID]
	if !exists {
		ordersMutex.Unlock()
		log.Printf("Order not found: %s", paymentEvent.OrderID)
		return
	}
	order.Status = "completed"
	ordersMutex.Unlock()

	// Publish order.completed event
	completedEvent := KafkaEvent{
		EventType: "ORDER_COMPLETED",
		Data:      order,
	}

	eventJSON, err := json.Marshal(completedEvent)
	if err != nil {
		log.Printf("Error marshaling completed event: %v", err)
		return
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(order.OrderID),
		Value: eventJSON,
	})

	if err != nil {
		log.Printf("Error publishing order.completed event: %v", err)
		return
	}

	log.Printf("Order completed: %s", order.OrderID)
}
