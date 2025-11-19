const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'analytics-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'analytics-service-group' });

// In-memory metrics store
const metrics = {
  totalOrders: 0,
  completedOrders: 0,
  totalRevenue: 0,
  paymentsSuccess: 0,
  paymentsRevenue: 0,
  ordersByStatus: {},
  revenueByDay: {},
  topProducts: {},
  recentEvents: []
};

const MAX_RECENT_EVENTS = 100;

// Process ORDER_COMPLETED events
const processOrderCompleted = (orderData) => {
  try {
    const order = orderData || {};
    
    metrics.totalOrders += 1;
    metrics.completedOrders += 1;
    
    const amount = parseFloat(order.total || order.Total || 0);
    metrics.totalRevenue += amount;
    
    // Track status
    const status = order.status || order.Status || 'completed';
    metrics.ordersByStatus[status] = (metrics.ordersByStatus[status] || 0) + 1;
    
    // Track revenue by day
    const timestamp = order.timestamp || order.Timestamp || new Date().toISOString();
    const day = new Date(timestamp).toISOString().slice(0, 10);
    metrics.revenueByDay[day] = (metrics.revenueByDay[day] || 0) + amount;
    
    // Track products from items
    const items = order.items || order.Items || [];
    items.forEach(item => {
      const itemId = item.itemId || item.ItemID || item.productId || 'unknown';
      const quantity = parseInt(item.quantity || item.Quantity || 1);
      const price = parseFloat(item.price || item.Price || 0);
      
      if (!metrics.topProducts[itemId]) {
        metrics.topProducts[itemId] = {
          itemId: itemId,
          name: item.name || item.Name || itemId,
          totalQuantity: 0,
          totalRevenue: 0
        };
      }
      
      metrics.topProducts[itemId].totalQuantity += quantity;
      metrics.topProducts[itemId].totalRevenue += price * quantity;
    });
    
    // Store recent event
    metrics.recentEvents.unshift({
      type: 'order.completed',
      orderId: order.orderId || order.OrderID,
      amount,
      timestamp,
      processedAt: new Date().toISOString()
    });
    
    if (metrics.recentEvents.length > MAX_RECENT_EVENTS) {
      metrics.recentEvents.pop();
    }
    
    console.log(`Processed order.completed: ${order.orderId || order.OrderID}, amount: ${amount}`);
  } catch (error) {
    console.error('Error processing order completed:', error);
  }
};

// Process PAYMENT_SUCCESS events
const processPaymentSuccess = (paymentData) => {
  try {
    const payment = paymentData || {};
    
    metrics.paymentsSuccess += 1;
    
    const amount = parseFloat(payment.amount || payment.Amount || 0);
    metrics.paymentsRevenue += amount;
    
    // Track revenue by day
    const timestamp = payment.timestamp || payment.Timestamp || new Date().toISOString();
    const day = new Date(timestamp).toISOString().slice(0, 10);
    metrics.revenueByDay[day] = (metrics.revenueByDay[day] || 0) + amount;
    
    // Store recent event
    metrics.recentEvents.unshift({
      type: 'payment.success',
      orderId: payment.orderId || payment.OrderID,
      amount,
      timestamp,
      processedAt: new Date().toISOString()
    });
    
    if (metrics.recentEvents.length > MAX_RECENT_EVENTS) {
      metrics.recentEvents.pop();
    }
    
    console.log(`Processed payment.success: ${payment.orderId || payment.OrderID}, amount: ${amount}`);
  } catch (error) {
    console.error('Error processing payment success:', error);
  }
};

// Connect to Kafka and start consuming
const connectConsumer = async () => {
  try {
    await consumer.connect();
    console.log('Analytics Service: Kafka consumer connected');
    
    // Subscribe to both topics
    await consumer.subscribe({ topic: 'order-events', fromBeginning: false });
    await consumer.subscribe({ topic: 'payment-events', fromBeginning: false });
    
    console.log('Subscribed to order-events and payment-events topics');
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const rawValue = message.value ? message.value.toString() : null;
          if (!rawValue) return;
          
          let payload;
          try {
            payload = JSON.parse(rawValue);
          } catch (e) {
            console.error('Failed to parse message as JSON:', e);
            return;
          }
          
          const eventType = payload.eventType || payload.EventType;
          const data = payload.data || payload.Data || payload;
          
          // Handle different event types
          if (eventType === 'ORDER_COMPLETED' || topic === 'order.completed') {
            processOrderCompleted(data);
          } else if (eventType === 'PAYMENT_SUCCESS' || topic === 'payment.success') {
            processPaymentSuccess(data);
          } else {
            console.log(`Received event: ${eventType} from topic: ${topic}`);
          }
        } catch (error) {
          console.error('Error processing message:', error);
        }
      }
    });
  } catch (error) {
    console.error('Analytics Service: Failed to connect consumer', error);
    setTimeout(connectConsumer, 5000);
  }
};

connectConsumer();

// HTTP Endpoints

// Get current metrics
app.get('/metrics', (req, res) => {
  res.json({
    service: 'analytics-service',
    timestamp: new Date().toISOString(),
    metrics: {
      totalOrders: metrics.totalOrders,
      completedOrders: metrics.completedOrders,
      totalRevenue: metrics.totalRevenue.toFixed(2),
      paymentsSuccess: metrics.paymentsSuccess,
      paymentsRevenue: metrics.paymentsRevenue.toFixed(2),
      ordersByStatus: metrics.ordersByStatus,
      revenueByDay: metrics.revenueByDay,
      topProducts: Object.values(metrics.topProducts)
        .sort((a, b) => b.totalRevenue - a.totalRevenue)
        .slice(0, 10),
      recentEventsCount: metrics.recentEvents.length
    }
  });
});

// Generate detailed report
app.get('/reports', (req, res) => {
  const topProducts = Object.values(metrics.topProducts)
    .sort((a, b) => b.totalRevenue - a.totalRevenue)
    .slice(0, 10)
    .map(p => ({
      itemId: p.itemId,
      name: p.name,
      totalQuantity: p.totalQuantity,
      totalRevenue: parseFloat(p.totalRevenue.toFixed(2))
    }));
  
  const revenueByDay = Object.entries(metrics.revenueByDay)
    .sort((a, b) => b[0].localeCompare(a[0]))
    .slice(0, 30)
    .map(([date, revenue]) => ({
      date,
      revenue: parseFloat(revenue.toFixed(2))
    }));
  
  const report = {
    generatedAt: new Date().toISOString(),
    summary: {
      totalOrders: metrics.totalOrders,
      completedOrders: metrics.completedOrders,
      totalRevenue: parseFloat(metrics.totalRevenue.toFixed(2)),
      paymentsSuccess: metrics.paymentsSuccess,
      paymentsRevenue: parseFloat(metrics.paymentsRevenue.toFixed(2)),
      averageOrderValue: metrics.completedOrders > 0 
        ? parseFloat((metrics.totalRevenue / metrics.completedOrders).toFixed(2))
        : 0
    },
    ordersByStatus: metrics.ordersByStatus,
    topProducts,
    revenueByDay,
    recentEvents: metrics.recentEvents.slice(0, 20)
  };
  
  res.json(report);
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    service: 'analytics-service',
    eventsProcessed: metrics.recentEvents.length
  });
});

const PORT = 3010;
app.listen(PORT, () => {
  console.log(`Analytics Service running on port ${PORT}`);
});

// Graceful shutdown
const shutdown = async () => {
  console.log('Shutting down analytics service...');
  try {
    await consumer.disconnect();
    console.log('Kafka consumer disconnected');
  } catch (error) {
    console.error('Error disconnecting consumer:', error);
  }
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);