const express = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const payments = [];

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'payment-service-group' });

// Connect to Kafka Producer
const connectProducer = async () => {
  try {
    await producer.connect();
    console.log('Payment Service: Kafka producer connected');
  } catch (error) {
    console.error('Payment Service: Failed to connect producer', error);
    setTimeout(connectProducer, 5000);
  }
};

// Connect to Kafka Consumer
const connectConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'order-events', fromBeginning: false });
    console.log('Payment Service: Kafka consumer connected and subscribed to order-events');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          console.log(`Payment Service: Received event - ${event.eventType}`);

          if (event.eventType === 'ORDER_CREATED') {
            await processPayment(event.data);
          }
        } catch (error) {
          console.error('Payment Service: Error processing message', error);
        }
      }
    });
  } catch (error) {
    console.error('Payment Service: Failed to connect consumer', error);
    setTimeout(connectConsumer, 5000);
  }
};

// Process payment logic
const processPayment = async (orderData) => {
  try {
    const randomUuid = uuidv4();
    
    // Simulate payment processing (90% success rate)
    const isSuccess = Math.random() > 0.1;
    
    const payment = {
      paymentId: `PAY-${randomUuid}`,
      orderId: orderData.orderId,
      amount: orderData.totalAmount ?? orderData.amount,
      status: isSuccess ? 'SUCCESS' : 'FAILED',
      timestamp: new Date().toISOString()
    };

    payments.push(payment);

    // Publish payment event to Kafka
    const eventType = isSuccess ? 'PAYMENT_SUCCESS' : 'PAYMENT_FAILED';
    await producer.send({
      topic: 'payment-events',
      messages: [{
        key: payment.paymentId,
        value: JSON.stringify({
          eventType: eventType,
          data: payment
        })
      }]
    });

    console.log(`Payment processed: ${payment.paymentId} - Status: ${payment.status}`);
  } catch (error) {
    console.error('Error processing payment:', error);
  }
};

// Manual payment endpoint
app.post('/payments', async (req, res) => {
  try {
    const randomUuid = uuidv4();
    
    const payment = {
      paymentId: `PAY-${randomUuid}`,
      orderId: req.body.orderId,
      amount: req.body.amount,
      status: req.body.status || 'SUCCESS',
      timestamp: new Date().toISOString()
    };

    // Publish payment event to Kafka
    const eventType = payment.status === 'SUCCESS' ? 'PAYMENT_SUCCESS' : 'PAYMENT_FAILED';
    await producer.send({
      topic: 'payment-events',
      messages: [{
        key: payment.paymentId,
        value: JSON.stringify({
          eventType: eventType,
          data: payment
        })
      }]
    });

    payments.push(payment);

    console.log(`Payment created manually: ${payment.paymentId}`);
    res.status(201).json({ message: 'Payment created', payment });
  } catch (error) {
    console.error('Error creating payment:', error);
    res.status(500).json({ error: 'Failed to create payment' });
  }
});

// Get payments endpoint
app.get('/payments', (req, res) => {
  res.json({ 
    message: 'Payments endpoint', 
    service: 'payment-service',
    payments: payments 
  });
});

// Get specific payment by ID
app.get('/payments/:paymentId', (req, res) => {
  const payment = payments.find(p => p.paymentId === req.params.paymentId);
  if (payment) {
    res.json({ payment });
  } else {
    res.status(404).json({ error: 'Payment not found' });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'payment-service' });
});

const PORT = process.env.PAYMENT_PORT || 3004;
app.listen(PORT, () => {
  console.log(`Payment Service running on port ${PORT}`);
});

// Initialize Kafka connections
connectProducer();
connectConsumer();

// Graceful shutdown
process.on('SIGTERM', async () => {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
});
