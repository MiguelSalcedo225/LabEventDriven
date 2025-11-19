const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'sms-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'sms-group' });

// Almacenamiento de los mensajes enviados
const sentMessages = [];

// Caché para almacenar los números de teléfono
const userPhones = {};  

// Simulated SMS sender
const sendSms = async ({ phone, message, meta }) => {
  await new Promise(resolve => setTimeout(resolve, 200));
  const sent = {
    id: `${Date.now()}-${Math.floor(Math.random()*1000)}`,
    phone,
    message,
    meta,
    timestamp: new Date().toISOString()
  };
  console.log(`SMS sent to ${phone}: ${message}`);
  sentMessages.push(sent);
  return sent;
};

const connectConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'order-events', fromBeginning: false });
    await consumer.subscribe({ topic: 'shipping-events', fromBeginning: false });

    console.log('SMS Service: Kafka consumer connected');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          const eventType = event?.eventType;
          const data = event?.data;

          if (topic === 'order-events' && eventType === 'ORDER_COMPLETED') {

            const phone = data?.phone;
            const userId = data?.userId;
            const orderId = data?.orderId;

            // Se almacena el número de teléfono en el caché
            if (phone && userId) {
              userPhones[userId] = phone;
              console.log(`Cached phone for userId ${userId}: ${phone}`);
            }

            const msg = `Su pedido ${orderId} ha sido completado. ¡Gracias por su compra!`;
            if (phone) {
              await sendSms({ phone, message: msg, meta: { reason: 'order.completed', orderId } });
            } else {
              console.warn(`SMS Service: order.completed received but no phone for order ${orderId}`);
            }
          }

          if (topic === 'shipping-events' && eventType === 'SHIPPING_CREATED') {

            let phone = data?.phone;
            const userId = data?.userId;
            const shippingId = data?.shippingId;

            // Si shipping no envía el número de teléfono, se utiliza el caché
            if (!phone && userId && userPhones[userId]) {
              phone = userPhones[userId];
              console.log(`Recovered cached phone for userId ${userId}: ${phone}`);
            }

            const msg = `Su envío ${shippingId} ha sido creado y está en proceso.`;

            if (phone) {
              await sendSms({ phone, message: msg, meta: { reason: "shipping.created", shippingId } });
            } else {
              console.warn(`SMS Service: shipping.created received but no phone for shipment ${shippingId}`);
            }
          }

        } catch (err) {
          console.error('SMS Service: error processing message', err);
        }
      }
    });

  } catch (error) {
    console.error('SMS Service: Kafka consumer connection failed', error);
    setTimeout(connectConsumer, 5000);
  }
};

connectConsumer();

app.get('/sent-sms', (req, res) => {
  res.json({ service: 'sms-service', sent: sentMessages });
});

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'sms-service' });
}); 

const PORT = process.env.PORT || 3008;
app.listen(PORT, () => {
  console.log(`SMS Service running on port ${PORT}`);
});

process.on('SIGTERM', async () => {
  try {
    await consumer.disconnect();
  } catch (e) {
    console.warn('SMS Service: error during disconnect', e);
  }
  process.exit(0);
});