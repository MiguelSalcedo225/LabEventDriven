const express = require('express');
const { Kafka } = require('kafkajs');
const nodemailer = require('nodemailer');

const app = express();
const PORT = 3007;

const {
  KAFKA_BROKER,
  SERVICE_NAME,
  EMAIL_USER,
  EMAIL_PASS
} = process.env;

console.log("Configuración cargada:");
console.log("KAFKA_BROKER:", KAFKA_BROKER);
console.log("SERVICE_NAME:", SERVICE_NAME);
console.log("EMAIL_USER:", EMAIL_USER);


//  HEALTH CHECK
app.get('/health', (req, res) => {
  res.status(200).send("Email Service OK");
});

//  EMAIL TRANSPORTER
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: EMAIL_USER,
    pass: EMAIL_PASS
  }
});

//  KAFKA CONFIG
const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: "email-service-group" });

async function startKafkaConsumer() {
  try {
    console.log("Conectando consumidor de Kafka...");
    await consumer.connect();

    console.log("Suscribiendo a topics...");
    await consumer.subscribe({ topic: "order-events", fromBeginning: false });
    await consumer.subscribe({ topic: "shipment-events", fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const eventData = JSON.parse(message.value.toString());
        console.log(`📥 Evento recibido en ${topic}:`, eventData);

        // LÓGICA DE ENVÍO DE EMAIL
        if (eventData.email) {
          function escapeHtml(str) {
            return String(str)
              .replace(/&/g, '&amp;')
              .replace(/</g, '&lt;')
              .replace(/>/g, '&gt;')
              .replace(/\"/g, '&quot;')
              .replace(/'/g, '&#39;');
          }

          const prettyJson = JSON.stringify(eventData, null, 2);

          const mailOptions = {
            from: EMAIL_USER,
            to: eventData.email,
            subject: `Notificación: ${eventData.eventType}`,
            text: `Hola ${eventData.username || ''},\n\nSe ha generado el evento "${eventData.eventType}".\n\nResumen:\n${prettyJson}\n\nSaludos,\nEquipo LabEventDriven`,
            html: `
              <div style="font-family: Arial, Helvetica, sans-serif; color: #222; line-height:1.4;">
                <h2 style="color:#0b66c3; margin:0 0 8px 0">Notificación: ${escapeHtml(eventData.eventType || '')}</h2>
                <p style="margin:0 0 8px 0">Hola ${escapeHtml(eventData.username || 'cliente')},</p>
                <p style="margin:0 0 12px 0">Se ha generado el siguiente evento. Abajo encontrarás un resumen con los datos recibidos:</p>
                <pre style="background:#f6f8fa; padding:12px; border-radius:6px; overflow:auto; white-space:pre-wrap;">${escapeHtml(prettyJson)}</pre>
                <p style="color:#666; font-size:12px; margin:12px 0 0 0">Este es un mensaje automático — por favor no respondas a este correo.</p>
              </div>
            `.trim()
          };

          try {
            await transporter.sendMail(mailOptions);
            console.log("Email enviado a:", eventData.email);
          } catch (err) {
            console.error("Error enviando email:", err);
          }
        }
      }
    });

  } catch (err) {
    console.error("Error conectando a Kafka:", err);
  }
}

startKafkaConsumer();

// =======================
//  START SERVER
// =======================
app.listen(PORT, () => {
  console.log(`Email Service corriendo en puerto ${PORT}`);
});
