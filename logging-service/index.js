const express = require('express');
const { Kafka } = require('kafkajs');

const SERVICE = process.env.SERVICE_NAME || 'logging-service';
const PORT = parseInt(process.env.PORT || '3009', 10);
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';

const app = express();
app.use(express.json());

const kafka = new Kafka({ clientId: SERVICE, brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: `${SERVICE}-group` });


const MAX_EVENTS = 2000;
const events = [];


const keyIndex = Object.create(null);


const sseClients = new Set();


const topicToService = {
  'user-events': 'user-service',
  'product-events': 'product-service',
  'inventory.updated': 'inventory-service',
  'email.sent': 'email-service',
  'sms.sent': 'sms-service',
  'analytics.updated': 'analytics-service',
  'order.created': 'order-service',
  'order.completed': 'order-service',
  'payment.success': 'payment-service',
  'payment.failed': 'payment-service',
  'shipment.sent': 'shipping-service',
  'shipment.delivered': 'shipping-service'
};


const topics = Object.keys(topicToService);

const pushEvent = (evt) => {

  evt.producedBy = topicToService[evt.topic] || 'unknown';
  evt.reactsTo = []; 
  evt.reactedBy = []; 


  if (evt.key) {
    const prev = keyIndex[evt.key] || [];
    for (const p of prev) {

      evt.reactsTo.push({ topic: p.topic, producedBy: p.producedBy, timestamp: p.timestamp });

      p.reactedBy = p.reactedBy || [];
      p.reactedBy.push({ topic: evt.topic, producedBy: evt.producedBy, timestamp: evt.timestamp });
    }
  }


  events.push(evt);
  if (evt.key) {
    keyIndex[evt.key] = keyIndex[evt.key] || [];
    keyIndex[evt.key].push(evt);
  }


  if (events.length > MAX_EVENTS) events.shift();


  const data = `data: ${JSON.stringify(evt)}\n\n`;
  for (const res of sseClients) {
    try { res.write(data); } catch (e) {}
  }
};

const connect = async () => {
  try {
    await consumer.connect();
    console.log(`${SERVICE}: consumer connected`);
    for (const t of topics) {
      await consumer.subscribe({ topic: t, fromBeginning: false });
      console.log(`${SERVICE}: subscribed to ${t}`);
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key && message.key.toString();
        const value = message.value && message.value.toString();
        const evt = {
          timestamp: new Date().toISOString(),
          topic,
          key: key || null,
          value: value || null
        };
        console.log(`${SERVICE}: LOG ${topic} - ${key} - ${value}`);
        pushEvent(evt);
      }
    });
  } catch (err) { console.error(`${SERVICE}: kafka connect error`, err); setTimeout(connect, 5000); }
};

connect();


app.get('/', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Logging Service - Event Pipeline</title>
  <style>
    body{font-family: Arial, Helvetica, sans-serif; background:#f7f9fc; color:#111}
    header{padding:16px; background:#2b6cb0; color:#fff}
    .container{max-width:1100px;margin:18px auto}
    .container { align-items: start; }
    .event{background:#fff;border-radius:6px;padding:12px;margin:8px 0;box-shadow:0 1px 3px rgba(0,0,0,.06)}
    .meta{font-size:12px;color:#666}
    .topic{font-weight:700;color:#2b6cb0}
    #events{height:70vh;overflow:auto;padding:8px}
    /* Right panel should not grow the page; limit height and add scrollbar */
    #services{height:70vh;overflow:auto;padding:8px}
    .badge{display:inline-block;padding:2px 8px;border-radius:12px;background:#edf2ff;color:#2b6cb0;font-weight:600;margin-right:8px}
  </style>
</head>
<body>
  <header><div class="container"><h2>Logging Service — Event Pipeline</h2></div></header>
  <div class="container" style="display:grid;grid-template-columns:2fr 1fr;gap:16px">
    <div>
      <p>Live stream of events. The page updates as events arrive.</p>
      <div id="events"></div>
    </div>
    <div>
      <p>Microservice view (produced / reacted / incoming)</p>
      <div id="services"></div>
    </div>
  </div>
  <script>
    const eventsEl = document.getElementById('events');
    const es = new EventSource('/stream');
    es.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data);
        const el = document.createElement('div');
        el.className = 'event';
        el.innerHTML = '<div style="display:flex;justify-content:space-between"><div><span class="badge">' + d.topic + '</span><span class="meta">' + d.timestamp + '</span></div><div class="meta">Key: ' + (d.key || '-') + ' </div></div><pre style="white-space:pre-wrap;margin-top:8px">' + escapeHtml(d.value) + '</pre>';
        eventsEl.insertBefore(el, eventsEl.firstChild);
        // also refresh services view (simple approach)
        renderServices();
      } catch (err) { console.error('parse err', err); }
    };
    es.onerror = (err) => { console.error('EventSource error', err); };
    // helper
    function escapeHtml(str){ if(!str) return ''; return str.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
    // fetch recent events on load
    fetch('/events').then(r=>r.json()).then(list=>{
      list.reverse().forEach(d=>{
        const el = document.createElement('div');
        el.className = 'event';
        el.innerHTML = '<div style="display:flex;justify-content:space-between"><div><span class="badge">' + d.topic + '</span><span class="meta">' + d.timestamp + '</span></div><div class="meta">Key: ' + (d.key || '-') + ' </div></div><pre style="white-space:pre-wrap;margin-top:8px">' + escapeHtml(d.value) + '</pre>';
        eventsEl.appendChild(el);
      });
      renderServices();
    }).catch(()=>{});
    const servicesEl = document.getElementById('services');
    function renderServices(){
      fetch('/services').then(r=>r.json()).then(payload=>{
        servicesEl.innerHTML = '';
        const svcNames = payload.services;
        for (const s of svcNames){
          const info = payload.data[s] || { produced: [], reacted: [], incoming: [] };
          const panel = document.createElement('div');
          panel.className = 'event';
          panel.innerHTML = '<div style="display:flex;justify-content:space-between"><div style="font-weight:700">' + s + '</div><div class="meta">Produced: ' + info.produced.length + ' | Reacted: ' + info.reacted.length + ' | Incoming: ' + info.incoming.length + '</div></div>';
          // produced list
          const prodList = document.createElement('div');
          prodList.style.marginTop = '8px';
          info.produced.slice(-10).reverse().forEach(e=>{
            const it = document.createElement('div');
            it.className = 'meta';
            it.innerHTML = '<span class="badge">' + e.topic + '</span> ' + e.timestamp + ' <small>key:' + (e.key||'-') + '</small>';
            prodList.appendChild(it);
          });
          panel.appendChild(prodList);
          // incoming samples
          if (info.incoming && info.incoming.length){
            const inc = document.createElement('div');
            inc.style.marginTop = '8px';
            inc.innerHTML = '<div style="font-weight:600;margin-bottom:4px">Reactions (incoming)</div>';
            info.incoming.slice(-5).reverse().forEach(r=>{
              const it = document.createElement('div');
              it.className = 'meta';
              it.innerHTML = '<span class="badge">' + r.topic + '</span> by <strong>' + (r.reactedBy||'-') + '</strong> at ' + r.timestamp + ' <small>key:' + (r.key||'-') + '</small>';
              inc.appendChild(it);
            });
            panel.appendChild(inc);
          }
          servicesEl.appendChild(panel);
        }
      }).catch(()=>{});
    }
  </script>
</body>
</html>
  `);
});


app.get('/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders && res.flushHeaders();


  res.write(':ok\n\n');
  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
  });
});


app.get('/events', (req, res) => {
  res.json(events.slice(-200));
});


app.get('/services', (req, res) => {
  const services = new Set(Object.values(topicToService));
  for (const e of events) if (e.producedBy) services.add(e.producedBy);
  if (events.some(ev => ev.producedBy === 'unknown')) services.add('unknown');

  const svcData = {};
  for (const s of services) {
    svcData[s] = { produced: [], reacted: [], incoming: [] };
  }

  for (const e of events) {
    const svc = e.producedBy || 'unknown';
    svcData[svc] = svcData[svc] || { produced: [], reacted: [], incoming: [] };
    svcData[svc].produced.push(e);
    if (e.reactsTo && e.reactsTo.length) svcData[svc].reacted.push(e);
    if (e.reactsTo) {
      for (const r of e.reactsTo) {
        const origin = r.producedBy || 'unknown';
        svcData[origin] = svcData[origin] || { produced: [], reacted: [], incoming: [] };
        svcData[origin].incoming = svcData[origin].incoming || [];
        svcData[origin].incoming.push({ reactedBy: e.producedBy, topic: e.topic, timestamp: e.timestamp, key: e.key });
      }
    }
  }

  res.json({ services: Object.keys(svcData).sort(), data: svcData });
});

app.get('/health', (req, res) => res.json({ status: 'healthy', service: SERVICE }));

const server = app.listen(PORT, () => console.log(`${SERVICE} listening on ${PORT}`));

process.on('SIGTERM', async () => { try { await consumer.disconnect(); } catch (e) {} server.close(() => process.exit(0)); });