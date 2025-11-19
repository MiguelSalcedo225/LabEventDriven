#!/usr/bin/env bash
set -euo pipefail

# Prueba E2E para LabEventDriven (flujo de eventos)
# Requisitos: curl, jq
# Pasos:
# 1. Crear producto
# 2. Esperar que inventory consuma el evento
# 3. Crear usuario 'victoria' con email y teléfono especificados
# 4. Crear orden usando el producto creado
# 5. Forzar payment SUCCESS vía POST /payments
# 6. Crear shipping para generar evento de envío
# 7. Verificar que SMS y Analytics procesaron los eventos

PRODUCT_URL=${PRODUCT_URL:-http://localhost:3002}
USER_URL=${USER_URL:-http://localhost:3001}
INVENTORY_URL=${INVENTORY_URL:-http://localhost:3005}
ORDER_URL=${ORDER_URL:-http://localhost:3003}
PAYMENT_URL=${PAYMENT_URL:-http://localhost:3004}
SHIPPING_URL=${SHIPPING_URL:-http://localhost:3006}
SMS_URL=${SMS_URL:-http://localhost:3008}
ANALYTICS_URL=${ANALYTICS_URL:-http://localhost:3010}

WAIT_SHORT=${WAIT_SHORT:-2}
WAIT_MED=${WAIT_MED:-4}

check_deps(){
  for cmd in curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "ERROR: falta '$cmd' - instálalo (ej. sudo apt install -y curl jq)" >&2
      exit 2
    fi
  done
}

echo "Iniciando prueba E2E de eventos..."
check_deps

echo "1) Creando producto de prueba en $PRODUCT_URL"
prod_payload=$(jq -n '{name: "Producto Test Vicky", price: 19.99, description: "Producto creado por test-e2e", quantity: 5}')
resp=$(curl -sS -X POST "$PRODUCT_URL/products" -H 'Content-Type: application/json' -d "$prod_payload")
productId=$(echo "$resp" | jq -r '.product.productId // .productId // empty')
productName=$(echo "$resp" | jq -r '.product.name // .name // empty')
productPrice=$(echo "$resp" | jq -r '.product.price // .price // 0')

if [ -z "$productId" ]; then
  echo "ERROR: no se obtuvo productId. Respuesta: $resp" >&2
  exit 3
fi

echo "  -> productId: $productId"

echo "2) Esperando $WAIT_SHORT segundos para que inventory procese el event"
sleep $WAIT_SHORT

echo "  -> Consultando inventario en $INVENTORY_URL"
inv=$(curl -sS "$INVENTORY_URL/inventory")
found=$(echo "$inv" | jq -r --arg pid "$productId" '(.inventory[$pid] // empty) | tostring' 2>/dev/null || true)
if [ -z "$found" ]; then
  echo "ERROR: inventory no contiene el productId $productId. Inventario completo:" >&2
  echo "$inv" | jq '.'
  exit 4
fi
echo "  -> Producto encontrado en inventory"

echo "3) Creando usuario 'victoria'"
user_payload=$(jq -n --arg u "victoria" --arg e "victoria.volveras@correounivalle.edu.co" --arg p "3106262271" '{username:$u, email:$e, adress:"Direccion de prueba", phone:$p}')
u_resp=$(curl -sS -X POST "$USER_URL/users" -H 'Content-Type: application/json' -d "$user_payload")
userId=$(echo "$u_resp" | jq -r '.user.userId // .userId // empty')
if [ -z "$userId" ]; then
  echo "ERROR: no se obtuvo userId. Respuesta: $u_resp" >&2
  exit 5
fi
echo "  -> userId: $userId"

echo "4) Creando orden para el usuario con el producto"
order_payload=$(jq -n --arg uid "$userId" --arg un "victoria" --arg em "victoria.volveras@correounivalle.edu.co" --arg ph "3106262271" --arg pid "$productId" --arg name "$productName" --arg price "$productPrice" '{userId:$uid, username:$un, email:$em, phone:$ph, items:[{itemId:$pid, name:$name, quantity:1, price:($price|tonumber)}]}')
o_resp=$(curl -sS -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' -d "$order_payload")
orderId=$(echo "$o_resp" | jq -r '.order.orderId // .orderId // empty')
if [ -z "$orderId" ]; then
  echo "ERROR: no se obtuvo orderId. Respuesta: $o_resp" >&2
  echo "Respuesta completa:"; echo "$o_resp" | jq '.' || true
  exit 6
fi
echo "  -> orderId: $orderId"

echo "5) Forzando payment SUCCESS vía $PAYMENT_URL/payments"
pay_payload=$(jq -n --arg oid "$orderId" --argjson amt $(jq -n --arg v "$productPrice" '$v|tonumber') '{orderId:$oid, amount:$amt, status:"SUCCESS"}')
p_resp=$(curl -sS -X POST "$PAYMENT_URL/payments" -H 'Content-Type: application/json' -d "$pay_payload")
echo "  -> payment response: $(echo "$p_resp" | jq -c '.')"

echo "Esperando $WAIT_MED segundos para que order-service procese payment y publique ORDER_COMPLETED"
sleep $WAIT_MED

echo "6) Creando shipping para generar evento de envío"
ship_payload=$(jq -n --arg uid "$userId" --arg addr "Calle Test 123" --arg city "Cali" --arg pcode "760001" --arg country "CO" '{userId:$uid, address:$addr, city:$city, postalCode:$pcode, country:$country}')
s_resp=$(curl -sS -X POST "$SHIPPING_URL/shipping" -H 'Content-Type: application/json' -d "$ship_payload")
echo "  -> shipping response: $(echo "$s_resp" | jq -c '.')"

echo "Esperando $WAIT_SHORT segundos para que sms/email/analytics consuman eventos"
sleep $WAIT_SHORT

echo "7) Verificando SMS service (/sent-sms)"
sms_list=$(curl -sS "$SMS_URL/sent-sms")
echo "$sms_list" | jq '.' || true

echo "8) Verificando Analytics (/metrics)"
metrics=$(curl -sS "$ANALYTICS_URL/metrics" || true)
echo "$metrics" | jq '.' || true

echo "Resumen de verificación (buscar valores esperados):"
echo "- orderId: $orderId"
echo "- productId: $productId"
echo "- userId: $userId"

echo "Prueba E2E completada. Revisa salidas anteriores para confirmar que los eventos fluyeron correctamente."

exit 0
