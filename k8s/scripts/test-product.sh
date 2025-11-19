#!/usr/bin/env bash
# Script para crear productos en lote en Product Service
# Uso:
#   - Crear N productos:
#       ./test-products.sh bulk-create 10 "Laptop Test"
#   - Crear desde archivo JSON:
#       ./test-products.sh from-file products.json

set -euo pipefail

PRODUCT_URL=${PRODUCT_URL:-http://localhost:3002}
INVENTORY_URL=${INVENTORY_URL:-http://localhost:3005}
OUT_FILE=${OUT_FILE:-./products-created.json}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-60}
POLL_INTERVAL=${POLL_INTERVAL:-2}

usage(){
  echo "Uso:";
  echo "  bulk-create <count> <base-name> [price] [qty] [wait]";
  echo "  from-file <file.json> [wait]";
}

check_deps(){
  for cmd in curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "ERROR: falta '$cmd'" >&2
      exit 2
    fi
  done
}

create_product_one(){
  local name="$1"
  local price="$2"
  local description="$3"
  local quantity="$4"

  payload=$(jq -n --arg name "$name" --argjson price "$price" --arg description "$description" --argjson quantity "$quantity" '{name:$name, price:$price, description:$description, quantity:$quantity}')
  curl -sS -X POST "$PRODUCT_URL/products" -H 'Content-Type: application/json' -d "$payload" || true
}

bulk_create(){
  local count="$1"
  local base_name="$2"
  local price=${3:-9.99}
  local quantity=${4:-10}

  echo "Creando $count productos en $PRODUCT_URL";
  rm -f "$OUT_FILE"; echo "[]" > "$OUT_FILE";

  for i in $(seq 1 "$count"); do
    name="${base_name} #${i}"
    echo "- Creando: $name"
    resp=$(create_product_one "$name" "$price" "" "$quantity")
    jq ". + [ $resp ]" "$OUT_FILE" > "$OUT_FILE.tmp" && mv "$OUT_FILE.tmp" "$OUT_FILE"
  done
  echo "Productos creados guardados en $OUT_FILE"
}

from_file(){
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "No existe archivo: $file" >&2
    exit 3
  fi
  rm -f "$OUT_FILE"; echo "[]" > "$OUT_FILE";

  len=$(jq 'length' "$file")
  echo "Creando $len productos desde archivo..."

  for i in $(seq 0 $((len-1))); do
    obj=$(jq -c ".[$i]" "$file")
    name=$(echo "$obj" | jq -r '.name')
    price=$(echo "$obj" | jq -r '.price')
    description=$(echo "$obj" | jq -r '.description // ""')
    quantity=$(echo "$obj" | jq -r '.quantity // 0')

    echo "- Creando: $name"
    resp=$(create_product_one "$name" "$price" "$description" "$quantity")
    jq ". + [ $resp ]" "$OUT_FILE" > "$OUT_FILE.tmp" && mv "$OUT_FILE.tmp" "$OUT_FILE"
  done
  echo "Productos guardados en $OUT_FILE"
}

main(){
  check_deps

  cmd=${1:-}
  case "$cmd" in
    bulk-create)
      bulk_create "$2" "$3" "${4-}" "${5-}"
      ;;
    from-file)
      from_file "$2"
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"