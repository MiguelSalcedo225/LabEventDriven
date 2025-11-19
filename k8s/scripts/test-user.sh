#!/usr/bin/env bash
# Script para crear usuarios en lote en User Service
# Uso:
#  - Crear N usuarios con nombres base:
#      ./k8s/scripts/test-create-users.sh bulk-create 10 "juan" example.com
#  - Crear desde archivo JSON (array de objetos con fields username,email,adress,phone):
#      ./k8s/scripts/test-create-users.sh from-file users.json
# Variables env:
#  - USER_URL (por defecto: http://localhost:3001)

set -euo pipefail

USER_URL=${USER_URL:-http://localhost:3001}
OUT_FILE=${OUT_FILE:-./users-created.json}

usage(){
  sed -n '1,120p' "$0" | sed -n '1,12p'
}

check_deps(){
  for cmd in curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "ERROR: falta comando '$cmd'. Instálalo (por ejemplo: sudo apt install -y curl jq)" >&2
      exit 2
    fi
  done
}

create_user_one(){
  local username="$1"
  local email="$2"
  local adress="$3"
  local phone="$4"

  payload=$(jq -n --arg username "$username" --arg email "$email" --arg adress "$adress" --arg phone "$phone" '{username:$username, email:$email, adress:$adress, phone:$phone}')
  resp=$(curl -sS -X POST "$USER_URL/users" -H 'Content-Type: application/json' -d "$payload") || true
  echo "$resp"
}

bulk_create(){
  local count="$1"
  local base_name="$2"
  local domain=${3:-example.com}

  echo "Creando $count usuarios en $USER_URL..."
  rm -f "$OUT_FILE"
  echo "[]" > "$OUT_FILE"

  for i in $(seq 1 "$count"); do
    username="${base_name}${i}"
    email="${username}@${domain}"
    echo "- Creando: $username <$email>"
    resp=$(create_user_one "$username" "$email" "" "")
    jq ". + [ $resp ]" "$OUT_FILE" > "$OUT_FILE.tmp" && mv "$OUT_FILE.tmp" "$OUT_FILE"
    id=$(echo "$resp" | jq -r '.user.userId // .userId // empty' 2>/dev/null || true)
    echo "  -> userId: ${id:-(no id)}"
  done
  echo "Usuarios creados almacenados en $OUT_FILE"
}

from_file(){
  local file="$1"
  if [ ! -f "$file" ]; then
    echo "Archivo no encontrado: $file" >&2
    exit 3
  fi
  rm -f "$OUT_FILE"
  echo "[]" > "$OUT_FILE"
  local len
  len=$(jq 'length' "$file")
  echo "Creando $len usuarios a partir de $file"
  for i in $(seq 0 $((len-1))); do
    obj=$(jq -c ".[$i]" "$file")
    username=$(echo "$obj" | jq -r '.username')
    email=$(echo "$obj" | jq -r '.email')
    adress=$(echo "$obj" | jq -r '.adress // ""')
    phone=$(echo "$obj" | jq -r '.phone // ""')
    echo "- Creando: $username <$email>"
    resp=$(create_user_one "$username" "$email" "$adress" "$phone")
    jq ". + [ $resp ]" "$OUT_FILE" > "$OUT_FILE.tmp" && mv "$OUT_FILE.tmp" "$OUT_FILE"
    id=$(echo "$resp" | jq -r '.user.userId // .userId // empty' 2>/dev/null || true)
    echo "  -> userId: ${id:-(no id)}"
  done
  echo "Usuarios creados almacenados en $OUT_FILE"
}

main(){
  if [ "${1-}" = "" ]; then
    usage
    exit 0
  fi
  check_deps

  cmd="$1"
  case "$cmd" in
    bulk-create)
      if [ -z "${2-}" ] || [ -z "${3-}" ]; then
        echo "Uso: $0 bulk-create <count> <base-name> [email-domain]" >&2
        exit 1
      fi
      bulk_create "$2" "$3" "${4-}"
      ;;
    from-file)
      if [ -z "${2-}" ]; then
        echo "Uso: $0 from-file <file.json>" >&2
        exit 1
      fi
      from_file "$2"
      ;;
    *)
      echo "Comando desconocido: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"