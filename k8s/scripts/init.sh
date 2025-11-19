#!/bin/bash

set -e  # stop on first error

echo "Starting Minikube..."
minikube start --cpus=2 --memory=4096

echo "Setting Docker environment for Minikube..."
eval $(minikube -p minikube docker-env)

# Navigate to repo root
cd "$(dirname "$0")/../.."

# Build all images
docker build -t user-service:latest ./user-service
docker build -t product-service:latest ./product-service
docker build -t shipping-service:latest ./shipping-service
docker build -t analytics-service:latest ./analytics-service
docker build -t inventory-service:latest ./inventory-service
docker build -t payment-service:latest ./payment-service
docker build -t sms-service:latest ./sms-service

docker build -t email-service:latest ./email-service

echo "Building order-service..."
docker build -t order-service:latest ./order-service

# Verify images are in Minikube
echo "Verifying images..."
docker images | grep service
docker images | grep product-service
docker images | grep shipping-service
docker images | grep analytics-service