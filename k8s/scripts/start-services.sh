#!/bin/bash

set -e

# Navigate to script directory
cd "$(dirname "$0")"

echo "--- Creating Namespace ---"
kubectl apply -f ../services/namespace.yml

echo "--- Deploying Zookeeper ---"
kubectl apply -f ../services/zookeeper.yml
echo "Waiting for Zookeeper pod to be created..."
sleep 5
kubectl wait --for=condition=ready pod -l app=zookeeper -n microservices --timeout=120s

echo "--- Deploying Kafka ---"
kubectl apply -f ../services/kafka.yml
echo "Waiting for Kafka pod to be created..."
sleep 5
kubectl wait --for=condition=ready pod -l app=kafka -n microservices --timeout=180s

echo "--- Deploying User Service ---"
kubectl apply -f ../services/user-service.yml
echo "Waiting for User Service pod to be created..."
sleep 5
kubectl wait --for=condition=ready pod -l app=user-service -n microservices --timeout=180s

echo "--- Deploying Email Service ---"
kubectl apply -f ../services/email-service.yml

echo "--- Waiting for Email Service ---"
kubectl wait --for=condition=ready pod -l app=email-service -n microservices --timeout=180s

echo "--- Deploying Order Service ---"
kubectl apply -f ../services/order-service.yml
echo "Waiting for Order Service pod to be created..."
sleep 5
kubectl wait --for=condition=ready pod -l app=order-service -n microservices --timeout=180s

echo "--- Deploying Product Service ---"
kubectl apply -f ../services/product-service.yml

echo "--- Waiting for Product Service ---"
# FIX: Do not use 'pod --all'. Target the specific app label instead.
kubectl wait --for=condition=ready pod -l app=product-service -n microservices --timeout=180s

echo "--- Deploying SMS Service ---"
kubectl apply -f ../services/sms-service.yml
echo "--- Waiting for SMS Service ---"
kubectl wait --for=condition=ready pod -l app=sms-service -n microservices --timeout=180s

echo "--- Deploying Payment Service ---"
kubectl apply -f ../services/payment-service.yml

echo "--- Waiting for Payment Service ---"
# FIX: Do not use 'pod --all'. Target the specific app label instead.
kubectl wait --for=condition=ready pod -l app=payment-service -n microservices --timeout=180s

echo "--- Deploying Inventory Service ---"
kubectl apply -f ../services/inventory-service.yml

echo "--- Waiting for Inventory Service ---"
kubectl wait --for=condition=ready pod -l app=inventory-service -n microservices --timeout=180s

echo "--- Deploying Shipping Service ---"
kubectl apply -f ../services/shipping-service.yml

echo "--- Waiting for Shipping Service ---"
# FIX: Do not use 'pod --all'. Target the specific app label instead.
kubectl wait --for=condition=ready pod -l app=shipping-service -n microservices --timeout=180s

echo "--- Deploying Analytics Service ---"
kubectl apply -f ../services/analytics-service.yml

echo "--- Waiting for Analytics Service ---"
# FIX: Do not use 'pod --all'. Target the specific app label instead.
kubectl wait --for=condition=ready pod -l app=analytics-service -n microservices --timeout=180s

echo "--- All Resources ---"
kubectl get all -n microservices

echo "--- Port Forwarding ---"
 
kubectl port-forward -n microservices svc/user-service 3001:3001 &
kubectl port-forward -n microservices svc/product-service 3002:3002 &
kubectl port-forward -n microservices svc/sms-service 3008:3008 &
kubectl port-forward -n microservices svc/payment-service 3004:3004 &

echo "Deployment complete. User Service accessible at localhost:3001"
echo "Product Service accessible at localhost:3002"
echo "Payment Service accessible at localhost:3004"
kubectl port-forward -n microservices svc/order-service 3003:3003 &
kubectl port-forward -n microservices svc/inventory-service 3005:3005 &
kubectl port-forward -n microservices svc/shipping-service 3006:3006 &
kubectl port-forward -n microservices svc/analytics-service 3010:3010 &

echo "Deployment complete. User Service accessible at localhost:3001"
echo "Product Service accessible at localhost:3002"
echo "Shipping Service accessible at localhost:3006"
echo "Order Service accessible at localhost:3004"
echo "Inventory Service accessible at localhost:3005"
echo "Analytics Service accessible at localhost:3010"
