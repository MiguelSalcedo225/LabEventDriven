#!/bin/bash

# Delete by name and tag
docker rmi order-service:latest
docker rmi inventory-service:latest
docker rmi notification-service:latest
docker rmi sms-service:latest
docker rmi shipping-service:latest
docker rmi analytics-service:latest
docker rmi payment-service:latest
docker rmi email-service:latest
