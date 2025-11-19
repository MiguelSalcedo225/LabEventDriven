**Project**

- **Description**: This repository provides an event-driven microservices demo using Apache Kafka, Docker Compose for local orchestration, and Kubernetes manifests + helper scripts for deploying the services into a cluster.

- **Goal**: Show how services produce/consume Kafka topics and how to add more microservices into this infra.

**Repository Structure**

- **`docker-compose.yml`**: Local composition of the full stack (Zookeeper, Kafka, and microservices). Used for local development and to create topics locally.
- **`k8s/`**: Kubernetes manifests and helper scripts used to create images and deploy services to a cluster.
  - **`k8s/services/`**: Kubernetes YAML files. Current files: `kafka.yml`, `zookeeper.yml`, `namespace.yml`, `user-service.yml`, `product-service.yml`, `inventory-service.yml`, `order-service.yml`. Add new microservice manifests here (one file per service).
  - **`k8s/scripts/`**: Bash scripts used to build images and control the environment:
    - `init.sh` — builds/creates docker images and any preparation needed for deployment.
    - `start-services.sh` — applies `k8s/services/*.yml` to start services in Kubernetes and creates `kubectl port-forward` entries for local access.
    - `delete-services.sh` — removes Kubernetes resources created for services.
    - `delete-images.sh` — removes docker images used by services (clean up local images after tests).
- **`user-service/`**: User management microservice (Node.js). Publishes `user.created` and `user.updated` events.
  - `Dockerfile` — image build instructions for the user-service.
  - `index.js` — service source (node app).
  - `package.json` — node dependencies and scripts.
- **`product-service/`**: Product catalog microservice (Node.js). Publishes `product.created` and `product.updated` events.
- **`inventory-service/`**: Inventory tracking microservice (Node.js). Consumes order and product events, publishes inventory updates.
- **`order-service/`**: Order management microservice (Go). Creates and tracks orders, publishes `order.created` and `order.completed`, consumes `payment.success` events.
  - `Dockerfile` — multi-stage build for Go service.
  - `main.go` — service implementation in Go.
  - `go.mod`, `go.sum` — Go dependencies.

**Microservices Architecture**

This project implements an event-driven microservices architecture with the following services:

1. **User Service** (Port 3001, Node.js)
   - Manages user accounts
   - Publishes: `user.created`, `user.updated`

2. **Product Service** (Port 3002, Node.js)
   - Manages product catalog
   - Publishes: `product.created`, `product.updated`

3. **Order Service** (Port 3003, Go) ⭐
   - Creates and tracks orders
   - Publishes: `order.created`, `order.completed`
   - Consumes: `payment.success`
   - **Flow**: Creates order → Waits for payment → Completes order → Triggers inventory/shipping

4. **Inventory Service** (Port 3005, Node.js)
   - Tracks stock levels
   - Publishes: `inventory.updated`
   - Consumes: `order.created`, `order.completed`, `product.created`

**Event Flow Example:**

```
1. Client → POST /orders → Order Service
2. Order Service → publishes order.created → Kafka
3. Payment Service → consumes order.created → processes payment
4. Payment Service → publishes payment.success → Kafka
5. Order Service → consumes payment.success → updates order status
6. Order Service → publishes order.completed → Kafka
7. Inventory Service → consumes order.completed → updates stock
8. Shipping Service → consumes order.completed → initiates shipment
```

**Important files and what they do**

- `docker-compose.yml`: starts `zookeeper`, `kafka`, and all microservices (`user-service`, `product-service`, `inventory-service`, `order-service`). When adding a new service for local testing, add it under the `services:` section here.
- `k8s/services/kafka-init.yml`: this YAML contains the container/entrypoint that runs `kafka-topics` commands to create the topics needed. Add your topic creation command here so the cluster init creates your topic automatically.
- `k8s/services/<your-service>.yml`: Kubernetes `Deployment` + `Service` for your microservice. Add one file per microservice in this folder.
- `k8s/scripts/init.sh`: script to build your docker image(s). Add the `docker build`/`docker push` (if you use a registry) commands for your service here.
- `k8s/scripts/start-services.sh`: script that runs `kubectl apply -f k8s/services/<your-service>.yml` for your service and sets up a `kubectl port-forward` so you can access it locally. Add the command here for your service.
- `k8s/scripts/delete-images.sh`: script that removes images used in the environment. Add the name (or `docker rmi` commands) for your image so the cleanup removes it.

**How to add a new microservice — Step by step**

1) Create your microservice folder

- Create a folder at repository root called `./<your-service>` (example: `./order-service`). Put your service code there.
- Add a `Dockerfile` to that folder. Minimal Node.js example:

```powershell
# from repo root
cd .\order-service
docker build -t myusername/order-service:latest .
```

2) Add your service to `docker-compose.yml` (for local development)

- Under the `services:` block in `docker-compose.yml` add an entry similar to the example below (adjust ports, environment and names):

```yaml
  order-service:
    build:
      context: ./order-service
      dockerfile: Dockerfile
    container_name: order-service
    ports:
      - "3002:3001"
    environment:
      - KAFKA_BROKER=kafka:29092
      - SERVICE_NAME=order-service
    networks:
      - microservices-net
    depends_on:
      kafka-init:
        condition: service_completed_successfully
      kafka:
        condition: service_healthy
```

3) Create Kubernetes manifest in `k8s/services/`

- Add `k8s/services/order-service.yml` (or `order-service.yml`) with a Deployment and a Service. Example minimal manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
      - name: order-service
        image: myusername/order-service:latest
        ports:
        - containerPort: 3001
        env:
        - name: KAFKA_BROKER
          value: "kafka:29092"

---

apiVersion: v1
kind: Service
metadata:
  name: order-service
  namespace: default
spec:
  selector:
    app: order-service
  ports:
  - protocol: TCP
    port: 3001
    targetPort: 3001
  type: ClusterIP
```

4) Add your Kafka topic in `k8s/services/kafka-init.yml`

- Open `k8s/services/kafka-init.yml` (or the `kafka-init` entry in `docker-compose.yml` for local compose) and add a `kafka-topics --create --if-not-exists --topic <your-topic> --partitions <n> --replication-factor 1` command.

- Example (the `kafka-init` container in `docker-compose.yml` runs a shell that contains `kafka-topics` commands). Add a line like:

```sh
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic order-events --partitions 3 --replication-factor 1
```

5) Register image creation in `k8s/scripts/init.sh`

- Edit `k8s/scripts/init.sh` and add the `docker build` command that creates your microservice image. If you push to a remote registry (recommended for remote K8s clusters), add `docker push` after tagging the image.

Example additions:

```bash
echo "Building order-service image"
docker build -t myusername/order-service:latest ./order-service
# optionally push if using a registry
docker push myusername/order-service:latest
```

Note: `k8s/scripts/init.sh` is a bash script — run it in WSL, Git Bash or a Unix shell on CI.

6) Add start/apply commands to `k8s/scripts/start-services.sh`

- Edit `k8s/scripts/start-services.sh` to apply the new manifest and create a port-forward so you can access the service locally. Example lines to add:

```bash
kubectl apply -f k8s/services/order-service.yml
# Wait for pods to be ready (optional)
kubectl -n default rollout status deployment/order-service --timeout=120s
# Port-forward so team members can access the service locally
kubectl -n default port-forward service/order-service 3002:3001 &
```

7) Add image cleanup in `k8s/scripts/delete-images.sh`

- Add your image to the cleanup script so that `delete-images.sh` removes `myusername/order-service:latest` when cleaning the environment.

Example line to add:

```bash
docker rmi -f myusername/order-service:latest || true
```

8) (Optional) Remove K8s resources in `k8s/scripts/delete-services.sh`

- If the repository has a `delete-services.sh`, add `kubectl delete -f k8s/services/order-service.yml` so the service can be removed cleanly.

9) Test everything locally

- Start kafka + zookeeper + topic creation + local services:

```powershell
docker compose up --build
```

- Or, build your image and run locally:

```powershell
docker build -t myusername/order-service:latest ./order-service
docker run --rm -e KAFKA_BROKER=kafka:29092 -p 3002:3001 myusername/order-service:latest
```

- To deploy on a Kubernetes cluster (after building/pushing image and updating `k8s/services/order-service.yml`):

```powershell
# run init.sh to build and (optionally) push images
bash k8s/scripts/init.sh

# apply k8s resources
bash k8s/scripts/start-services.sh
```

10) PR checklist for your team member

- Add the service folder with code and `Dockerfile`.
- Update `docker-compose.yml` with a development service entry.
- Add `k8s/services/<your-service>.yml` and verify the `image:` points to the image created in `init.sh`.
- Add topic creation to `k8s/services/kafka-init.yml`.
- Add `docker build` (and `docker push` if needed) to `k8s/scripts/init.sh`.
- Add `kubectl apply` and `kubectl port-forward` lines to `k8s/scripts/start-services.sh`.
- Add `docker rmi` or image name to `k8s/scripts/delete-images.sh` for cleanup.
- Add a short README in your service folder explaining how to run the service locally and the endpoints it exposes.

Notes and repository-specific clarifications

- The repo uses `k8s/services/` and `k8s/scripts/*.sh` (bash) — prefer editing those files, not new folders named `ks8` or `microservices` (typos seen in some instructions). Use `k8s/services/` for YAMLs and `k8s/scripts/` for the helper scripts.
- `k8s/scripts/*` are bash scripts; on Windows use WSL, Git Bash, or run them in CI (Linux) to avoid shell incompatibilities.


## Kafka Overview

- **What is Kafka:** Apache Kafka is a distributed streaming platform for building real-time data pipelines and streaming applications. It provides durable, partitioned, and replicated logs (topics) that producers write to and consumers read from.

- **Key concepts:**
  - **Broker:** A Kafka server that stores and serves topic data.
  - **Topic:** A named stream of records; topics are split into **partitions** for parallelism and scale.
  - **Producer:** A client that writes messages to topics.
  - **Consumer:** A client that reads messages from topics; consumers can be grouped into **consumer groups** for coordinated consumption.
  - **Offset:** A pointer to a position in a partition that identifies a record.

## How to use Kafka in this project

- **Local development (Docker Compose):**
  - Start the stack (Zookeeper, Kafka, topic init, and `user-service`):

```powershell
docker compose up --build
```

  - The `kafka-init` container (see `docker-compose.yml`) runs `kafka-topics` commands to create required topics (e.g. `user-events`). Add or modify topic creation there for local setups.

- **Common CLI commands (execute against the `kafka` container):**

  - List topics:

```powershell
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

  - Create a topic:

```powershell
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic my-topic --partitions 3 --replication-factor 1
```

  - Produce messages (type and Enter):

```powershell
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic user-events
```

  - Consume messages (from beginning):

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic user-events --from-beginning
```

  Notes: services inside the Docker network use `kafka:29092`. From the host you can use `localhost:9092` because `9092` is exposed by `docker-compose.yml`.

- **Service integration:**
  - Services in this repo read the `KAFKA_BROKER` environment variable (see `docker-compose.yml` where `KAFKA_BROKER=kafka:29092`). In Kubernetes, use the cluster DNS name (for example `kafka:29092`) in pod env vars.

- **Kubernetes / Cluster init:**
  - The `k8s/services/kafka-init.yml` (and the `kafka-init` entry in `docker-compose.yml`) is the place to add `kafka-topics --create` commands so topics are created automatically during deployment.

- **Helpful tips:**
  - Keep `KAFKA_AUTO_CREATE_TOPICS_ENABLE` disabled in production to avoid accidental topic creation; create topics explicitly.
  - Choose partition counts according to expected throughput and consumer parallelism.
  - Monitor consumer offsets to ensure consumers are processing messages in time.


