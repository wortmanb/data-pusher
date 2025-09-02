# Elasticsearch Data Pusher

A high-volume data generator for pushing realistic log data to Elasticsearch at configurable rates.

## Quick Start

### Local Development with Docker Compose

```bash
# Start Elasticsearch, Kibana, and data-pusher
docker-compose up -d

# View logs
docker-compose logs -f data-pusher

# Stop all services
docker-compose down
```

Access Kibana at http://localhost:5601 to view the generated data.

### Kubernetes Deployment

```bash
# Deploy to Kubernetes
kubectl apply -f k8s-data-pusher.yaml

# Check status
kubectl get pods -n elasticsearch

# View logs
kubectl logs -f deployment/data-pusher -n elasticsearch
```

### Systemd Service (Linux)

```bash
# Copy service file
sudo cp data-pusher.service /etc/systemd/system/

# Customize paths and settings in the service file as needed
sudo systemctl edit data-pusher.service

# Enable and start
sudo systemctl enable data-pusher.service
sudo systemctl start data-pusher.service
```

## Configuration

### Environment Variables

- `ES_INDEX_PREFIX` - Index name prefix (default: logs)
- `ES_DOCS_PER_SECOND` - Target documents per second (default: 1000)
- `ES_BATCH_SIZE` - Batch size for bulk indexing (default: 500)
- `ES_THREADS` - Number of worker threads (default: 4)
- `PUSH_DURATION` - Duration to run in seconds (default: 3600)
- `ES_USERNAME` - Elasticsearch username for authentication
- `ES_PASSWORD` - Elasticsearch password for authentication

### Command Line Options

```bash
python data-pusher.py --help
```

Key options:
- `--index` - Target index name
- `--rate` - Documents per second
- `--duration` - Duration in seconds
- `--host` - Elasticsearch host
- `--username` - Elasticsearch username (or ES_USERNAME env var)
- `--password` - Elasticsearch password (or ES_PASSWORD env var)
- `--config` - Configuration file (kept for compatibility, currently unused)

## Data Format

Generates realistic microservice log data with:
- Timestamp fields
- Service names and log levels
- Realistic log messages with placeholders
- Metrics (response times, memory usage, etc.)
- Error details for ERROR level logs
- User and session tracking

## Building

```bash
# Build Docker image
docker build -t data-pusher .

# Build and run with custom settings
docker build -t data-pusher . && \
docker run data-pusher \
  python data-pusher.py \
  --index my-logs \
  --rate 500 \
  --duration 1800 \
  --host elasticsearch:9200
```