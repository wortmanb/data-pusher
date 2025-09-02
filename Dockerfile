# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install Python dependencies
RUN pip install --no-cache-dir \
    elasticsearch

# Copy the data-pusher script
COPY data-pusher.py ./

# Create non-root user
RUN groupadd -r datapusher && useradd -r -g datapusher datapusher

# Change ownership and switch to non-root user
RUN chown -R datapusher:datapusher /app
USER datapusher

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command - runs indefinitely by default
CMD ["python", "data-pusher.py", "--index", "logs", "--rate", "1000", "--verbose"]