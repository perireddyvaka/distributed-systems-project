# Multi-stage or optimized lightweight container image for Distributed Search Engine
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies (build-essential, curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install pinned Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source modules and compiled protocol buffers
COPY proto/ /app/proto/
COPY shared/ /app/shared/
COPY indexing/ /app/indexing/
COPY storage/ /app/storage/
COPY replication/ /app/replication/
COPY coordinator/ /app/coordinator/
COPY services/ /app/services/
COPY client/ /app/client/
COPY reference/ /app/reference/
COPY scripts/ /app/scripts/
COPY datasets/ /app/datasets/

# Create data directory for SQLite persistence
RUN mkdir -p /data

# Default port exposed by Coordinator
EXPOSE 50050 50051 50052 50061 50062

CMD ["python", "services/entrypoint_coordinator.py"]
