FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy everything
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Create data directory
RUN mkdir -p /app/data/categories

# Make start script executable
RUN chmod +x start.sh

EXPOSE 8000

CMD ["./start.sh"]
