# Use a slim Python image
FROM python:3.11-slim

# System dependencies (for lxml, duckdb, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up the working directory
WORKDIR /app

# Setup Python environment
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/
# Adjust PYTHONPATH so src module can be resolved
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Run the pipeline
CMD ["python", "src/main_pipeline.py"]