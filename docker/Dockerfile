# ---------------------------
# Base Image
# ---------------------------
FROM python:3.10-slim

# ---------------------------
# Set working directory
# ---------------------------
WORKDIR /app

# ---------------------------
# Environment variables
# ---------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ---------------------------
# System dependencies (PostgreSQL)
# ---------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------
# Install Python dependencies
# ---------------------------
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# ---------------------------
# Copy project files
# ---------------------------
COPY . .

# ---------------------------
# Default command: Run ETL pipeline
# ---------------------------
CMD ["python", "scripts/pipeline_orchestrator.py"]
