# Use a stable, lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y git build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies and install
COPY requirment.txt .
RUN pip install --no-cache-dir -r requirment.txt

# Copy dbt project
COPY dbtconnect/ ./dbtconnect/

# Copy dbt profiles.yml for Snowflake connection
COPY profiles.yml /root/.dbt/profiles.yml

# Copy FastAPI app
COPY app.py .

# Set environment variables for dbt
ENV DBT_PROFILES_DIR=/root/.dbt
ENV DBT_PROJECT_DIR=/app/dbtconnect

# Expose FastAPI port
EXPOSE 8000

# Default command to start FastAPI server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
