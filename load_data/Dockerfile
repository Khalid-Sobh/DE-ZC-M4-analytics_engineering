# Use a slim official image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies and clean up cache in one layer to keep it slim
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script
COPY main.py .

# Run the script
CMD ["python", "main.py"]