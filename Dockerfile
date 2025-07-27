# Python version
FROM python:3.10.18-slim

# Set working directory
WORKDIR /app

# Copy local project files into the container
COPY . .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Run the app
CMD ["python", "src/pipeline.py"]