# Use an official Python runtime as a parent image
FROM python:3.12-slim

WORKDIR /app

# Copy requirements first (this layer gets cached)
COPY ./src/requirements.txt /app/

# Install dependencies (this layer gets cached if requirements don't change)
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source code (this layer changes when code changes)
COPY ./src /app/src

# Keep the container running
RUN echo '#!/bin/bash\nwhile true; do sleep 3600; done' > /app/keep_alive.sh
RUN chmod +x /app/keep_alive.sh
ENTRYPOINT ["/app/keep_alive.sh"]