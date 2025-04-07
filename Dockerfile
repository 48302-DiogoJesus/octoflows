# Use an official Python runtime as a parent image
FROM python:3.12-slim

WORKDIR /app

# Install pipreqs to generate requirements.txt inside the container
RUN pip install pipreqs

# Copy the source code first
COPY ./src /app/src

# Generate requirements.txt from the source code
RUN pipreqs /app/src --force --savepath /app/requirements.txt

# Install dependencies from the generated requirements.txt
# RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Keep the container running
RUN echo '#!/bin/bash\nwhile true; do sleep 3600; done' > /app/keep_alive.sh
RUN chmod +x /app/keep_alive.sh
ENTRYPOINT ["/app/keep_alive.sh"]
