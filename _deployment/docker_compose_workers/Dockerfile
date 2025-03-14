# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY ./src/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the src and flask_worker_server directories into the container
COPY ./src /app/src
COPY ./src/flask_worker_server /app/flask_worker_server

# Set the environment variable for Flask
ENV FLASK_APP=flask_worker_server/handler.py
ENV FLASK_ENV=production

# Expose port 5000 for the Flask app
EXPOSE 5000

# Run the Flask application
CMD ["flask", "run", "--host=0.0.0.0"]