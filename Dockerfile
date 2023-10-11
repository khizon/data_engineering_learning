# Use an appropriate base image, such as Python.
FROM python:3.11-slim

# Set the working directory inside the container.
WORKDIR /data_engineering

# Copy your requirements.txt file to the container.
COPY requirements.txt .

# Install the packages listed in requirements.txt, clearing the cache.
RUN pip install --no-cache-dir -r requirements.txt