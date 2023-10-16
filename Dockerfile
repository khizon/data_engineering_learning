# Use an appropriate base image, such as Python.
FROM python:3.11-slim

# Add git
RUN apt-get -y update
RUN apt-get -y install git

# Set the working directory inside the container.
WORKDIR /data_engineering

# Copy your requirements.txt file to the container.
COPY requirements.txt .

# Install the packages listed in requirements.txt, clearing the cache.
RUN pip install --no-cache-dir -r requirements.txt
