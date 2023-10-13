# Use an appropriate base image, such as Python.
FROM apache/airflow:2.7.2

# Copy your requirements.txt file to the container.
COPY requirements.txt /requirements.txt

# Install the packages listed in requirements.txt, clearing the cache.
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt