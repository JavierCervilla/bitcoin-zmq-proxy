# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8765 available to the world outside this container
EXPOSE 8765

# Run script with a restart policy when the container launches
CMD ["python", "proxy.py"]
