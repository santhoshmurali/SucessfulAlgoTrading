# Use a lightweight Python image as the base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt /app

# Install the required dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the rest of the application code to the container
COPY . /app

WORKDIR /app/bin/programs

# Set the command to run the script
CMD ["python", "GetStockInfo.py"]
