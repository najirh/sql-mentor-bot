   # Use an official Python runtime as a parent image
   FROM python:3.9-slim

   # Set the working directory in the container
   WORKDIR /app

   # Install build essentials and clean up
   RUN apt-get update && apt-get install -y \
       build-essential \
       libffi-dev \
       && rm -rf /var/lib/apt/lists/*

   # Copy the requirements file into the container
   COPY requirements.txt .

   # Install any needed packages specified in requirements.txt
   RUN pip install --no-cache-dir -r requirements.txt

   # Copy the current directory contents into the container at /app
   COPY . .

   # Run bot.py when the container launches
   CMD ["python", "bot.py"]
