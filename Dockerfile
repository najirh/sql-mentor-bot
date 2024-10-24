   # Use an official Python runtime as a parent image
   FROM python:3.9

   # Set the working directory in the container
   WORKDIR /app

   # Copy the requirements file into the container
   COPY requirements.txt .

   # Install any needed packages specified in requirements.txt
   RUN pip install --no-cache-dir -r requirements.txt

   # Copy the current directory contents into the container at /app
   COPY . .

   # Expose the port the app runs on
   EXPOSE 8080

   # Run bot.py when the container launches
   CMD ["python", "bot.py"]
