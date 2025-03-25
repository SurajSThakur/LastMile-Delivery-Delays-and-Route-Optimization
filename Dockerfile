# Use official Python image
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy the entire project to the container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which FastAPI runs
EXPOSE 5000

# Command to run the FastAPI app
CMD ["uvicorn", "FastAPI:app", "--host", "0.0.0.0", "--port", "5000"]