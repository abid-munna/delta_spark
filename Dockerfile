# Use the Bitnami Spark image as the base
FROM bitnami/spark:3.3

# Switch to the root user to perform administrative tasks
USER root

# Ensure pip is installed
RUN apt-get update && apt-get install -y python3-pip

# Install Delta Lake for PySpark
RUN pip install delta-spark

# Create the /app directory and set the proper permissions
RUN mkdir -p /app && chmod -R 777 /app

# Copy your application files to /app
COPY main.py /app/


# Switch back to the non-root user (optional)
USER 1001

# Set the working directory to /app
WORKDIR /app

# # Set the entry point to run your Spark job
# ENTRYPOINT ["spark-submit", "/app/main.py"]
