# Use an Ubuntu base image
FROM ubuntu:22.04

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    ghostscript \
    curl \
    && apt-get clean

# Install Node.js 16 and npm
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean

# Set the working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package.json package-lock.json ./

# Install Node.js dependencies
RUN npm install

# Install ts-node globally
RUN npm install -g ts-node

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on (if applicable)
EXPOSE 3000

# Command to run the application
CMD ["sh", "-c", "npm start > /app/logs/output.log 2>&1"]
