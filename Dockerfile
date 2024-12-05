# Use a base image of Python (Alpine version for a smaller container size and minimal dependencies)
FROM python:3.10-alpine

# Install required build tools and libraries for native dependencies and librdkafka
# This includes compilers and development libraries to allow Python packages with C/C++ extensions to compile properly
RUN apk update && apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev bash git


# Clone the repository into the 'producer' folder
RUN git clone https://github.com/DIETI-DISTA-IoT/Wandber.git wandber


# Set the working directory inside the container
# This is where all the files will be copied, and the commands will be executed
WORKDIR /wandber

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Install the dependencies specified in the requirements file and other required libraries
RUN pip install --no-cache-dir -r requirements.txt

# Command to start the data simulator script when the container is run
CMD ["python", "wandber.py"]
