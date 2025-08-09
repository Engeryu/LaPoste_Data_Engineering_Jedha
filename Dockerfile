# Use a miniconda base image which includes the conda package manager
FROM continuumio/miniconda3:latest

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY environment.yml .

# Create the conda environment from the environment.yml file.
# The environment is named 'pysupercourier' as defined in the yml.
# We also clean up the conda cache to reduce the final image size.
RUN conda env create -f environment.yml && \
    conda clean -afy

# Copy the application source code into the container
COPY ./src ./src
COPY main.py .

# Use ENTRYPOINT to set up the environment. The 'conda run' command with '--no-capture-output'
# ensures that the terminal (stdin/stdout) is passed through correctly to the child process.
# This is the key to making interactive scripts work reliably.
ENTRYPOINT ["conda", "run", "-n", "pysupercourier", "--no-capture-output"]

# Use CMD to define the default command to execute within the activated environment.
CMD ["python", "-u", "main.py"]