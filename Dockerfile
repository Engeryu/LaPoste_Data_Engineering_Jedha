# --- STAGE 1: The Builder ---
FROM mambaorg/micromamba:latest AS builder

COPY --chown=micromamba:micromamba conda_etl.yml /tmp/conda_etl.yml
COPY --chown=micromamba:micromamba . /app

RUN micromamba create -y -n supercourier-etl -f /tmp/conda_etl.yml && \
    micromamba clean --all --yes


# --- STAGE 2: The Final Image ---
FROM mambaorg/micromamba:latest

# Switch to ROOT user temporarily to perform administrative tasks
USER root

# Create a non-root user and its home directory
RUN useradd --create-home --shell /bin/bash appuser

# Copy the pre-installed conda environment from the builder stage
COPY --from=builder /opt/conda/envs/supercourier-etl /opt/conda/envs/supercourier-etl

# Copy the application code into the new user's home directory
COPY --from=builder /app /home/appuser/app

# Set correct ownership for the entire app directory
RUN chown -R appuser:appuser /home/appuser/app

# Switch back to the non-root user for runtime
USER appuser
WORKDIR /home/appuser/app

# Add the app directory to Python's path to ensure modules are found
ENV PYTHONPATH=/home/appuser/app

# Command to start the Uvicorn web server
# The full path to the executable inside the conda env is more robust
CMD ["/opt/conda/envs/supercourier-etl/python", "-m", "uvicorn" "web_app.api:app", "--host", "0.0.0.0", "--port", "80"]