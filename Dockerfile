# --- STAGE 1: The Builder ---
# Use micromamba, a lighter and faster alternative to Conda for containers.
FROM mambaorg/micromamba:latest AS builder

# Copy the environment file and the source code.
COPY --chown=micromamba:micromamba conda_etl.yml /tmp/conda_etl.yml
COPY --chown=micromamba:micromamba . /app

# Create the conda environment from the YAML file and clean up the cache.
RUN micromamba create -y -n supercourier-etl -f /tmp/conda_etl.yml && \
    micromamba clean --all --yes


# --- STAGE 2: The Final Image ---
FROM mambaorg/micromamba:latest

# Create a non-root user for security.
RUN useradd --create-home --shell /bin/bash appuser
USER appuser
WORKDIR /home/appuser/app

# Copy the pre-installed conda environment and the application code from the builder stage.
COPY --chown=appuser:appuser --from=builder /opt/conda/envs/supercourier-etl /opt/conda/envs/supercourier-etl
COPY --chown=appuser:appuser --from=builder /app .

# Command to start the Uvicorn web server.
# It uses the micromamba executable to run the command in the correct environment.
CMD ["/opt/conda/envs/supercourier-etl/bin/uvicorn", "api:app", "--host", "0.0.0.0", "--port", "80"]