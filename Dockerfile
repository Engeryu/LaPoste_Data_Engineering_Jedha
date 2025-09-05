# --- STAGE 1: The Builder ---
FROM mambaorg/micromamba:latest AS builder

COPY --chown=micromamba:micromamba conda_etl.yml /tmp/conda_etl.yml

RUN micromamba create -y -n supercourier-etl -f /tmp/conda_etl.yml && \
    micromamba clean --all --yes

COPY --chown=micromamba:micromamba . /app

# --- STAGE 2: The Final Image ---
FROM mambaorg/micromamba:latest

COPY --from=builder /opt/conda/envs/supercourier-etl /opt/conda/envs/supercourier-etl
COPY --from=builder /app /app

WORKDIR /app

ENTRYPOINT ["micromamba", "run", "-n", "supercourier-etl", "python", "-m", "supercourier_etl.main"]
CMD []