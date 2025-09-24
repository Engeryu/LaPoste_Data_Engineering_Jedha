# Dockerfile

FROM continuumio/miniconda3:latest

WORKDIR /app

COPY conda_etl.yml .
RUN conda env create -f conda_etl.yml && conda clean -afy

SHELL ["conda", "run", "-n", "supercourier-etl", "/bin/bash", "-c"]
RUN pip install "fastapi[all]"

# Utiliser des chemins de destination explicites et absolus
COPY ./supercourier_etl /app/supercourier_etl
COPY api.py /app/api.py
COPY start.sh /app/start.sh
COPY ./static /app/static
COPY ./templates /app/templates

RUN chmod +x /app/start.sh

EXPOSE 80

CMD ["./start.sh"]