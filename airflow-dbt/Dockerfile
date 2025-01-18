FROM quay.io/astronomer/astro-runtime:12.6.0

RUN python -m venv .airflow_dbt && source .airflow_dbt/bin/activate &&\
        pip install -r requirements.txt
