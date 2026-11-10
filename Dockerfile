FROM astrocrpublic.azurecr.io/runtime:3.3-9

# Image metadata
LABEL org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow (Astro Runtime) ETL: Frankfurter exchange rates -> MongoDB" \
      org.opencontainers.image.authors="23L0995" \
      com.mlops.task="task3-airflow-etl"
