FROM quay.io/astronomer/astro-runtime:12.5.0-python-3.12

USER root

RUN mkdir -p /home/astro/.dbt
COPY dbt/profiles.yml /home/astro/.dbt/profiles.yml

USER astro