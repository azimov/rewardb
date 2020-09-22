FROM postgres
ENV POSTGRES_DB eunomia
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres
COPY init.sql /docker-entrypoint-initdb.d/
COPY cdm_schema.sql /docker-entrypoint-initdb.d/
COPY eunomia_dt /eunomia_dt