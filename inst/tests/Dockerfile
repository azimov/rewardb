FROM postgres
ENV POSTGRES_DB eunomia
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres
COPY init.sql /docker-entrypoint-initdb.d/
COPY eunomia.sql /docker-entrypoint-initdb.d/