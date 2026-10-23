# [TV1] Hive Metastore 4.0.1 + driver PostgreSQL
FROM apache/hive:4.0.1
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar
USER hive
