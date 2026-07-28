-- This script is executed during the initialization of the Postgres container.
-- The default database created by the container config is youtube_dw.
-- We manually create airflow_db for Airflow metadata storage.

CREATE DATABASE airflow_db;
