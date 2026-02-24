-- Create airflow database if it doesn't exist (must run outside a DO block)
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
