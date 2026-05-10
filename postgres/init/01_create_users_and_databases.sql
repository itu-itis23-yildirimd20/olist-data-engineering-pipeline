-- Create application users
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'olist') THEN
    CREATE USER olist WITH PASSWORD 'olist';
  END IF;
END $$;
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE USER airflow WITH PASSWORD 'airflow';
  END IF;
END $$;

-- Create databases
SELECT 'CREATE DATABASE olist OWNER olist' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'olist')\gexec
SELECT 'CREATE DATABASE airflow OWNER airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Grant olist schema privileges
\c olist
GRANT ALL ON SCHEMA public TO olist;
