/*
UPDATE pg_database SET datallowconn = 'false' WHERE datname IN ('postgres', 'nacht29');

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname IN ('postgres', 'nacht29');

DROP DATABASE IF EXISTS postgres;
DROP DATABASE IF EXISTS nacht29;
*/

/*  -- for psql shell
SELECT 'CREATE DATABASE wikimedia'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'wikimedia')\gexec
*/

-- CREATE DATABASE wikimedia;

CREATE SCHEMA IF NOT EXISTS "wikimedia.test";

CREATE TABLE IF NOT EXISTS "wikimedia.test.test_table"(
	test_col1 INT PRIMARY KEY,
	test_col2 VARCHAR(10)
);


INSERT INTO "wikimedia.test.test_table"
VALUES
	(1, 'c1'),
	(2, 'c2')
ON CONFLICT (test_col1) DO NOTHING;;

SELECT * FROM "wikimedia.test.test_table";
