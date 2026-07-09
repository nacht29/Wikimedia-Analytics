-- CREATE DATABASE wikimedia_staging;


-- USE wikimedia_staging;
CREATE SCHEMA IF NOT EXISTS recentchange_stream;
CREATE SCHEMA IF NOT EXISTS meta;


CREATE TABLE IF NOT EXISTS recentchange_stream.backfill_points (
	last_event_id TEXT PRIMARY KEY,
	backfill_status INT NOT NULL,
	created_at TIMESTAMP NOT NULL,
	updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS recentchange_stream.backfill_queue (
	start_event_id TEXT PRIMARY KEY,
	end_event_id TEXT NOT NULL,
	backfill_status INT NOT NULL,
	created_at TIMESTAMP NOT NULL,
	updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS meta.backfill_status (
	status_no INT PRIMARY KEY,
	backfill_status TEXT NOT NULL
);

-- 5. Insert Seed Data
INSERT INTO meta.backfill_status (
	status_no,
	backfill_status
)
VALUES
	(0, 'Pending'),
	(1, 'Resolved'),
	(2, 'Error');
