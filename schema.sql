CREATE TABLE IF NOT EXISTS scan_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_identifier TEXT NOT NULL,
    drive_name TEXT NOT NULL,
    base_path TEXT NOT NULL,
    scan_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS file_entries (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    filename TEXT NOT NULL,
    full_path TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    md5_hash TEXT NOT NULL,
    created_time DATETIME NOT NULL,
    modified_time DATETIME NOT NULL,
    FOREIGN KEY (run_id) REFERENCES scan_runs(run_id)
);
