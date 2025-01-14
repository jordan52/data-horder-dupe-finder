import argparse
import hashlib
import os
import sqlite3
from datetime import datetime
import sys

def init_database():
    """Initialize the SQLite database with schema"""
    conn = sqlite3.connect('filesystem.db')
    with open('schema.sql', 'r') as schema_file:
        conn.executescript(schema_file.read())
    conn.close()

def calculate_md5(filepath):
    """Calculate MD5 hash of a file"""
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def get_file_times(filepath):
    """Get creation and modification times of a file"""
    stats = os.stat(filepath)
    # Use either st_birthtime (macOS) or st_ctime (other platforms) for creation time
    created = getattr(stats, 'st_birthtime', stats.st_ctime)
    modified = stats.st_mtime
    return (
        datetime.fromtimestamp(created),
        datetime.fromtimestamp(modified)
    )

def scan_filesystem(conn, run_id, base_path):
    """Recursively scan filesystem and store file information"""
    cursor = conn.cursor()
    
    for root, _, files in os.walk(base_path):
        for filename in files:
            full_path = os.path.join(root, filename)
            try:
                relative_path = os.path.relpath(full_path, base_path)
                md5_hash = calculate_md5(full_path)
                created_time, modified_time = get_file_times(full_path)
                
                cursor.execute('''
                    INSERT INTO file_entries 
                    (run_id, filename, full_path, relative_path, md5_hash, created_time, modified_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    run_id, filename, full_path, relative_path, md5_hash,
                    created_time.isoformat(), modified_time.isoformat()
                ))
                
            except (PermissionError, FileNotFoundError) as e:
                print(f"Error processing {full_path}: {e}", file=sys.stderr)
                continue
    
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description='Filesystem crawler and indexer')
    parser.add_argument('run_identifier', help='Unique identifier for this scan run')
    parser.add_argument('drive_name', help='Name of the drive being scanned')
    parser.add_argument('path', help='Base path to start scanning from')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.path):
        print(f"Error: Path '{args.path}' does not exist", file=sys.stderr)
        sys.exit(1)
    
    init_database()
    conn = sqlite3.connect('filesystem.db')
    
    try:
        cursor = conn.cursor()
        
        # Create scan run entry
        cursor.execute('''
            INSERT INTO scan_runs (run_identifier, drive_name, base_path)
            VALUES (?, ?, ?)
        ''', (args.run_identifier, args.drive_name, args.path))
        
        run_id = cursor.lastrowid
        conn.commit()
        
        # Scan filesystem
        scan_filesystem(conn, run_id, args.path)
        
        print(f"Scan completed successfully. Run ID: {run_id}")
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()
