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

def find_duplicates(conn):
    """Find files that share the same MD5 hash but have different paths"""
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT f1.md5_hash, f1.full_path, f2.full_path, 
               sr1.run_identifier as run1, sr2.run_identifier as run2
        FROM file_entries f1
        JOIN file_entries f2 ON f1.md5_hash = f2.md5_hash
        JOIN scan_runs sr1 ON f1.run_id = sr1.run_id
        JOIN scan_runs sr2 ON f2.run_id = sr2.run_id
        WHERE f1.full_path < f2.full_path
        ORDER BY f1.md5_hash
    ''')
    
    duplicates = cursor.fetchall()
    if not duplicates:
        print("No duplicate files found.")
        return
    
    current_hash = None
    for md5_hash, path1, path2, run1, run2 in duplicates:
        if md5_hash != current_hash:
            print(f"\nFiles with hash {md5_hash}:")
            current_hash = md5_hash
        print(f"  Run '{run1}': {path1}")
        print(f"  Run '{run2}': {path2}")

def main():
    parser = argparse.ArgumentParser(description='Filesystem crawler and indexer')
    parser.add_argument('run_identifier', help='Unique identifier for this scan run')
    parser.add_argument('drive_name', help='Name of the drive being scanned')
    parser.add_argument('path', help='Base path to start scanning from')
    parser.add_argument('--find-duplicates', action='store_true',
                      help='Show duplicate files after scanning')
    
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
        
        if args.find_duplicates:
            find_duplicates(conn)
            
    finally:
        conn.close()

if __name__ == '__main__':
    main()
