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
    """Find files that share the same filename and MD5 hash"""
    cursor = conn.cursor()
    
    cursor.execute('''
        WITH DuplicateGroups AS (
            SELECT filename, md5_hash, full_path
            FROM file_entries
            GROUP BY filename, md5_hash, full_path
            HAVING COUNT(*) > 1
        ),
        UniqueDuplicates AS (
            SELECT filename, md5_hash
            FROM file_entries
            GROUP BY filename, md5_hash
            HAVING COUNT(DISTINCT full_path) > 1
        )
        SELECT 
            f.filename,
            f.md5_hash,
            f.full_path,
            sr.run_identifier
        FROM file_entries f
        JOIN scan_runs sr ON f.run_id = sr.run_id
        JOIN UniqueDuplicates ud ON f.filename = ud.filename 
            AND f.md5_hash = ud.md5_hash
        ORDER BY f.filename, f.md5_hash, sr.run_identifier
    ''')
    
    duplicates = cursor.fetchall()
    if not duplicates:
        print("No duplicate files found.")
        return
    
    current_key = None
    for filename, md5_hash, path, run_id in duplicates:
        key = (filename, md5_hash)
        if key != current_key:
            print(f"\nDuplicate file: {filename}")
            print(f"MD5 Hash: {md5_hash}")
            current_key = key
        print(f"  Run '{run_id}': {path}")

def find_modified_files(conn):
    """Find files that share the same name but have different MD5 hashes"""
    cursor = conn.cursor()
    
    cursor.execute('''
        WITH FileVersions AS (
            SELECT 
                f.filename,
                f.md5_hash,
                f.full_path,
                f.modified_time,
                sr.run_identifier,
                ROW_NUMBER() OVER (
                    PARTITION BY f.filename 
                    ORDER BY datetime(f.modified_time) DESC
                ) as version_rank
            FROM file_entries f
            JOIN scan_runs sr ON f.run_id = sr.run_id
            WHERE f.filename IN (
                SELECT filename
                FROM file_entries
                GROUP BY filename
                HAVING COUNT(DISTINCT md5_hash) > 1
            )
        )
        SELECT 
            filename,
            md5_hash,
            full_path,
            modified_time,
            run_identifier,
            version_rank
        FROM FileVersions
        ORDER BY filename, version_rank
    ''')
    
    results = cursor.fetchall()
    if not results:
        print("No modified files found.")
        return
    
    current_file = None
    for filename, md5_hash, path, mod_time, run_id, rank in results:
        if filename != current_file:
            print(f"\nFile: {filename}")
            current_file = filename
        
        latest = " (Latest version)" if rank == 1 else ""
        print(f"  Run '{run_id}': {path}")
        print(f"    Modified: {mod_time}")
        print(f"    MD5: {md5_hash}{latest}")

def find_duplicates(conn):
    """Find files that share the same filename and MD5 hash"""
    cursor = conn.cursor()
    
    cursor.execute('''
        WITH DuplicateGroups AS (
            SELECT filename, md5_hash
            FROM file_entries
            GROUP BY filename, md5_hash
            HAVING COUNT(*) > 1
        )
        SELECT DISTINCT
            f.filename,
            f.md5_hash,
            f.full_path,
            sr.run_identifier
        FROM file_entries f
        JOIN scan_runs sr ON f.run_id = sr.run_id
        JOIN DuplicateGroups d ON f.filename = d.filename 
            AND f.md5_hash = d.md5_hash
        ORDER BY f.filename, f.md5_hash, sr.run_identifier
    ''')
    
    duplicates = cursor.fetchall()
    if not duplicates:
        print("No duplicate files found.")
        return
    
    current_key = None
    for filename, md5_hash, path, run_id in duplicates:
        key = (filename, md5_hash)
        if key != current_key:
            print(f"\nDuplicate file: {filename}")
            print(f"MD5 Hash: {md5_hash}")
            current_key = key
        print(f"  Run '{run_id}': {path}")

def main():
    parser = argparse.ArgumentParser(description='Filesystem crawler and indexer')
    parser.add_argument('run_identifier', help='Unique identifier for this scan run')
    parser.add_argument('drive_name', help='Name of the drive being scanned')
    parser.add_argument('path', help='Base path to start scanning from')
    parser.add_argument('--find-duplicates', action='store_true',
                      help='Show duplicate files after scanning')
    parser.add_argument('--find-modified', action='store_true',
                      help='Show files with same name but different content')
    
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
        
        if args.find_modified:
            find_modified_files(conn)
            
    finally:
        conn.close()

if __name__ == '__main__':
    main()
