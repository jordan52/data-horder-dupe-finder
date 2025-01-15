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

def clear_path_entries(conn, base_path):
    """Delete all file entries for a given base path"""
    cursor = conn.cursor()
    
    # Find all run_ids associated with this base path
    cursor.execute('''
        SELECT run_id 
        FROM scan_runs 
        WHERE base_path = ?
    ''', (base_path,))
    
    run_ids = cursor.fetchall()
    if not run_ids:
        print(f"No scans found for path: {base_path}")
        return
    
    # Delete file entries for these runs
    run_id_list = ','.join(str(rid[0]) for rid in run_ids)
    cursor.execute(f'''
        DELETE FROM file_entries 
        WHERE run_id IN ({run_id_list})
    ''')
    
    # Delete the scan runs
    cursor.execute('''
        DELETE FROM scan_runs 
        WHERE base_path = ?
    ''', (base_path,))
    
    deleted_files = cursor.rowcount
    conn.commit()
    print(f"Cleared {deleted_files} entries for path: {base_path}")

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
        GROUP BY filename, md5_hash, full_path
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
            sr.run_identifier,
            sr.scan_timestamp
        FROM file_entries f
        JOIN scan_runs sr ON f.run_id = sr.run_id
        JOIN DuplicateGroups d ON f.filename = d.filename 
            AND f.md5_hash = d.md5_hash
        GROUP BY f.filename, f.md5_hash, f.full_path, sr.run_identifier
        ORDER BY f.filename, f.md5_hash, sr.run_identifier
    ''')
    
    duplicates = cursor.fetchall()
    if not duplicates:
        print("No duplicate files found.")
        return
    
    current_key = None
    for filename, md5_hash, path, run_id, scan_timestamp in duplicates:
        key = (filename, md5_hash)
        if key != current_key:
            print(f"\nDuplicate file: {filename} at {scan_timestamp}")
            print(f"MD5 Hash: {md5_hash}")
            current_key = key
        print(f"  Run '{run_id}': {path}")

def main():
    parser = argparse.ArgumentParser(description='Filesystem crawler and indexer')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Scan command
    scan_parser = subparsers.add_parser('scan', help='Scan filesystem and store metadata')
    scan_parser.add_argument('run_identifier', help='Unique identifier for this scan run')
    scan_parser.add_argument('drive_name', help='Name of the drive being scanned')
    scan_parser.add_argument('path', help='Base path to start scanning from')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze existing scans')
    analyze_parser.add_argument('analysis_type', choices=['find_duplicates', 'find_modified'],
                              help='Type of analysis to perform')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear entries for a specific path')
    clear_parser.add_argument('base_path', help='Base path to clear entries for')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    init_database()
    conn = sqlite3.connect('filesystem.db')
    
    try:
        if args.command == 'scan':
            if not os.path.exists(args.path):
                print(f"Error: Path '{args.path}' does not exist", file=sys.stderr)
                sys.exit(1)
                
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
            
        elif args.command == 'analyze':
            if args.analysis_type == 'find_duplicates':
                find_duplicates(conn)
            elif args.analysis_type == 'find_modified':
                find_modified_files(conn)
        
        elif args.command == 'clear':
            clear_path_entries(conn, args.base_path)
                
    finally:
        conn.close()

if __name__ == '__main__':
    main()
