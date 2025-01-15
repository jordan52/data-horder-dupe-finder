# Filesystem Crawler

A Python command-line tool that crawls a filesystem and stores file metadata in a SQLite database. It can track multiple scans and identify duplicate files across different locations.

## Features

- Recursively scans filesystem directories
- Stores file metadata including:
  - MD5 checksums
  - Creation and modification times
  - Full and relative paths
- Tracks multiple scan runs with unique identifiers
- Detects duplicate files based on:
  - Filename matches
  - MD5 hash matches
  - Different file locations

## Installation

No additional dependencies required. Just clone the repository and run with Python 3.x.

## Usage

Basic scan:
```bash
python fs_crawler.py "scan_id" "drive_name" "/path/to/scan"
```

Scan and find duplicates:
```bash
python fs_crawler.py "scan_id" "drive_name" "/path/to/scan" --find-duplicates
```

### Arguments

- `run_identifier`: Unique identifier for this scan run
- `drive_name`: Name of the drive being scanned
- `path`: Base path to start scanning from
- `--find-duplicates`: Optional flag to show duplicate files after scanning

## Database Schema

The tool creates a SQLite database (`filesystem.db`) with two tables:

### scan_runs
- `run_id`: Unique identifier for each scan (auto-incrementing)
- `run_identifier`: User-provided scan identifier
- `drive_name`: Name of the scanned drive
- `base_path`: Root path of the scan
- `scan_timestamp`: When the scan was performed

### file_entries
- `file_id`: Unique identifier for each file entry
- `run_id`: References the scan run
- `filename`: Name of the file
- `full_path`: Absolute path to the file
- `relative_path`: Path relative to scan base path
- `md5_hash`: MD5 checksum of file contents
- `created_time`: File creation timestamp
- `modified_time`: File modification timestamp

## Error Handling

The tool handles common filesystem errors:
- Permission denied
- File not found
- Other access errors

Errors are logged to stderr while the scan continues.
