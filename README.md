# ETL Project

This project performs ETL (Extract, Transform, Load) operations on datasets obtained from a provided link.

## Features

- Automated download, extraction, and cleanup of archive files.
- Extraction of data from CSV, JSON, and XML formats.
- Transformation of data units (e.g., converting height and weight).
- Loading transformed data into a CSV file.

## Usage

### 1. Download and extract data archive

Run the following script to download the archive, extract it into the `source` folder, and remove the archive after extraction:

```bash
python download_extract.py
```
