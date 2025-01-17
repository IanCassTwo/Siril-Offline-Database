import requests
import threading
from bs4 import BeautifulSoup
import os
import shutil
import csv
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import psycopg2
import json
import traceback
import sys

# 
# This will process one spectra csv file in case you need to reprocess one
#

POOL_SIZE = 8
lock = threading.Lock()
data_queue = queue.Queue()

def fetch_urls(url):
    print(f"Fetching list of urls from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return [url + link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.csv.gz')]

def process_file(file_path):

    connection_string = 'postgresql://stars:stars@localhost:5432/stars2'
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    temp_tablename = f"spectra_temp_{threading.current_thread().ident}"

    # All mean spectra are sampled to the same set of absolute wavelength positions, viz.~343 values from 336~to 1020~nm with a step of 2~nm.
    wavelengths = []
    wavelength = 3.36
    for i in range(343):
        wavelengths.append(wavelength)
        wavelength += 0.2
    
    print(f"Processing {file_path}")
    output_csv = f"{temp_tablename}.csv"
    with gzip.open(file_path, mode='rt', encoding='utf-8') as file, open(output_csv, 'w', newline='') as outfile:

        num = 0
        for line in file:
            if line.startswith('#'):
                continue

            line = line.replace('[', '{').replace(']', '}')
            outfile.write(line.strip() + '\n');
            num += 1

        print(f"[{temp_tablename}] Written {num} lines to {output_csv}")

    # Create temporary table
    print(f"Creating table {temp_tablename}")
    cursor.execute(f"""
        CREATE TEMPORARY TABLE {temp_tablename} (
         source_id BIGINT,
         solution_id BIGINT,
         ra REAL,
         dec REAL,
         wavelength REAL[],
         flux REAL[],
         flux_error REAL[]
        );
    """)
    conn.commit()

    # Bulk load the csv
    print(f"[{temp_tablename}] bulk loading {output_csv}")
    with open(output_csv, 'r') as f:
        cursor.copy_expert(f"""
            COPY {temp_tablename} (source_id, solution_id, ra, dec, flux, flux_error)
            FROM STDIN WITH CSV HEADER
        """, f)

    # Prune the temporary table
    print(f"[{temp_tablename}] Pruning temporary table {temp_tablename}")
    cursor.execute(f"""
        DELETE FROM {temp_tablename} t
        WHERE NOT EXISTS (
            SELECT 1 FROM stars s
            WHERE s.source_id = t.source_id
        )
    """)
    conn.commit()

    # Copy the data
    print(f"[{temp_tablename}] Copying temporary table {temp_tablename} to spectra")
    cursor.execute(f"""
        UPDATE stars
        SET flux = temp_table.flux
        FROM {temp_tablename} AS temp_table
        WHERE stars.source_id = temp_table.source_id;
    """)
    conn.commit()
    print(f"Copied temp table")

    # Clean up
    print(f"[{temp_tablename}] Dropping temporary table {temp_tablename}")
    cursor.execute(f"DROP TABLE {temp_tablename};")
    conn.commit()
    cursor.close()
    conn.close()

    os.remove(file_path)
    os.remove(output_csv)
    return (f"Processed {file_path}")


def main():

    if len(sys.argv) > 1:
        filename = sys.argv[1]  # The first command-line argument
    else:
        print(f"{sys.argv[0]} <filename>")
        sys.exit()

    process_file(filename)

if __name__ == "__main__":
    main()

