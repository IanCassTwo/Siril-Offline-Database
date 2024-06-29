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


POOL_SIZE = 2
lock = threading.Lock()
data_queue = queue.Queue()

def fetch_urls(url):
    print(f"Fetching list of urls from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return [url + link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.csv.gz')]

def download_and_process_file(file_url):
    print(f"Downloading {file_url}")
    file_name = file_url.split('/')[-1]
    file_path = os.path.join('.', file_name)
    
    with requests.get(file_url, stream=True) as r:
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    connection_string = 'postgresql://stars:stars@localhost:5432/stars'
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
        INSERT INTO spectra (source_id, flux, flux_error)
        SELECT source_id, flux, flux_error
        FROM {temp_tablename}
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

def write_to_file():
    print(f"Queue reader started")
    with open('data.csv', 'w') as file_handle:
        file_handle.write(f'random_index,ra,dec,pmra,pmdec,pmag,bmag,source_id\n')
        while True:
            data = data_queue.get()
            if data is None:
                break  # Stop processing when None is encountered
            file_handle.write(data)

def main():
    url = 'https://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_sampled_mean_spectrum/'
    links = fetch_urls(url)
    
    total = len(links)
    finished = 0

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
        futures = {executor.submit(download_and_process_file, link): link for link in links}
        # Wait for all tasks to complete
        for future in as_completed(futures):
            try:
                result = future.result()  # Ensure any exceptions raised are propagated
                finished += 1
                print(f"[{finished}/{total}] {result}")
            except Exception as e:
                print(f"Exception occurred: {str(e)}")
                print(traceback.format_exc())

if __name__ == "__main__":
    main()
