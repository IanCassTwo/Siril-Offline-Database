import sys
import requests
import threading
from bs4 import BeautifulSoup
import os
import shutil
import csv
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

#
# This file will process a single downloaded GAIA source csv in case you need
# to reprocess one. You'll need to import into the database with a COPY command
#

POOL_SIZE = 16
lock = threading.Lock()
data_queue = queue.Queue()

def process_file(file_path):
    print(f"Processing {file_path}")
    with gzip.open(file_path, mode='rt', encoding='utf-8') as file:

        total = 0
        accepted = 0

        header_indices = {}

        for line in file:
            if line.startswith('#'):
                continue

            row = line.strip().split(',')

            if header_indices == {}:
                header_indices = {name: idx for idx, name in enumerate(row)}
                continue

            total += 1

            has_xp_sampled = 1 if row[header_indices['has_xp_sampled']] == '"True"' else 0


            try:
                phot_g_mean_mag = float(row[header_indices['phot_g_mean_mag']])
            except ValueError:
                continue

            if phot_g_mean_mag > 20:
                continue

            pmra = 0
            if row[header_indices['pmra']] != 'null':
                pmra = row[header_indices['pmra']]

            pmdec = 0
            if row[header_indices['pmdec']] != 'null':
                pmdec = row[header_indices['pmdec']]


            teff = 0
            if row[header_indices['teff_gspphot']] != 'null':
                teff = row[header_indices['teff_gspphot']]

            accepted += 1

            data_queue.put(f"{row[header_indices['ra']]},{row[header_indices['dec']]},{pmra},{pmdec},{row[header_indices['phot_g_mean_mag']]},{row[header_indices['source_id']]},{has_xp_sampled},{teff}\n")
        
    os.remove(file_path)
    return (f"Processed {file_path}: Accepted {accepted} lines from {total}")

def write_to_file():
    print(f"Queue reader started")
    #with open('data.csv', 'w') as file_handle:
    with open('data.csv', 'w', buffering=1024*2048) as file_handle:  # buffer
        file_handle.write(f'ra,dec,pmra,pmdec,phot_g_mean_mag,source_id,has_xp_sampled,teff\n')
        while True:
            data = data_queue.get()
            if data is None:
                break  # Stop processing when None is encountered
            file_handle.write(data)

def main():

    if len(sys.argv) > 1:
        filename = sys.argv[1]  # The first command-line argument
    else:
        print(f"{sys.argv[0]} <filename>")
        sys.exit()

    print("Starting queue reader")
    write_thread = threading.Thread(target=write_to_file)
    write_thread.start()
    print("Started queue reader")
    
    process_file(filename)
    data_queue.put(None)
    write_thread.join()

if __name__ == "__main__":
    main()
