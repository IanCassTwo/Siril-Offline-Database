import requests
import threading
from bs4 import BeautifulSoup
import os
import shutil
import csv
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

POOL_SIZE = 8
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


            if row[header_indices['has_xp_sampled']] == '"False"' and row[header_indices['has_xp_continuous']] == '"False"':
                continue

            try:
                phot_g_mean_mag = float(row[header_indices['phot_g_mean_mag']])
            except ValueError:
                continue

            if phot_g_mean_mag > 15:
                continue

            accepted += 1

            data_queue.put(f"{row[header_indices['random_index']]},{row[header_indices['ra']]},{row[header_indices['dec']]},{row[header_indices['pmra']]},{row[header_indices['pmdec']]},{row[header_indices['phot_g_mean_mag']]},{row[header_indices['phot_bp_mean_mag']]},{row[header_indices['source_id']]}\n")
        
    os.remove(file_path)
    return (f"Processed {file_path}: Accepted {accepted} lines from {total}")

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
    url = 'https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/'
    links = fetch_urls(url)
    
    print("Starting queue reader")
    write_thread = threading.Thread(target=write_to_file)
    write_thread.start()
    print("Started queue reader")
    
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

    # Signal the worker to stop by putting None into the queue
    # and wait for the worker thread to complete
    data_queue.put(None)
    write_thread.join()

if __name__ == "__main__":
    main()
