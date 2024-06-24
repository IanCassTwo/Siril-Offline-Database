import requests
from bs4 import BeautifulSoup
import os
import shutil
import csv
import gzip

# Fetch the urls of the csv files in the gaia archive
url = 'https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
links = soup.find_all('a')

# Create our output file
output = open('data.csv', 'w')
output.write(f'random_index,ra,dec,pmra,pmdec,pmag,bmag,teff,source_id\n')

# Iterate over the links
total_links = len(links)
for index, link in enumerate(links):
    href = link.get('href')
    
    # Skip parent directory link and non-csv.gz links
    if href == '../' or not href.endswith('.csv.gz'):
        continue
    
    # Download the file
    file_url = url + href
    print(f"({index}/{total_links}) Downloading {file_url}...")
    file_name = "gaia.csv.gz"
    file_path = os.path.join('.', file_name)
    
    with requests.get(file_url, stream=True) as r:
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    
    print(f"Downloaded {file_url}")

    # Open the compressed CSV file
    with gzip.open(file_path, mode='rt', encoding='utf-8') as file:
        csv_reader = csv.DictReader((line for line in file if not line.startswith('#')))

        print(f"Parsing data")
        total = 0
        accepted = 0
        for row in csv_reader:

            total += 1

            if row['has_xp_sampled'] != "True":
                continue

            try:
                phot_g_mean_mag = float(row['phot_g_mean_mag'])
            except ValueError:
                continue  # Skip if 'phot_g_mean_mag' cannot be converted to float

            if phot_g_mean_mag > 12.3:
                continue

            accepted += 1

            output.write(f"{row['random_index']},{row['ra']},{row['dec']},{row['pmra']},{row['pmdec']},{row['phot_g_mean_mag']},{row['phot_bp_mean_mag']},{row['teff_gspphot']},{row['source_id']}\n")
        print(f"Accepted {accepted} lines from {total}")
    
    
print("All files processed.")

