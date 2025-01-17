import struct
import requests
import logging
import argparse

#
# This script is a test of looking up data from a binary database
# held online using range requests
#

# Setup logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Constants
URL = "https://astro-downloads.wheep.co.uk/healpix8.bin"
HEADER_FORMAT = '48s B B B 77s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
INDEX_SIZE = 786432  # Number of entries in the index
INDEX_ENTRY_SIZE = 4  # Each entry is a uint32 (4 bytes)
INDEX_TOTAL_BYTES = HEADER_SIZE + (INDEX_SIZE * INDEX_ENTRY_SIZE)
RECORD_FORMAT = "iihhHh"  # Format for the record (int, int, short, short, short)
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

# Function to perform a range request
def fetch_range(url, start, end):
    headers = {"Range": f"bytes={start}-{end}"}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    return response.content

def main(healpixid):
    # Read the header
    header_data = fetch_range(URL, 0, HEADER_SIZE - 1)
    catalogue_title, gaia_version, healpix_level, catalogue_type, spare = struct.unpack(HEADER_FORMAT, header_data)
    catalogue_title = catalogue_title.decode('utf-8').rstrip('\x00')

    print("Catalogue Title:", catalogue_title)
    print("Gaia Version:", gaia_version)
    print("Healpix Level:", healpix_level)
    print("Catalogue Type:", catalogue_type)

    # Find index entry for this healpix
    index_position = (healpixid * INDEX_ENTRY_SIZE) + HEADER_SIZE
    logger.debug(f"Header Size {HEADER_SIZE}.")
    logger.debug(f"Index Position {index_position}.")

    # Fetch the correct number of records for the current healpix ID
    start = 0
    if healpixid == 0:
        numrecords_data = fetch_range(URL, index_position, index_position + INDEX_ENTRY_SIZE - 1 )
        (numrecords,) = struct.unpack("I", numrecords_data)
    else:
        index_position -= INDEX_ENTRY_SIZE
        numrecords_data = fetch_range(URL, index_position , index_position + (INDEX_ENTRY_SIZE * 2) - 1)
        (start,numrecords) = struct.unpack("II", numrecords_data)

    # Calculate the number of records
    numrecords -= start
    logger.debug(f"Start Position: {start}")
    logger.debug(f"Number of Records for Healpix {healpixid}: {numrecords}")

    # Seek to the record position
    record_position = INDEX_TOTAL_BYTES + (start * RECORD_SIZE)
    logger.debug(f"Index Total Bytes: {INDEX_TOTAL_BYTES}")
    logger.debug(f"Record Position: {record_position}")

    # Fetch the record data
    record_data = fetch_range(URL, record_position, record_position + (numrecords * RECORD_SIZE) - 1)

    # Parse the records
    for i in range(numrecords):
        offset = i * RECORD_SIZE
        record = struct.unpack(RECORD_FORMAT, record_data[offset:offset + RECORD_SIZE])

        # Process and print the record
        ra, dec, pmra, pmdec, teff, mag = record
        ra /= 1000000.0
        dec /= 100000.0
        mag /= 1000.0

        print(f"Record for healpixid {healpixid}: ra={ra}, dec={dec}, pmra={pmra}, pmdex={pmdec}, teff={teff}, mag={mag}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read Healpix data from a remote binary file using HTTP range requests.")
    parser.add_argument("healpixid", type=int, help="The Healpix ID to query.")
    args = parser.parse_args()

    main(args.healpixid)

