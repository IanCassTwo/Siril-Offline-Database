import struct
import sys
import logging

#
# This script allows testing of the astrometry binary file
#

# Constants
HEADER_FORMAT = '48s B B B 77s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
INDEX_SIZE = 786432  # Number of entries in the index
INDEX_ENTRY_SIZE = 4  # Each entry is a uint16 (2 bytes)
INDEX_TOTAL_BYTES = HEADER_SIZE + (INDEX_SIZE * INDEX_ENTRY_SIZE) # Total size of the index
RECORD_FORMAT = "iihhHh"  # Format for the record (int, int, short, short, short)
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)  # Size of each record in bytes


# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Function to read a specific record
def read_record(file_path, healpixid):
    logger.debug(f"Starting to read record for healpixid {healpixid} from file {file_path}")

    if not (0 <= healpixid <= INDEX_SIZE):
        logger.error(f"healpixid {healpixid} is out of range (0 to {INDEX_SIZE - 1}).")
        raise ValueError(f"healpixid must be between 0 and {INDEX_SIZE}.")

    try:
        with open(file_path, "rb") as f:
            logger.debug(f"Opened file {file_path} successfully.")

            # Read the header
            header_data = f.read(HEADER_SIZE)
            catalogue_title, gaia_version, healpix_level, catalogue_type, spare = struct.unpack(HEADER_FORMAT, header_data)
            catalogue_title = catalogue_title.decode('utf-8').rstrip('\x00')
            print("Catalogue Title:", catalogue_title)
            print("Gaia Version:", gaia_version)
            print("Healpix Level:", healpix_level)
            print("Catalogue Type:", catalogue_type)

            start = 0

            # Find index entry for this healpix
            index_position = (healpixid * INDEX_ENTRY_SIZE) + HEADER_SIZE
            logger.debug(f"Header Size {HEADER_SIZE}.")
            logger.debug(f"Seeking to index position {index_position}.")
            f.seek(index_position)
            numrecords_data = f.read(INDEX_ENTRY_SIZE)
            (numrecords,) = struct.unpack("I", numrecords_data)

            # Start position is the current entry minus the previous entry + 1
            # but only if we're not already at the start of the file
            if healpixid != 0:
                f.seek(index_position - INDEX_ENTRY_SIZE)
                start_data =  f.read(INDEX_ENTRY_SIZE)
                (start,) = struct.unpack("I", start_data)

            # Number of records to reach is healpix index value minus the start position
            numrecords -= start

            logger.debug(f"Start position is {start}")
            logger.debug(f"Number of records for healpix {healpixid} is {numrecords}")
                
            # Seek to the end of the index and then to the record offset
            logger.debug(f"INDEX_TOTAL_BYTES = {INDEX_TOTAL_BYTES}")
            record_position = INDEX_TOTAL_BYTES + (start * RECORD_SIZE)
            logger.debug(f"Seeking to record position {record_position}.")
            f.seek(record_position)

            # Read the record
            for i in range(numrecords):
                record_data = f.read(RECORD_SIZE)
                #logger.debug(f"Read record data ({RECORD_SIZE}): {record_data}")

                # Unpack the record
                record = struct.unpack(RECORD_FORMAT, record_data)
                #logger.debug(f"Unpacked record: {record}")

                # Process and print the record
                ra, dec, pmra, pmdec, teff, mag = record
                ra /= 1000000.0
                dec /= 100000.0
                mag /= 1000.0

                print(f"Record for healpixid {healpixid}: ra={ra}, dec={dec}, pmra={pmra}, pmdex={pmdec}, teff={teff}, mag={mag}")
            print(f"Numrecords = {numrecords}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        print(f"File not found: {file_path}")
    except Exception as e:
        logger.exception("An error occurred while reading the record.")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Invalid number of arguments. Usage: python script.py <healpixid>")
        print("Usage: python script.py <healpixid>")
        sys.exit(1)

    try:
        healpixid = int(sys.argv[1])
        logger.debug(f"Received healpixid: {healpixid}")
        read_record("healpix8-no-spcc-index-header.bin", healpixid)
    except ValueError:
        logger.error("Invalid healpixid provided. Must be an integer between 0 and 786431.")
        print("Please provide a valid integer healpixid between 0 and 786431.")

