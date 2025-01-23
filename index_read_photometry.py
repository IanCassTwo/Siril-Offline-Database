import struct
import sys
import logging

# Constants
HEADER_FORMAT = '=48s B B B B B I I I 63s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

RECORD_FORMAT = "=iihhhB343e"  # Format for the record 
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)  # Size of each record in bytes

INT32_MAX = 2**31 -1
RADEC_SCALE = INT32_MAX / 360.0

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Function to read a specific record
def read_record(file_path, healpixid):
    logger.debug(f"Starting to read record for healpixid {healpixid} from file {file_path}")


    try:
        with open(file_path, "rb") as f:
            logger.debug(f"Opened file {file_path} successfully.")

            # Read the header
            header_data = f.read(HEADER_SIZE)
            catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, spare = struct.unpack(HEADER_FORMAT, header_data)
            catalogue_title = catalogue_title.decode('utf-8').rstrip('\x00')
            print("Catalogue Title:", catalogue_title)
            print("Gaia Version:", gaia_version)
            print("Healpix Level:", healpix_level)
            print("Catalogue Type:", catalogue_type)
            print("Catalogue Level:", catalogue_level)
            print("Chunked :", chunked)
            print("Chunk Healpix :", chunk_healpix)
            print("First Healpix :", first_healpix)
            print("Last Healpix :", last_healpix)
            print(f"Header Size {HEADER_SIZE}.")

            indexoffset = healpixid - first_healpix

            INDEX_SIZE = last_healpix - first_healpix + 1  # Number of entries in the index
            #INDEX_SIZE = 3  # Number of entries in the index
            INDEX_ENTRY_SIZE = 4  # Each entry is a uint16 (2 bytes)
            INDEX_TOTAL_BYTES = HEADER_SIZE + (INDEX_SIZE * INDEX_ENTRY_SIZE) # Total size of the index

            # Find index entry for this healpix
            start = 0
            index_position = (indexoffset * INDEX_ENTRY_SIZE) + HEADER_SIZE
            print(f"Index Entries {INDEX_SIZE}, Index Size {INDEX_TOTAL_BYTES}.")

            if healpixid < 0:
                return

            logger.debug(f"Seeking to index position {index_position}.")
            f.seek(index_position)
            numrecords_data = f.read(INDEX_ENTRY_SIZE)
            (numrecords,) = struct.unpack("I", numrecords_data)

            # Start position is the current entry minus the previous entry + 1
            # but only if we're not already at the start of the file
            if indexoffset != 0:
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
                ra, dec, pmra, pmdec, mag, expo = record[:6]
                flux = record[6:]
                ra /= RADEC_SCALE
                dec /= RADEC_SCALE
                mag /= 1000

                print(f"Record for healpixid {healpixid}: ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, mag={mag}, expo={expo}, flux = {flux}")
            print(f"Numrecords = {numrecords}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        print(f"File not found: {file_path}")
    except Exception as e:
        logger.exception("An error occurred while reading the record.")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Invalid number of arguments. Usage: python script.py <filename> [healpixid]")
        print("Usage: python script.py <chunk> [healpixid]")
        sys.exit(1)

    filename = sys.argv[1]
    healpixid = -1
    if len(sys.argv) > 2:
        healpixid = int(sys.argv[2])
    logger.debug(f"Received healpixid: {healpixid}")
    read_record(filename, healpixid)

