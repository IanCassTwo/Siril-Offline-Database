import struct
import sys
import logging
import pytest

# Constants
HEADER_FORMAT = '=48s B B B B B I I I 63s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
RECORD_FORMAT = "=iihhhB343e"  # Format for the record (int, int, short, short, short)
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)  # Size of each record in bytes
INT32_MAX = 2**31 -1
RADEC_SCALE = INT32_MAX / 360.0

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Function to read a specific record
def read_record(file_path, healpixid) :
    logger.debug(f"Starting to read record for healpix {healpixid} from file {file_path}")

    try:
        with open(file_path, "rb") as f:
            logger.debug(f"Opened file {file_path} successfully.")

           # Read the header
            header_data = f.read(HEADER_SIZE)
            catalogue_title, gaia_version, healpix_level, catalogue_type, chunked, catalogue_level, chunk_healpix, first_healpix, last_healpix, spare = struct.unpack(HEADER_FORMAT, header_data)
            catalogue_title = catalogue_title.decode('utf-8').rstrip('\x00')
            print("Catalogue Title:", catalogue_title)
            print("Gaia Version:", gaia_version)
            print("Healpix Level:", healpix_level)
            print("Catalogue Type:", catalogue_type)
            print("Chunked :", chunked)
            print("Catalogue Level:", catalogue_level)
            print("Chunk Healpix :", chunk_healpix)
            print("First Healpix :", first_healpix)
            print("Last Healpix :", last_healpix)
            print(f"Header Size {HEADER_SIZE}.")

            logger.debug(f"Healpixid is {healpixid}")
            indexoffset = healpixid - first_healpix 
            logger.debug(f"indexoffset is {indexoffset}")

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
            records = []
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

                records.append(f"ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, mag={mag}, flux0={flux[0]}")

                #print(f"Record for healpixid {healpixid}: ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, teff={teff}, mag={mag}")
            #print(f"Numrecords = {numrecords}")
            return catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        print(f"File not found: {file_path}")
    except Exception as e:
        logger.exception("An error occurred while reading the record.")
        print(f"An error occurred: {e}")


def test_header():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_64.dat", 262145)
    assert catalogue_title == "Siril Gaia DR3 photometric extract chunk 64"
    assert gaia_version == 3
    assert healpix_level == 8
    assert catalogue_type == 2
    assert catalogue_level == 2
    assert chunked == 1


def test_healpix0():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_0.dat", 0)
    assert len(records) == 9
    assert records[0] == "ra=45.00497987726935, dec=0.01987952739926033, pmra=29, pmdec=19, mag=14.128, flux0=2.72265625"


def test_healpix5000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_1.dat", 5000)
    assert len(records) == 23
    assert records[0] == "ra=54.479144948757785, dec=16.05020983892037, pmra=8, pmdec=-11, mag=14.242, flux0=1.7548828125"


def test_healpix10000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_2.dat", 10000)
    assert len(records) == 13
    assert records[0] == "ra=40.10356996213252, dec=20.16860599637432, pmra=1, pmdec=-4, mag=14.065, flux0=6.71875"


def test_healpix15000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_3.dat", 15000)
    assert len(records) == 12
    assert records[0] == "ra=35.611724981857336, dec=29.778068899073666, pmra=5, pmdec=5, mag=12.868, flux0=1.9052734375"


def test_healpix20000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_4.dat", 20000)
    assert len(records) == 41
    assert records[0] == "ra=64.01708993316493, dec=33.57240693344381, pmra=1, pmdec=0, mag=13.444, flux0=0.3291015625"


def test_healpix25000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_6.dat", 25000)
    assert len(records) == 29
    assert records[0] == "ra=56.58437996012362, dec=35.370692962487546, pmra=-4, pmdec=-7, mag=14.778, flux0=1.814453125"


def test_healpix30000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_7.dat", 30000)
    assert len(records) == 37
    assert records[0] == "ra=84.66359986209477, dec=52.84729384484109, pmra=0, pmdec=-5, mag=13.726, flux0=3.78125"


def test_healpix35000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_8.dat", 35000)
    assert len(records) == 14
    assert records[0] == "ra=15.153563458078336, dec=27.653639999988318, pmra=-5, pmdec=-1, mag=13.5, flux0=0.93603515625"


def test_healpix40000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_9.dat", 40000)
    assert len(records) == 24
    assert records[0] == "ra=34.8397029167226, dec=43.428706994014185, pmra=-2, pmdec=-1, mag=14.734, flux0=3.34765625"


def test_healpix45000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_10.dat", 45000)
    assert len(records) == 88
    assert records[0] == "ra=2.5821716722949275, dec=51.662986917264284, pmra=-1, pmdec=-1, mag=13.875, flux0=0.62548828125"


def test_healpix50000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_12.dat", 50000)
    assert len(records) == 57
    assert records[0] == "ra=47.594565845837145, dec=50.53219995020525, pmra=-2, pmdec=-3, mag=11.85, flux0=3.556640625"


def test_healpix55000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_13.dat", 55000)
    assert len(records) == 54
    assert records[0] == "ra=75.23923989163676, dec=67.57481388169099, pmra=-1, pmdec=0, mag=13.462, flux0=0.34033203125"


def test_healpix60000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_14.dat", 60000)
    assert len(records) == 35
    assert records[0] == "ra=8.107635401239449, dec=65.72124484261555, pmra=-5, pmdec=0, mag=14.152, flux0=1.87890625"


def test_healpix65000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_15.dat", 65000)
    assert len(records) == 29
    assert records[0] == "ra=62.40505985096332, dec=85.26288984588481, pmra=3, pmdec=-15, mag=10.158, flux0=2.2734375"


def test_healpix70000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_17.dat", 70000)
    assert len(records) == 18
    assert records[0] == "ra=150.47172983664632, dec=14.509916945598048, pmra=0, pmdec=-1, mag=14.296, flux0=3.328125"


def test_healpix75000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_18.dat", 75000)
    assert len(records) == 20
    assert records[0] == "ra=129.0591399413809, dec=18.571025979971058, pmra=0, pmdec=-5, mag=14.155, flux0=3.173828125"


def test_healpix80000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_19.dat", 80000)
    assert len(records) == 15
    assert records[0] == "ra=127.9319199770372, dec=25.982266881494905, pmra=-4, pmdec=-1, mag=13.545, flux0=0.358642578125"


def test_healpix85000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_20.dat", 85000)
    assert len(records) == 15
    assert records[0] == "ra=157.1522199908049, dec=30.425054929417115, pmra=-2, pmdec=-7, mag=12.988, flux0=0.6240234375"


def test_healpix90000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_21.dat", 90000)
    assert len(records) == 7
    assert records[0] == "ra=174.90967984074246, dec=50.48644599061759, pmra=-14, pmdec=-1, mag=14.517, flux0=2.486328125"


def test_healpix95000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_23.dat", 95000)
    assert len(records) == 8
    assert records[0] == "ra=161.8427998953698, dec=49.404616881816004, pmra=14, pmdec=-21, mag=13.078, flux0=0.640625"


def test_healpix100000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_24.dat", 100000)
    assert len(records) == 15
    assert records[0] == "ra=113.21837994885554, dec=29.359857863448493, pmra=0, pmdec=-2, mag=14.952, flux0=2.755859375"


def test_healpix105000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_25.dat", 105000)
    assert len(records) == 23
    assert records[0] == "ra=114.26519985975008, dec=39.89779993886957, pmra=-1, pmdec=-8, mag=14.831, flux0=2.021484375"


def test_healpix110000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_26.dat", 110000)
    assert len(records) == 32
    assert records[0] == "ra=98.03558996787089, dec=48.192579936325814, pmra=0, pmdec=-14, mag=13.82, flux0=3.91015625"


def test_healpix115000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_28.dat", 115000)
    assert len(records) == 6
    assert records[0] == "ra=137.77845998191202, dec=47.12644996453377, pmra=1, pmdec=-4, mag=14.851, flux0=2.37109375"


def test_healpix120000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_29.dat", 120000)
    assert len(records) == 10
    assert records[0] == "ra=165.0947999605419, dec=63.501919891453305, pmra=1, pmdec=-6, mag=14.427, flux0=3.38671875"


def test_healpix125000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_30.dat", 125000)
    assert len(records) == 21
    assert records[0] == "ra=107.97916994801683, dec=62.33455295783214, pmra=-3, pmdec=-27, mag=13.285, flux0=0.798828125"


def test_healpix130000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_31.dat", 130000)
    assert len(records) == 28
    assert records[0] == "ra=106.17156994816455, dec=82.00650990102743, pmra=4, pmdec=1, mag=14.083, flux0=5.0390625"


def test_healpix135000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_32.dat", 135000)
    assert len(records) == 9
    assert records[0] == "ra=226.82061988246656, dec=16.739354998217593, pmra=-9, pmdec=-8, mag=13.213, flux0=1.029296875"


def test_healpix140000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_34.dat", 140000)
    assert len(records) == 10
    assert records[0] == "ra=210.2732099640524, dec=15.196534961087877, pmra=23, pmdec=-27, mag=14.618, flux0=2.248046875"


def test_healpix145000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_35.dat", 145000)
    assert len(records) == 10
    assert records[0] == "ra=228.15849987238573, dec=29.684091838860926, pmra=-16, pmdec=-4, mag=14.606, flux0=3.00390625"


def test_healpix150000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_36.dat", 150000)
    assert len(records) == 11
    assert records[0] == "ra=244.69828997026116, dec=31.415454908933235, pmra=-1, pmdec=-10, mag=14.206, flux0=5.171875"


def test_healpix155000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_37.dat", 155000)
    assert len(records) == 21
    assert records[0] == "ra=268.40471993591854, dec=48.57493184906194, pmra=-2, pmdec=-3, mag=14.886, flux0=2.701171875"


def test_healpix160000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_39.dat", 160000)
    assert len(records) == 15
    assert records[0] == "ra=252.07197994555904, dec=45.0430298620104, pmra=-1, pmdec=-21, mag=14.667, flux0=2.744140625"


def test_healpix165000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_40.dat", 165000)
    assert len(records) == 11
    assert records[0] == "ra=206.39957996383288, dec=26.324454893509134, pmra=-7, pmdec=-11, mag=13.235, flux0=0.5322265625"


def test_healpix170000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_41.dat", 170000)
    assert len(records) == 10
    assert records[0] == "ra=208.84276995847128, dec=36.59296387647882, pmra=0, pmdec=-4, mag=14.97, flux0=0.9677734375"


def test_healpix175000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_42.dat", 175000)
    assert len(records) == 7
    assert records[0] == "ra=182.06666995867465, dec=44.76225991023809, pmra=13, pmdec=-11, mag=14.991, flux0=1.3447265625"


def test_healpix180000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_43.dat", 180000)
    assert len(records) == 14
    assert records[0] == "ra=186.89988990635604, dec=61.218593912766586, pmra=-7, pmdec=-15, mag=14.426, flux0=3.541015625"


def test_healpix185000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_45.dat", 185000)
    assert len(records) == 11
    assert records[0] == "ra=234.46708984415375, dec=60.15718999326098, pmra=-2, pmdec=-10, mag=14.746, flux0=2.5625"


def test_healpix190000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_46.dat", 190000)
    assert len(records) == 9
    assert records[0] == "ra=209.12309986032687, dec=65.02589992481558, pmra=-8, pmdec=2, mag=13.38, flux0=0.55615234375"


def test_healpix195000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_47.dat", 195000)
    assert len(records) == 14
    assert records[0] == "ra=206.10346995578308, dec=78.68342995582307, pmra=-6, pmdec=-16, mag=14.201, flux0=3.2421875"


def test_healpix200000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_48.dat", 200000)
    assert len(records) == 35
    assert records[0] == "ra=319.2167398329902, dec=13.258213984434592, pmra=0, pmdec=-12, mag=14.426, flux0=3.263671875"


def test_healpix205000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_50.dat", 205000)
    assert len(records) == 95
    assert records[0] == "ra=303.43395997930037, dec=12.363286899571905, pmra=-7, pmdec=-9, mag=14.23, flux0=3.265625"


def test_healpix210000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_51.dat", 210000)
    assert len(records) == 55
    assert records[0] == "ra=322.7508499486143, dec=26.62712994340208, pmra=-9, pmdec=-7, mag=14.765, flux0=3.28125"


def test_healpix215000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_52.dat", 215000)
    assert len(records) == 37
    assert records[0] == "ra=343.422939937293, dec=33.92636498153506, pmra=6, pmdec=-5, mag=14.813, flux0=1.5615234375"


def test_healpix220000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_53.dat", 220000)
    assert len(records) == 52
    assert records[0] == "ra=345.28502985149856, dec=44.23068195778443, pmra=-13, pmdec=-6, mag=14.223, flux0=3.548828125"


def test_healpix225000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_54.dat", 225000)
    assert len(records) == 127
    assert records[0] == "ra=323.682699987517, dec=49.32389999242681, pmra=-1, pmdec=-8, mag=13.27, flux0=1.0078125"


def test_healpix230000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_56.dat", 230000)
    assert len(records) == 127
    assert records[0] == "ra=291.0783998533517, dec=24.642299965323087, pmra=-2, pmdec=-5, mag=13.81, flux0=3.955078125"


def test_healpix235000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_57.dat", 235000)
    assert len(records) == 88
    assert records[0] == "ra=311.6933998613122, dec=43.83944288074944, pmra=-2, pmdec=-9, mag=12.445, flux0=0.278076171875"


def test_healpix240000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_58.dat", 240000)
    assert len(records) == 29
    assert records[0] == "ra=277.0675399140769, dec=40.276849884668756, pmra=0, pmdec=-8, mag=14.635, flux0=2.830078125"


def test_healpix245000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_59.dat", 245000)
    assert len(records) == 48
    assert records[0] == "ra=285.51129985857347, dec=57.826569964097146, pmra=-2, pmdec=-14, mag=14.523, flux0=2.73828125"


def test_healpix250000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_61.dat", 250000)
    assert len(records) == 127
    assert records[0] == "ra=330.01311995555324, dec=56.660349842468435, pmra=0, pmdec=-2, mag=12.948, flux0=0.94677734375"


def test_healpix255000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_62.dat", 255000)
    assert len(records) == 60
    assert records[0] == "ra=306.17367991533763, dec=61.6131299275966, pmra=-6, pmdec=-4, mag=14.843, flux0=2.33203125"


def test_healpix260000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_63.dat", 260000)
    assert len(records) == 27
    assert records[0] == "ra=332.290799884261, dec=80.5194239879583, pmra=-3, pmdec=0, mag=13.238, flux0=0.202392578125"


def test_healpix265000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_64.dat", 265000)
    assert len(records) == 1
    assert records[0] == "ra=353.2893998703404, dec=-28.727669915523226, pmra=28, pmdec=-3, mag=13.483, flux0=0.6650390625"


def test_healpix270000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_65.dat", 270000)
    assert len(records) == 4
    assert records[0] == "ra=7.016552838970233, dec=-14.35956991014982, pmra=22, pmdec=-3, mag=14.171, flux0=4.1484375"


def test_healpix275000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_67.dat", 275000)
    assert len(records) == 3
    assert records[0] == "ra=356.8315398492066, dec=-15.396061975227697, pmra=17, pmdec=-2, mag=11.304, flux0=5.078125"


def test_healpix280000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_68.dat", 280000)
    assert len(records) == 13
    assert records[0] == "ra=30.93691395173637, dec=-9.567002863421571, pmra=32, pmdec=-26, mag=12.422, flux0=2.244140625"


def test_healpix285000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_69.dat", 285000)
    assert len(records) == 11
    assert records[0] == "ra=31.999036870896365, dec=-0.8845328543728835, pmra=2, pmdec=-18, mag=11.893, flux0=4.7265625"


def test_healpix290000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_70.dat", 290000)
    assert len(records) == 4
    assert records[0] == "ra=11.968748873085131, dec=3.1151442057989276, pmra=-14, pmdec=-22, mag=12.021, flux0=3.953125"


def test_healpix295000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_72.dat", 295000)
    assert len(records) == 13
    assert records[0] == "ra=339.22664991543934, dec=-17.155019946934196, pmra=5, pmdec=1, mag=13.863, flux0=5.36328125"


def test_healpix300000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_73.dat", 300000)
    assert len(records) == 7
    assert records[0] == "ra=348.0292399730669, dec=-1.7638898835349315, pmra=-1, pmdec=-11, mag=14.987, flux0=2.10546875"


def test_healpix305000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_74.dat", 305000)
    assert len(records) == 15
    assert records[0] == "ra=332.19625989543096, dec=2.1296863640331134, pmra=1, pmdec=-9, mag=12.581, flux0=0.95361328125"


def test_healpix310000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_75.dat", 310000)
    assert len(records) == 17
    assert records[0] == "ra=329.03481992382314, dec=10.868458957815756, pmra=-3, pmdec=-6, mag=14.741, flux0=3.18359375"


def test_healpix315000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_76.dat", 315000)
    assert len(records) == 15
    assert records[0] == "ra=358.26109992259234, dec=14.852640896501317, pmra=10, pmdec=-1, mag=14.529, flux0=3.466796875"


def test_healpix325000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_79.dat", 325000)
    assert len(records) == 19
    assert records[0] == "ra=6.727772972885413, dec=29.061200930299794, pmra=3, pmdec=-1, mag=12.848, flux0=1.380859375"


def test_healpix330000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_80.dat", 330000)
    assert len(records) == 27
    assert records[0] == "ra=87.86848994338348, dec=-32.056857921209584, pmra=-3, pmdec=-4, mag=14.163, flux0=4.63671875"


def test_healpix335000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_81.dat", 335000)
    assert len(records) == 62
    assert records[0] == "ra=100.20994584085882, dec=-17.249822959885847, pmra=-1, pmdec=-4, mag=12.475, flux0=2.1484375"


def test_healpix340000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_83.dat", 340000)
    assert len(records) == 38
    assert records[0] == "ra=89.3703399083439, dec=-18.75694799179069, pmra=-19, pmdec=-19, mag=13.15, flux0=1.1630859375"


def test_healpix345000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_84.dat", 345000)
    assert len(records) == 127
    assert records[0] == "ra=110.03516999540625, dec=-12.324636938201559, pmra=-2, pmdec=6, mag=13.816, flux0=0.6826171875"


def test_healpix350000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_85.dat", 350000)
    assert len(records) == 27
    assert records[0] == "ra=129.36062995780287, dec=1.2304622126885048, pmra=-2, pmdec=1, mag=14.935, flux0=1.001953125"


def test_healpix355000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_86.dat", 355000)
    assert len(records) == 37
    assert records[0] == "ra=91.12694994133288, dec=0.3663543799735393, pmra=-6, pmdec=-11, mag=14.307, flux0=1.298828125"


def test_healpix360000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_87.dat", 360000)
    assert len(records) == 32
    assert records[0] == "ra=111.08814993458247, dec=13.255806869480669, pmra=1, pmdec=-4, mag=14.9, flux0=1.8466796875"


def test_healpix365000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_89.dat", 365000)
    assert len(records) == 21
    assert records[0] == "ra=81.19875997360738, dec=-4.467296565169141, pmra=-1, pmdec=3, mag=14.301, flux0=3.16015625"


def test_healpix370000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_90.dat", 370000)
    assert len(records) == 20
    assert records[0] == "ra=66.80095986779824, dec=-0.5762169512809333, pmra=0, pmdec=-13, mag=14.269, flux0=3.3515625"


def test_healpix375000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_91.dat", 375000)
    assert len(records) == 11
    assert records[0] == "ra=62.29441990251393, dec=8.152356877993958, pmra=0, pmdec=7, mag=12.213, flux0=1.072265625"


def test_healpix380000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_92.dat", 380000)
    assert len(records) == 86
    assert records[0] == "ra=90.69204986593316, dec=11.435900894662318, pmra=0, pmdec=-4, mag=14.618, flux0=1.501953125"


def test_healpix385000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_93.dat", 385000)
    assert len(records) == 61
    assert records[0] == "ra=100.20318583594783, dec=28.306209886589183, pmra=4, pmdec=-10, mag=14.591, flux0=2.884765625"


def test_healpix390000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_95.dat", 390000)
    assert len(records) == 107
    assert records[0] == "ra=91.40333599010637, dec=27.285602869133278, pmra=1, pmdec=-5, mag=14.198, flux0=4.20703125"


def test_healpix395000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_96.dat", 395000)
    assert len(records) == 23
    assert records[0] == "ra=182.4459998600399, dec=-28.26465391938791, pmra=-6, pmdec=19, mag=12.761, flux0=1.4453125"


def test_healpix400000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_97.dat", 400000)
    assert len(records) == 15
    assert records[0] == "ra=181.44594996303596, dec=-20.682979924922332, pmra=-24, pmdec=-20, mag=14.871, flux0=1.3623046875"


def test_healpix410000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_100.dat", 410000)
    assert len(records) == 16
    assert records[0] == "ra=204.65911984660622, dec=-15.01241287915614, pmra=-6, pmdec=-20, mag=13.486, flux0=0.8486328125"


def test_healpix415000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_101.dat", 415000)
    assert len(records) == 12
    assert records[0] == "ra=222.5346198596687, dec=-1.4513966075384042, pmra=-12, pmdec=-10, mag=14.029, flux0=4.234375"


def test_healpix420000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_102.dat", 420000)
    assert len(records) == 4
    assert records[0] == "ra=183.52117994032855, dec=-2.9564910395799626, pmra=-11, pmdec=2, mag=13.682, flux0=0.40185546875"


def test_healpix425000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_103.dat", 425000)
    assert len(records) == 6
    assert records[0] == "ra=201.3809799781912, dec=10.59555994840132, pmra=23, pmdec=-38, mag=9.88, flux0=3.302734375"


def test_healpix430000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_104.dat", 430000)
    assert len(records) == 9
    assert records[0] == "ra=156.06121990646292, dec=-2.3569328348883114, pmra=-10, pmdec=2, mag=13.099, flux0=1.7236328125"


def test_healpix435000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_106.dat", 435000)
    assert len(records) == 21
    assert records[0] == "ra=145.93410994202554, dec=-3.190071882302906, pmra=-29, pmdec=-3, mag=12.823, flux0=1.9873046875"


def test_healpix440000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_107.dat", 440000)
    assert len(records) == 4
    assert records[0] == "ra=160.28279997421558, dec=9.662286997615492, pmra=-16, pmdec=-9, mag=13.929, flux0=5.73046875"


def test_healpix445000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_108.dat", 445000)
    assert len(records) == 3
    assert records[0] == "ra=172.72434987720303, dec=8.784836851425858, pmra=-21, pmdec=-5, mag=14.787, flux0=1.318359375"


def test_healpix450000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_109.dat", 450000)
    assert len(records) == 7
    assert records[0] == "ra=194.80034997444614, dec=25.370183934164316, pmra=-70, pmdec=26, mag=13.672, flux0=0.75244140625"


def test_healpix455000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_111.dat", 455000)
    assert len(records) == 8
    assert records[0] == "ra=184.52811992938075, dec=24.41637984682637, pmra=-2, pmdec=-33, mag=13.443, flux0=0.49609375"


def test_healpix460000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_112.dat", 460000)
    assert len(records) == 127
    assert records[0] == "ra=274.92412985997464, dec=-32.08210991326818, pmra=-4, pmdec=-10, mag=12.336, flux0=2.66015625"


def test_healpix465000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_113.dat", 465000)
    assert len(records) == 127
    assert records[0] == "ra=275.9828198868701, dec=-22.33693896901651, pmra=-1, pmdec=-6, mag=14.138, flux0=0.09466552734375"


def test_healpix470000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_114.dat", 470000)
    assert len(records) == 17
    assert records[0] == "ra=253.10849983855545, dec=-15.671947456743542, pmra=-9, pmdec=1, mag=14.823, flux0=1.1826171875"


def test_healpix475000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_115.dat", 475000)
    assert len(records) == 27
    assert records[0] == "ra=271.05079993188883, dec=-2.0827119993431085, pmra=-5, pmdec=0, mag=14.533, flux0=0.01218414306640625"


def test_healpix480000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_117.dat", 480000)
    assert len(records) == 55
    assert records[0] == "ra=303.75929986301776, dec=-4.732123298911435, pmra=0, pmdec=-15, mag=13.654, flux0=0.60009765625"


def test_healpix485000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_118.dat", 485000)
    assert len(records) == 61
    assert records[0] == "ra=282.28487985315024, dec=-0.8740960438149497, pmra=-7, pmdec=-8, mag=9.232, flux0=0.0005679130554199219"


def test_healpix490000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_119.dat", 490000)
    assert len(records) == 39
    assert records[0] == "ra=284.8213798761467, dec=7.8533582984718295, pmra=30, pmdec=-10, mag=14.439, flux0=2.251953125"


def test_healpix495000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_120.dat", 495000)
    assert len(records) == 35
    assert records[0] == "ra=249.24267992807674, dec=-5.066009371106517, pmra=-22, pmdec=3, mag=14.955, flux0=0.5556640625"


def test_healpix500000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_122.dat", 500000)
    assert len(records) == 9
    assert records[0] == "ra=238.40307986289406, dec=-6.511182918451346, pmra=-9, pmdec=-5, mag=14.641, flux0=1.8623046875"


def test_healpix505000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_123.dat", 505000)
    assert len(records) == 18
    assert records[0] == "ra=250.6748999146162, dec=6.906868930397028, pmra=-2, pmdec=2, mag=13.463, flux0=0.787109375"


def test_healpix510000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_124.dat", 510000)
    assert len(records) == 71
    assert records[0] == "ra=264.40119994077884, dec=6.019524450422974, pmra=-4, pmdec=-4, mag=14.285, flux0=2.0546875"


def test_healpix515000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_125.dat", 515000)
    assert len(records) == 64
    assert records[0] == "ra=273.8496399455935, dec=22.36536183504637, pmra=-3, pmdec=2, mag=14.658, flux0=2.708984375"


def test_healpix520000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_126.dat", 520000)
    assert len(records) == 23
    assert records[0] == "ra=260.1421499159849, dec=25.965314985236763, pmra=1, pmdec=6, mag=13.322, flux0=1.3076171875"


def test_healpix525000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_128.dat", 525000)
    assert len(records) == 15
    assert records[0] == "ra=21.976832962584137, dec=-83.68439492009784, pmra=20, pmdec=-16, mag=14.487, flux0=2.896484375"


def test_healpix530000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_129.dat", 530000)
    assert len(records) == 35
    assert records[0] == "ra=78.38434990420208, dec=-67.17586496247716, pmra=0, pmdec=0, mag=14.883, flux0=2.83984375"


def test_healpix535000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_130.dat", 535000)
    assert len(records) == 11
    assert records[0] == "ra=18.950570960971792, dec=-65.26780986472396, pmra=30, pmdec=-5, mag=14.981, flux0=2.6484375"


def test_healpix540000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_131.dat", 540000)
    assert len(records) == 11
    assert records[0] == "ra=49.08917183479721, dec=-48.84231992477659, pmra=-5, pmdec=26, mag=14.439, flux0=3.267578125"


def test_healpix545000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_133.dat", 545000)
    assert len(records) == 22
    assert records[0] == "ra=84.08162990775034, dec=-50.05106999075555, pmra=2, pmdec=3, mag=12.463, flux0=0.11865234375"


def test_healpix550000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_134.dat", 550000)
    assert len(records) == 12
    assert records[0] == "ra=64.50185395055536, dec=-44.9182318732693, pmra=21, pmdec=20, mag=14.823, flux0=0.74365234375"


def test_healpix555000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_135.dat", 555000)
    assert len(records) == 19
    assert records[0] == "ra=72.76192498987629, dec=-25.556273919323583, pmra=-1, pmdec=1, mag=14.747, flux0=2.279296875"


def test_healpix560000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_136.dat", 560000)
    assert len(records) == 11
    assert records[0] == "ra=7.3924235288949784, dec=-52.66932290637368, pmra=7, pmdec=1, mag=14.847, flux0=1.544921875"


def test_healpix565000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_137.dat", 565000)
    assert len(records) == 8
    assert records[0] == "ra=33.40424684500519, dec=-35.20812997371337, pmra=-153, pmdec=-374, mag=13.288, flux0=0.537109375"


def test_healpix570000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_139.dat", 570000)
    assert len(records) == 11
    assert records[0] == "ra=18.942584851264293, dec=-36.32263995535794, pmra=9, pmdec=-9, mag=14.928, flux0=1.6494140625"


def test_healpix575000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_140.dat", 575000)
    assert len(records) == 13
    assert records[0] == "ra=48.190566938459206, dec=-31.683984935136504, pmra=1, pmdec=-3, mag=10.001, flux0=2.513671875"


def test_healpix580000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_141.dat", 580000)
    assert len(records) == 12
    assert records[0] == "ra=51.3181879424109, dec=-20.045152967816755, pmra=38, pmdec=-23, mag=12.674, flux0=1.3876953125"


def test_healpix585000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_142.dat", 585000)
    assert len(records) == 9
    assert records[0] == "ra=35.566424986145655, dec=-15.925454933161593, pmra=21, pmdec=3, mag=13.863, flux0=2.875"


def test_healpix590000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_144.dat", 590000)
    assert len(records) == 27
    assert records[0] == "ra=112.73823486302896, dec=-87.03262996256008, pmra=0, pmdec=15, mag=14.747, flux0=2.33984375"


def test_healpix595000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_145.dat", 595000)
    assert len(records) == 127
    assert records[0] == "ra=174.81974989865893, dec=-70.51060497318888, pmra=-3, pmdec=-1, mag=14.551, flux0=2.52734375"


def test_healpix600000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_146.dat", 600000)
    assert len(records) == 53
    assert records[0] == "ra=124.97449988730926, dec=-63.42370198267684, pmra=7, pmdec=2, mag=13.172, flux0=1.158203125"


def test_healpix605000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_147.dat", 605000)
    assert len(records) == 83
    assert records[0] == "ra=129.19983990918837, dec=-52.38947999309258, pmra=-1, pmdec=0, mag=13.205, flux0=0.0855712890625"


def test_healpix610000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_148.dat", 610000)
    assert len(records) == 89
    assert records[0] == "ra=157.88752999058343, dec=-47.34463995664596, pmra=-19, pmdec=12, mag=14.451, flux0=2.611328125"


def test_healpix615000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_150.dat", 615000)
    assert len(records) == 127
    assert records[0] == "ra=146.7698198867821, dec=-48.50053793215218, pmra=-4, pmdec=4, mag=13.888, flux0=0.472900390625"


def test_healpix620000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_151.dat", 620000)
    assert len(records) == 32
    assert records[0] == "ra=165.23810988536016, dec=-29.30297290408191, pmra=-7, pmdec=4, mag=12.788, flux0=2.068359375"


def test_healpix625000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_152.dat", 625000)
    assert len(records) == 32
    assert records[0] == "ra=101.36781993385767, dec=-54.67086790812707, pmra=3, pmdec=-3, mag=13.15, flux0=1.3115234375"


def test_healpix630000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_153.dat", 630000)
    assert len(records) == 105
    assert records[0] == "ra=123.74647496442611, dec=-37.14713994280768, pmra=-2, pmdec=3, mag=12.862, flux0=1.2041015625"


def test_healpix635000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_155.dat", 635000)
    assert len(records) == 77
    assert records[0] == "ra=113.57900999187444, dec=-38.26697494753961, pmra=-1, pmdec=2, mag=14.125, flux0=1.009765625"


def test_healpix640000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_156.dat", 640000)
    assert len(records) == 87
    assert records[0] == "ra=140.64510992758213, dec=-35.661635862505825, pmra=1, pmdec=-1, mag=13.926, flux0=2.029296875"


def test_healpix645000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_157.dat", 645000)
    assert len(records) == 10
    assert records[0] == "ra=150.12880991684682, dec=-17.846751929189427, pmra=-5, pmdec=-6, mag=13.572, flux0=0.95068359375"


def test_healpix650000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_158.dat", 650000)
    assert len(records) == 127
    assert records[0] == "ra=118.82010994424117, dec=-18.819266938985916, pmra=1, pmdec=1, mag=14.677, flux0=1.701171875"


def test_healpix655000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_159.dat", 655000)
    assert len(records) == 48
    assert records[0] == "ra=131.18322987630182, dec=-5.006680919326227, pmra=3, pmdec=2, mag=13.62, flux0=0.81103515625"


def test_healpix660000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_161.dat", 660000)
    assert len(records) == 81
    assert records[0] == "ra=248.57129987728376, dec=-74.58321984605082, pmra=-1, pmdec=-5, mag=14.833, flux0=2.63671875"


def test_healpix665000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_162.dat", 665000)
    assert len(records) == 127
    assert records[0] == "ra=214.2876299537195, dec=-66.8040499402229, pmra=-3, pmdec=-2, mag=14.215, flux0=4.23046875"


def test_healpix670000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_163.dat", 670000)
    assert len(records) == 127
    assert records[0] == "ra=221.06669983876247, dec=-55.855739943615966, pmra=-5, pmdec=-3, mag=13.917, flux0=0.035369873046875"


def test_healpix675000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_164.dat", 675000)
    assert len(records) == 104
    assert records[0] == "ra=250.2842299688068, dec=-50.85587686433264, pmra=-1, pmdec=-4, mag=14.841, flux0=0.50732421875"


def test_healpix680000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_166.dat", 680000)
    assert len(records) == 127
    assert records[0] == "ra=241.18815997670782, dec=-52.76785687206679, pmra=-4, pmdec=-3, mag=14.564, flux0=0.6240234375"


def test_healpix685000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_167.dat", 685000)
    assert len(records) == 84
    assert records[0] == "ra=247.18052991068944, dec=-32.39019799623182, pmra=-9, pmdec=-5, mag=11.515, flux0=4.515625"


def test_healpix690000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_168.dat", 690000)
    assert len(records) == 93
    assert records[0] == "ra=206.46317983347137, dec=-51.984089916564564, pmra=-4, pmdec=-2, mag=13.358, flux0=0.2117919921875"


def test_healpix695000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_169.dat", 695000)
    assert len(records) == 32
    assert records[0] == "ra=205.6532299917439, dec=-40.55560298289899, pmra=-5, pmdec=-1, mag=11.64, flux0=1.1171875"


def test_healpix700000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_170.dat", 700000)
    assert len(records) == 36
    assert records[0] == "ra=189.13139996544055, dec=-36.39301994647506, pmra=-24, pmdec=-5, mag=10.498, flux0=1.6142578125"


def test_healpix705000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_172.dat", 705000)
    assert len(records) == 42
    assert records[0] == "ra=226.738679905766, dec=-34.559599996805005, pmra=-4, pmdec=0, mag=14.895, flux0=1.4609375"


def test_healpix710000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_173.dat", 710000)
    assert len(records) == 23
    assert records[0] == "ra=246.11559989215598, dec=-19.412229926983, pmra=-12, pmdec=-15, mag=13.843, flux0=2.255859375"


def test_healpix715000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_174.dat", 715000)
    assert len(records) == 23
    assert records[0] == "ra=207.83848988257276, dec=-20.348552884696307, pmra=-3, pmdec=0, mag=12.203, flux0=1.048828125"


def test_healpix720000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_175.dat", 720000)
    assert len(records) == 5
    assert records[0] == "ra=223.71169991032764, dec=-8.279796842615957, pmra=11, pmdec=0, mag=14.146, flux0=0.84375"


def test_healpix725000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_177.dat", 725000)
    assert len(records) == 25
    assert records[0] == "ra=357.12414990976646, dec=-77.87664998223849, pmra=3, pmdec=0, mag=13.38, flux0=0.94677734375"


def test_healpix730000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_178.dat", 730000)
    assert len(records) == 36
    assert records[0] == "ra=286.6902199046175, dec=-70.13443991082461, pmra=7, pmdec=-36, mag=12.746, flux0=1.4755859375"


def test_healpix735000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_179.dat", 735000)
    assert len(records) == 25
    assert records[0] == "ra=322.71926995493436, dec=-53.13112989679497, pmra=10, pmdec=-7, mag=14.378, flux0=2.529296875"


def test_healpix740000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_180.dat", 740000)
    assert len(records) == 20
    assert records[0] == "ra=331.23766990901794, dec=-55.01860390185313, pmra=11, pmdec=0, mag=14.512, flux0=3.71875"


def test_healpix745000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_181.dat", 745000)
    assert len(records) == 8
    assert records[0] == "ra=344.94552991583265, dec=-37.425551897578664, pmra=13, pmdec=-12, mag=14.817, flux0=0.982421875"


def test_healpix750000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_183.dat", 750000)
    assert len(records) == 11
    assert records[0] == "ra=338.86049988626525, dec=-35.59799190405663, pmra=16, pmdec=-5, mag=14.136, flux0=3.251953125"


def test_healpix755000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_184.dat", 755000)
    assert len(records) == 37
    assert records[0] == "ra=295.14974989702444, dec=-55.45010394204878, pmra=5, pmdec=-16, mag=13.632, flux0=0.73876953125"


def test_healpix760000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_185.dat", 760000)
    assert len(records) == 19
    assert records[0] == "ra=296.98797988565076, dec=-44.94074197716114, pmra=3, pmdec=-3, mag=13.813, flux0=0.67724609375"


def test_healpix765000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_186.dat", 765000)
    assert len(records) == 96
    assert records[0] == "ra=282.3255599114697, dec=-39.813389982941274, pmra=3, pmdec=-8, mag=14.396, flux0=3.265625"


def test_healpix770000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_187.dat", 770000)
    assert len(records) == 45
    assert records[0] == "ra=293.18649996686094, dec=-21.361889942252024, pmra=4, pmdec=-19, mag=12.147, flux0=2.341796875"


def test_healpix775000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_189.dat", 775000)
    assert len(records) == 18
    assert records[0] == "ra=327.9940198957892, dec=-22.29266692991027, pmra=6, pmdec=-8, mag=13.632, flux0=0.476806640625"


def test_healpix780000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_190.dat", 780000)
    assert len(records) == 36
    assert records[0] == "ra=305.8498499476583, dec=-18.821450927677308, pmra=-1, pmdec=-5, mag=13.509, flux0=0.263427734375"


def test_healpix785000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, catalogue_level, chunked, chunk_healpix, first_healpix, last_healpix, records = read_record("siril_cat2_healpix8_xpsamp_191.dat", 785000)
    assert len(records) == 31
    assert records[0] == "ra=306.92232988165796, dec=-9.871610947824832, pmra=-3, pmdec=-4, mag=14.71, flux0=1.1103515625"
