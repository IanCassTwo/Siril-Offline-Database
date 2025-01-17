import struct
import sys
import logging
import pytest

# Constants
HEADER_FORMAT = '48s B B B 77s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
INDEX_SIZE = 786432  # Number of entries in the index
#INDEX_SIZE = 2  # Number of entries in the index
INDEX_ENTRY_SIZE = 4  # Each entry is a uint16 (2 bytes)
INDEX_TOTAL_BYTES = HEADER_SIZE + (INDEX_SIZE * INDEX_ENTRY_SIZE) # Total size of the index
RECORD_FORMAT = "iihhHh"  # Format for the record (int, int, short, short, short)
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)  # Size of each record in bytes
FILENAME = "siril_cat_healpix8_astro.dat" 


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
            records = []
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

                records.append(f"ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, teff={teff}, mag={mag}")

                #print(f"Record for healpixid {healpixid}: ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, teff={teff}, mag={mag}")
            #print(f"Numrecords = {numrecords}")
            return catalogue_title, gaia_version, healpix_level, catalogue_type, records

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        print(f"File not found: {file_path}")
    except Exception as e:
        logger.exception("An error occurred while reading the record.")
        print(f"An error occurred: {e}")


def test_header():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 0)
    assert catalogue_title == "Siril Gaia DR3 astrometric extract"
    assert gaia_version == 3
    assert healpix_level == 8
    assert catalogue_type == 1

def test_healpix0():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 0)
    assert len(records) == 96
    assert records[0] == "ra=44.996155, dec=0.00561, pmra=11, pmdec=-4, teff=5052, mag=17.641"


def test_healpix5000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 5000)
    assert len(records) == 127
    assert records[0] == "ra=54.479145, dec=16.05021, pmra=8, pmdec=-11, teff=4796, mag=14.242"


def test_healpix10000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 10000)
    assert len(records) == 127
    assert records[0] == "ra=40.082684, dec=20.1104, pmra=1, pmdec=-8, teff=5585, mag=15.228"


def test_healpix15000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 15000)
    assert len(records) == 127
    assert records[0] == "ra=35.529808, dec=29.71457, pmra=1, pmdec=-16, teff=5283, mag=14.268"


def test_healpix20000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 20000)
    assert len(records) == 127
    assert records[0] == "ra=63.98943, dec=33.53016, pmra=0, pmdec=-3, teff=5946, mag=16.126"


def test_healpix25000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 25000)
    assert len(records) == 127
    assert records[0] == "ra=56.59782, dec=35.33479, pmra=2, pmdec=-8, teff=4956, mag=15.925"


def test_healpix30000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 30000)
    assert len(records) == 127
    assert records[0] == "ra=84.65173, dec=52.83318, pmra=6, pmdec=-6, teff=0, mag=16.008"


def test_healpix35000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 35000)
    assert len(records) == 127
    assert records[0] == "ra=15.133527, dec=27.641, pmra=11, pmdec=-6, teff=0, mag=19.094"


def test_healpix40000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 40000)
    assert len(records) == 127
    assert records[0] == "ra=34.839703, dec=43.4287, pmra=-2, pmdec=-1, teff=0, mag=14.734"


def test_healpix45000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 45000)
    assert len(records) == 127
    assert records[0] == "ra=2.614083, dec=51.65704, pmra=0, pmdec=-2, teff=5761, mag=15.223"


def test_healpix50000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 50000)
    assert len(records) == 127
    assert records[0] == "ra=47.536823, dec=50.51205, pmra=3, pmdec=-2, teff=5784, mag=15.842"


def test_healpix55000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 55000)
    assert len(records) == 127
    assert records[0] == "ra=75.23924, dec=67.57481, pmra=-1, pmdec=0, teff=0, mag=13.462"


def test_healpix60000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 60000)
    assert len(records) == 127
    assert records[0] == "ra=8.151631, dec=65.72077, pmra=-1, pmdec=0, teff=9325, mag=16.797"


def test_healpix65000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 65000)
    assert len(records) == 127
    assert records[0] == "ra=62.40506, dec=85.26289, pmra=3, pmdec=-15, teff=0, mag=10.158"


def test_healpix70000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 70000)
    assert len(records) == 111
    assert records[0] == "ra=150.46025, dec=14.49332, pmra=-1, pmdec=-1, teff=5395, mag=16.125"


def test_healpix75000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 75000)
    assert len(records) == 127
    assert records[0] == "ra=129.02478, dec=18.52794, pmra=-4, pmdec=-6, teff=3659, mag=18.043"


def test_healpix80000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 80000)
    assert len(records) == 127
    assert records[0] == "ra=127.98415, dec=25.96557, pmra=-8, pmdec=-10, teff=4639, mag=16.168"


def test_healpix85000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 85000)
    assert len(records) == 86
    assert records[0] == "ra=157.15326, dec=30.35076, pmra=-16, pmdec=0, teff=0, mag=19.792"


def test_healpix90000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 90000)
    assert len(records) == 78
    assert records[0] == "ra=174.92998, dec=50.4929, pmra=-2, pmdec=-2, teff=0, mag=19.727"


def test_healpix95000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 95000)
    assert len(records) == 59
    assert records[0] == "ra=161.88734, dec=49.34964, pmra=-2, pmdec=-1, teff=0, mag=19.839"


def test_healpix100000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 100000)
    assert len(records) == 127
    assert records[0] == "ra=113.2151, dec=29.33371, pmra=0, pmdec=0, teff=4943, mag=17.337"


def test_healpix105000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 105000)
    assert len(records) == 127
    assert records[0] == "ra=114.25762, dec=39.84872, pmra=8, pmdec=3, teff=3800, mag=16.667"


def test_healpix110000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 110000)
    assert len(records) == 127
    assert records[0] == "ra=98.03559, dec=48.19258, pmra=0, pmdec=-14, teff=5744, mag=13.82"


def test_healpix115000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 115000)
    assert len(records) == 110
    assert records[0] == "ra=137.74251, dec=46.98146, pmra=-1, pmdec=-17, teff=0, mag=19.675"


def test_healpix120000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 120000)
    assert len(records) == 94
    assert records[0] == "ra=165.0948, dec=63.50192, pmra=1, pmdec=-6, teff=5683, mag=14.427"


def test_healpix125000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 125000)
    assert len(records) == 127
    assert records[0] == "ra=107.97917, dec=62.33455, pmra=-3, pmdec=-27, teff=0, mag=13.285"


def test_healpix130000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 130000)
    assert len(records) == 127
    assert records[0] == "ra=106.345375, dec=81.96467, pmra=2, pmdec=-2, teff=5076, mag=17.595"


def test_healpix135000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 135000)
    assert len(records) == 127
    assert records[0] == "ra=226.77205, dec=16.66099, pmra=-6, pmdec=-2, teff=0, mag=19.438"


def test_healpix140000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 140000)
    assert len(records) == 103
    assert records[0] == "ra=210.23778, dec=15.10631, pmra=6, pmdec=-7, teff=4406, mag=18.191"


def test_healpix145000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 145000)
    assert len(records) == 127
    assert records[0] == "ra=228.17847, dec=29.68155, pmra=-16, pmdec=0, teff=5223, mag=16.705"


def test_healpix150000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 150000)
    assert len(records) == 127
    assert records[0] == "ra=244.69829, dec=31.41545, pmra=-1, pmdec=-10, teff=5543, mag=14.206"


def test_healpix155000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 155000)
    assert len(records) == 127
    assert records[0] == "ra=268.422849, dec=48.57571, pmra=-2, pmdec=-16, teff=0, mag=15.537"


def test_healpix160000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 160000)
    assert len(records) == 127
    assert records[0] == "ra=252.02252, dec=45.0299, pmra=-9, pmdec=6, teff=3707, mag=18.439"


def test_healpix165000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 165000)
    assert len(records) == 93
    assert records[0] == "ra=206.37068, dec=26.28399, pmra=-3, pmdec=-1, teff=0, mag=19.037"


def test_healpix170000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 170000)
    assert len(records) == 97
    assert records[0] == "ra=208.83318, dec=36.44446, pmra=16, pmdec=-10, teff=3526, mag=17.882"


def test_healpix175000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 175000)
    assert len(records) == 69
    assert records[0] == "ra=182.20993, dec=44.65139, pmra=27, pmdec=-10, teff=3334, mag=18.325"


def test_healpix180000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 180000)
    assert len(records) == 101
    assert records[0] == "ra=186.91502, dec=61.20865, pmra=0, pmdec=0, teff=6738, mag=18.982"


def test_healpix185000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 185000)
    assert len(records) == 127
    assert records[0] == "ra=234.47461, dec=60.08292, pmra=-6, pmdec=-15, teff=3682, mag=18.055"


def test_healpix190000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 190000)
    assert len(records) == 97
    assert records[0] == "ra=209.12012, dec=64.97391, pmra=-4, pmdec=0, teff=0, mag=19.999"


def test_healpix195000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 195000)
    assert len(records) == 127
    assert records[0] == "ra=206.10347, dec=78.68343, pmra=-6, pmdec=-16, teff=5168, mag=14.201"


def test_healpix200000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 200000)
    assert len(records) == 127
    assert records[0] == "ra=319.21674, dec=13.25821, pmra=0, pmdec=-12, teff=0, mag=14.426"


def test_healpix205000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 205000)
    assert len(records) == 127
    assert records[0] == "ra=303.43396, dec=12.36328, pmra=-7, pmdec=-9, teff=5532, mag=14.23"


def test_healpix210000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 210000)
    assert len(records) == 127
    assert records[0] == "ra=322.75085, dec=26.62713, pmra=-9, pmdec=-7, teff=5672, mag=14.765"


def test_healpix215000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 215000)
    assert len(records) == 127
    assert records[0] == "ra=343.46286, dec=33.88488, pmra=-11, pmdec=-3, teff=0, mag=16.344"


def test_healpix220000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 220000)
    assert len(records) == 127
    assert records[0] == "ra=345.24652, dec=44.21124, pmra=0, pmdec=-2, teff=0, mag=15.973"


def test_healpix225000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 225000)
    assert len(records) == 127
    assert records[0] == "ra=323.6827, dec=49.3239, pmra=-1, pmdec=-8, teff=0, mag=13.27"


def test_healpix230000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 230000)
    assert len(records) == 127
    assert records[0] == "ra=291.0784, dec=24.6423, pmra=-2, pmdec=-5, teff=5722, mag=13.81"


def test_healpix235000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 235000)
    assert len(records) == 127
    assert records[0] == "ra=311.6934, dec=43.83944, pmra=-2, pmdec=-9, teff=0, mag=12.445"


def test_healpix240000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 240000)
    assert len(records) == 127
    assert records[0] == "ra=277.0342, dec=40.24994, pmra=-5, pmdec=-4, teff=4622, mag=16.229"


def test_healpix245000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 245000)
    assert len(records) == 127
    assert records[0] == "ra=285.5055, dec=57.79551, pmra=3, pmdec=-14, teff=5553, mag=15.666"


def test_healpix250000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 250000)
    assert len(records) == 127
    assert records[0] == "ra=330.01312, dec=56.66035, pmra=0, pmdec=-2, teff=7349, mag=12.948"


def test_healpix255000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 255000)
    assert len(records) == 127
    assert records[0] == "ra=306.23624, dec=61.57043, pmra=-1, pmdec=-2, teff=0, mag=15.259"


def test_healpix260000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 260000)
    assert len(records) == 127
    assert records[0] == "ra=332.28558, dec=80.51519, pmra=-4, pmdec=-3, teff=4872, mag=17.623"


def test_healpix265000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 265000)
    assert len(records) == 109
    assert records[0] == "ra=353.32376, dec=-28.9415, pmra=10, pmdec=-33, teff=4561, mag=18.794"


def test_healpix270000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 270000)
    assert len(records) == 93
    assert records[0] == "ra=7.034518, dec=-14.44601, pmra=2, pmdec=-10, teff=0, mag=15.792"


def test_healpix275000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 275000)
    assert len(records) == 106
    assert records[0] == "ra=356.83508, dec=-15.39027, pmra=16, pmdec=2, teff=3486, mag=18.866"


def test_healpix280000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 280000)
    assert len(records) == 99
    assert records[0] == "ra=30.936914, dec=-9.567, pmra=32, pmdec=-26, teff=5656, mag=12.422"


def test_healpix285000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 285000)
    assert len(records) == 107
    assert records[0] == "ra=31.999037, dec=-0.88453, pmra=2, pmdec=-18, teff=6046, mag=11.893"


def test_healpix290000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 290000)
    assert len(records) == 104
    assert records[0] == "ra=11.951634, dec=2.99239, pmra=2, pmdec=-5, teff=3507, mag=18.377"


def test_healpix295000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 295000)
    assert len(records) == 127
    assert records[0] == "ra=339.24915, dec=-17.24543, pmra=18, pmdec=-20, teff=4380, mag=14.763"


def test_healpix300000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 300000)
    assert len(records) == 127
    assert records[0] == "ra=348.0465, dec=-1.78571, pmra=-11, pmdec=-9, teff=3737, mag=17.081"


def test_healpix305000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 305000)
    assert len(records) == 127
    assert records[0] == "ra=332.21838, dec=2.09967, pmra=0, pmdec=-13, teff=3931, mag=17.885"


def test_healpix310000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 310000)
    assert len(records) == 127
    assert records[0] == "ra=329.06287, dec=10.83782, pmra=-1, pmdec=-10, teff=4387, mag=17.85"


def test_healpix315000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 315000)
    assert len(records) == 127
    assert records[0] == "ra=358.25418, dec=14.80426, pmra=6, pmdec=-5, teff=5887, mag=17.792"


def test_healpix320000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 320000)
    assert len(records) == 127
    assert records[0] == "ra=345.9573, dec=12.0472, pmra=-6, pmdec=-9, teff=0, mag=15.629"


def test_healpix325000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 325000)
    assert len(records) == 127
    assert records[0] == "ra=6.690668, dec=28.9945, pmra=-2, pmdec=-2, teff=5432, mag=17.621"


def test_healpix330000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 330000)
    assert len(records) == 127
    assert records[0] == "ra=87.90554, dec=-32.06533, pmra=7, pmdec=-7, teff=4402, mag=15.955"


def test_healpix335000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 335000)
    assert len(records) == 127
    assert records[0] == "ra=100.209946, dec=-17.24982, pmra=-1, pmdec=-4, teff=5938, mag=12.475"


def test_healpix340000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 340000)
    assert len(records) == 127
    assert records[0] == "ra=89.31237, dec=-18.82113, pmra=1, pmdec=2, teff=0, mag=15.054"


def test_healpix345000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 345000)
    assert len(records) == 127
    assert records[0] == "ra=110.03517, dec=-12.32463, pmra=-2, pmdec=6, teff=5770, mag=13.816"


def test_healpix350000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 350000)
    assert len(records) == 127
    assert records[0] == "ra=129.36893, dec=1.20003, pmra=0, pmdec=-4, teff=5078, mag=16.908"


def test_healpix355000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 355000)
    assert len(records) == 127
    assert records[0] == "ra=91.06183, dec=0.30726, pmra=1, pmdec=0, teff=7602, mag=15.751"


def test_healpix360000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 360000)
    assert len(records) == 127
    assert records[0] == "ra=111.08815, dec=13.2558, pmra=1, pmdec=-4, teff=5646, mag=14.9"


def test_healpix365000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 365000)
    assert len(records) == 127
    assert records[0] == "ra=81.20812, dec=-4.46126, pmra=0, pmdec=-1, teff=5225, mag=16.578"


def test_healpix370000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 370000)
    assert len(records) == 127
    assert records[0] == "ra=66.80096, dec=-0.57621, pmra=0, pmdec=-13, teff=0, mag=14.269"


def test_healpix375000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 375000)
    assert len(records) == 127
    assert records[0] == "ra=62.234108, dec=8.1004, pmra=0, pmdec=0, teff=4801, mag=18.679"


def test_healpix380000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 380000)
    assert len(records) == 127
    assert records[0] == "ra=90.72012, dec=11.43841, pmra=0, pmdec=0, teff=0, mag=15.127"


def test_healpix385000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 385000)
    assert len(records) == 127
    assert records[0] == "ra=100.203186, dec=28.30621, pmra=4, pmdec=-10, teff=0, mag=14.591"


def test_healpix390000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 390000)
    assert len(records) == 127
    assert records[0] == "ra=91.403336, dec=27.2856, pmra=1, pmdec=-5, teff=6784, mag=14.198"


def test_healpix395000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 395000)
    assert len(records) == 127
    assert records[0] == "ra=182.46162, dec=-28.28163, pmra=-7, pmdec=0, teff=5398, mag=17.298"


def test_healpix400000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 400000)
    assert len(records) == 127
    assert records[0] == "ra=181.40402, dec=-20.71789, pmra=-7, pmdec=0, teff=4330, mag=18.596"


def test_healpix405000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 405000)
    assert len(records) == 127
    assert records[0] == "ra=165.57632, dec=-16.62471, pmra=-45, pmdec=13, teff=3314, mag=18.751"


def test_healpix410000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 410000)
    assert len(records) == 127
    assert records[0] == "ra=204.60176, dec=-15.07625, pmra=-2, pmdec=0, teff=5193, mag=16.467"


def test_healpix415000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 415000)
    assert len(records) == 127
    assert records[0] == "ra=222.54512, dec=-1.48153, pmra=-2, pmdec=-5, teff=3637, mag=18.792"


def test_healpix420000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 420000)
    assert len(records) == 127
    assert records[0] == "ra=183.49918, dec=-2.96757, pmra=-1, pmdec=-31, teff=0, mag=19.6"


def test_healpix425000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 425000)
    assert len(records) == 90
    assert records[0] == "ra=201.44156, dec=10.51776, pmra=-5, pmdec=-3, teff=0, mag=19.647"


def test_healpix430000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 430000)
    assert len(records) == 121
    assert records[0] == "ra=156.07494, dec=-2.36767, pmra=-12, pmdec=2, teff=0, mag=19.691"


def test_healpix435000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 435000)
    assert len(records) == 127
    assert records[0] == "ra=145.92935, dec=-3.24286, pmra=-5, pmdec=-9, teff=3510, mag=17.363"


def test_healpix440000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 440000)
    assert len(records) == 112
    assert records[0] == "ra=160.30388, dec=9.60287, pmra=-1, pmdec=0, teff=0, mag=19.94"


def test_healpix445000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 445000)
    assert len(records) == 76
    assert records[0] == "ra=172.60962, dec=8.71453, pmra=-1, pmdec=-6, teff=0, mag=19.632"


def test_healpix450000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 450000)
    assert len(records) == 77
    assert records[0] == "ra=194.76141, dec=25.2963, pmra=0, pmdec=-11, teff=0, mag=15.355"


def test_healpix455000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 455000)
    assert len(records) == 77
    assert records[0] == "ra=184.61084, dec=24.33965, pmra=-12, pmdec=0, teff=0, mag=19.995"


def test_healpix460000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 460000)
    assert len(records) == 127
    assert records[0] == "ra=274.92413, dec=-32.08211, pmra=-4, pmdec=-10, teff=0, mag=12.336"


def test_healpix465000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 465000)
    assert len(records) == 127
    assert records[0] == "ra=275.95895, dec=-22.32671, pmra=2, pmdec=-7, teff=5847, mag=13.675"


def test_healpix470000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 470000)
    assert len(records) == 127
    assert records[0] == "ra=253.12976, dec=-15.70926, pmra=-2, pmdec=-5, teff=0, mag=15.223"


def test_healpix475000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 475000)
    assert len(records) == 127
    assert records[0] == "ra=271.0508, dec=-2.08271, pmra=-5, pmdec=0, teff=4631, mag=14.533"


def test_healpix480000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 480000)
    assert len(records) == 127
    assert records[0] == "ra=303.7472, dec=-4.76644, pmra=-5, pmdec=-2, teff=0, mag=16.069"


def test_healpix485000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 485000)
    assert len(records) == 127
    assert records[0] == "ra=282.34406, dec=-0.85616, pmra=0, pmdec=-3, teff=0, mag=13.414"


def test_healpix490000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 490000)
    assert len(records) == 127
    assert records[0] == "ra=284.77472, dec=7.80954, pmra=29, pmdec=34, teff=3844, mag=15.787"


def test_healpix495000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 495000)
    assert len(records) == 127
    assert records[0] == "ra=249.25703, dec=-5.07286, pmra=-9, pmdec=-9, teff=5145, mag=16.946"


def test_healpix500000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 500000)
    assert len(records) == 127
    assert records[0] == "ra=238.34886, dec=-6.56424, pmra=6, pmdec=1, teff=4388, mag=17.429"


def test_healpix505000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 505000)
    assert len(records) == 127
    assert records[0] == "ra=250.67908, dec=6.89748, pmra=-14, pmdec=-6, teff=0, mag=15.912"


def test_healpix510000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 510000)
    assert len(records) == 127
    assert records[0] == "ra=264.38016, dec=5.98733, pmra=-2, pmdec=-9, teff=5026, mag=15.712"


def test_healpix515000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 515000)
    assert len(records) == 127
    assert records[0] == "ra=273.84964, dec=22.36536, pmra=-3, pmdec=2, teff=0, mag=14.658"


def test_healpix520000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 520000)
    assert len(records) == 127
    assert records[0] == "ra=260.14215, dec=25.96531, pmra=1, pmdec=6, teff=5832, mag=13.322"


def test_healpix525000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 525000)
    assert len(records) == 127
    assert records[0] == "ra=21.36987, dec=-83.7654, pmra=13, pmdec=-3, teff=5512, mag=15.892"


def test_healpix530000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 530000)
    assert len(records) == 127
    assert records[0] == "ra=78.38391, dec=-67.17609, pmra=0, pmdec=0, teff=0, mag=15.728"


def test_healpix535000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 535000)
    assert len(records) == 127
    assert records[0] == "ra=18.8693, dec=-65.29624, pmra=16, pmdec=-5, teff=5533, mag=18.408"


def test_healpix540000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 540000)
    assert len(records) == 119
    assert records[0] == "ra=49.06548, dec=-48.87679, pmra=2, pmdec=-29, teff=3463, mag=17.809"


def test_healpix545000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 545000)
    assert len(records) == 127
    assert records[0] == "ra=84.08163, dec=-50.05107, pmra=2, pmdec=3, teff=0, mag=12.463"


def test_healpix550000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 550000)
    assert len(records) == 127
    assert records[0] == "ra=64.503426, dec=-44.97788, pmra=1, pmdec=0, teff=4477, mag=18.673"


def test_healpix555000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 555000)
    assert len(records) == 127
    assert records[0] == "ra=72.76913, dec=-25.58866, pmra=3, pmdec=-6, teff=0, mag=19.295"


def test_healpix560000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 560000)
    assert len(records) == 127
    assert records[0] == "ra=7.289485, dec=-52.75379, pmra=2, pmdec=-4, teff=4540, mag=17.758"


def test_healpix565000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 565000)
    assert len(records) == 78
    assert records[0] == "ra=33.403824, dec=-35.2708, pmra=5, pmdec=-1, teff=6232, mag=15.11"


def test_healpix570000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 570000)
    assert len(records) == 92
    assert records[0] == "ra=18.978601, dec=-36.38429, pmra=71, pmdec=-30, teff=0, mag=19.529"


def test_healpix575000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 575000)
    assert len(records) == 110
    assert records[0] == "ra=48.166428, dec=-31.73326, pmra=9, pmdec=-1, teff=0, mag=19.169"


def test_healpix580000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 580000)
    assert len(records) == 93
    assert records[0] == "ra=51.346123, dec=-20.08676, pmra=0, pmdec=-4, teff=5450, mag=17.109"


def test_healpix585000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 585000)
    assert len(records) == 79
    assert records[0] == "ra=35.503387, dec=-16.00532, pmra=5, pmdec=-1, teff=3703, mag=17.759"


def test_healpix590000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 590000)
    assert len(records) == 127
    assert records[0] == "ra=112.542885, dec=-87.07391, pmra=-3, pmdec=7, teff=4849, mag=16.265"


def test_healpix595000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 595000)
    assert len(records) == 127
    assert records[0] == "ra=174.81975, dec=-70.5106, pmra=-3, pmdec=-1, teff=0, mag=14.551"


def test_healpix600000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 600000)
    assert len(records) == 127
    assert records[0] == "ra=125.02549, dec=-63.41948, pmra=-3, pmdec=14, teff=5347, mag=15.682"


def test_healpix605000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 605000)
    assert len(records) == 127
    assert records[0] == "ra=129.19984, dec=-52.38948, pmra=-1, pmdec=0, teff=0, mag=13.205"


def test_healpix610000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 610000)
    assert len(records) == 127
    assert records[0] == "ra=157.88753, dec=-47.34464, pmra=-19, pmdec=12, teff=4716, mag=14.451"


def test_healpix615000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 615000)
    assert len(records) == 127
    assert records[0] == "ra=146.76982, dec=-48.50053, pmra=-4, pmdec=4, teff=0, mag=13.888"


def test_healpix620000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 620000)
    assert len(records) == 127
    assert records[0] == "ra=165.23811, dec=-29.30297, pmra=-7, pmdec=4, teff=6546, mag=12.788"


def test_healpix625000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 625000)
    assert len(records) == 127
    assert records[0] == "ra=101.40183, dec=-54.70255, pmra=10, pmdec=-11, teff=4177, mag=17.119"


def test_healpix630000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 630000)
    assert len(records) == 127
    assert records[0] == "ra=123.746475, dec=-37.14714, pmra=-2, pmdec=3, teff=0, mag=12.862"


def test_healpix635000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 635000)
    assert len(records) == 127
    assert records[0] == "ra=113.57769, dec=-38.25728, pmra=-5, pmdec=11, teff=5743, mag=15.74"


def test_healpix640000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 640000)
    assert len(records) == 127
    assert records[0] == "ra=140.64511, dec=-35.66163, pmra=1, pmdec=-1, teff=5274, mag=13.926"


def test_healpix645000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 645000)
    assert len(records) == 127
    assert records[0] == "ra=150.10248, dec=-17.87801, pmra=-2, pmdec=-1, teff=5404, mag=17.278"


def test_healpix650000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 650000)
    assert len(records) == 127
    assert records[0] == "ra=118.82011, dec=-18.81926, pmra=1, pmdec=1, teff=0, mag=14.677"


def test_healpix655000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 655000)
    assert len(records) == 127
    assert records[0] == "ra=131.122449, dec=-5.07006, pmra=-3, pmdec=3, teff=0, mag=15.124"


def test_healpix660000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 660000)
    assert len(records) == 127
    assert records[0] == "ra=248.5729, dec=-74.59667, pmra=4, pmdec=0, teff=5515, mag=15.53"


def test_healpix665000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 665000)
    assert len(records) == 127
    assert records[0] == "ra=214.28763, dec=-66.80405, pmra=-3, pmdec=-2, teff=0, mag=14.215"


def test_healpix670000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 670000)
    assert len(records) == 127
    assert records[0] == "ra=221.0667, dec=-55.85574, pmra=-5, pmdec=-3, teff=0, mag=13.917"


def test_healpix675000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 675000)
    assert len(records) == 127
    assert records[0] == "ra=250.28423, dec=-50.85587, pmra=-1, pmdec=-4, teff=0, mag=14.841"


def test_healpix680000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 680000)
    assert len(records) == 127
    assert records[0] == "ra=241.18816, dec=-52.76785, pmra=-4, pmdec=-3, teff=0, mag=14.564"


def test_healpix685000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 685000)
    assert len(records) == 127
    assert records[0] == "ra=247.14835, dec=-32.42964, pmra=5, pmdec=0, teff=6524, mag=15.48"


def test_healpix690000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 690000)
    assert len(records) == 127
    assert records[0] == "ra=206.52592, dec=-51.98866, pmra=3, pmdec=-8, teff=5724, mag=15.074"


def test_healpix695000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 695000)
    assert len(records) == 127
    assert records[0] == "ra=205.6632, dec=-40.60497, pmra=-14, pmdec=-1, teff=4612, mag=16.016"


def test_healpix700000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 700000)
    assert len(records) == 127
    assert records[0] == "ra=189.1314, dec=-36.39302, pmra=-24, pmdec=-5, teff=6461, mag=10.498"


def test_healpix705000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 705000)
    assert len(records) == 127
    assert records[0] == "ra=226.74649, dec=-34.56967, pmra=-5, pmdec=-7, teff=5043, mag=15.134"


def test_healpix710000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 710000)
    assert len(records) == 127
    assert records[0] == "ra=246.08641, dec=-19.45133, pmra=-12, pmdec=-12, teff=4262, mag=16.756"


def test_healpix715000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 715000)
    assert len(records) == 127
    assert records[0] == "ra=207.76917, dec=-20.40193, pmra=1, pmdec=-8, teff=5337, mag=17.219"


def test_healpix720000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 720000)
    assert len(records) == 127
    assert records[0] == "ra=223.58948, dec=-8.37834, pmra=-9, pmdec=-11, teff=5223, mag=18.444"


def test_healpix725000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 725000)
    assert len(records) == 127
    assert records[0] == "ra=357.23602, dec=-77.89959, pmra=-3, pmdec=-14, teff=5507, mag=17.967"


def test_healpix730000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 730000)
    assert len(records) == 127
    assert records[0] == "ra=286.74066, dec=-70.1353, pmra=-1, pmdec=0, teff=0, mag=16.377"


def test_healpix735000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 735000)
    assert len(records) == 127
    assert records[0] == "ra=322.7262, dec=-53.18256, pmra=6, pmdec=-11, teff=5649, mag=17.942"


def test_healpix740000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 740000)
    assert len(records) == 127
    assert records[0] == "ra=331.27246, dec=-55.10059, pmra=-8, pmdec=-8, teff=4953, mag=18.433"


def test_healpix745000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 745000)
    assert len(records) == 122
    assert records[0] == "ra=344.8642, dec=-37.52375, pmra=5, pmdec=-6, teff=0, mag=19.107"


def test_healpix750000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 750000)
    assert len(records) == 127
    assert records[0] == "ra=338.9331, dec=-35.64871, pmra=6, pmdec=-7, teff=0, mag=19.705"


def test_healpix755000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 755000)
    assert len(records) == 127
    assert records[0] == "ra=295.17056, dec=-55.47588, pmra=-15, pmdec=-94, teff=0, mag=16.253"


def test_healpix760000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 760000)
    assert len(records) == 127
    assert records[0] == "ra=297.0207, dec=-44.95818, pmra=0, pmdec=-1, teff=5096, mag=16.204"


def test_healpix765000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 765000)
    assert len(records) == 127
    assert records[0] == "ra=282.32556, dec=-39.81339, pmra=3, pmdec=-8, teff=5662, mag=14.396"


def test_healpix770000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 770000)
    assert len(records) == 127
    assert records[0] == "ra=293.1865, dec=-21.36189, pmra=4, pmdec=-19, teff=5252, mag=12.147"


def test_healpix775000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 775000)
    assert len(records) == 127
    assert records[0] == "ra=328.0048, dec=-22.33586, pmra=-7, pmdec=-15, teff=0, mag=16.525"


def test_healpix780000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 780000)
    assert len(records) == 127
    assert records[0] == "ra=305.87418, dec=-18.81804, pmra=-2, pmdec=-1, teff=0, mag=16.776"


def test_healpix785000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 785000)
    assert len(records) == 127
    assert records[0] == "ra=306.92392, dec=-9.87728, pmra=-7, pmdec=-22, teff=5514, mag=15.274"
