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
INT32_MAX = 2**31 -1
RADEC_SCALE = INT32_MAX / 360.0

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
                ra /= RADEC_SCALE
                dec /= RADEC_SCALE
                mag /= 1000.0
 
                records.append(f"ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, teff={teff}, mag={mag}")

                print(f"Record for healpixid {healpixid}: ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, teff={teff}, mag={mag}")
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
    print(records[0])
    assert len(records) == 96
    assert records[0] == "ra=44.996154906692055, dec=0.005615204575292396, pmra=11, pmdec=-4, teff=5052, mag=17.641"


def test_healpix5000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 5000)
    assert len(records) == 127
    assert records[0] == "ra=54.479144948757785, dec=16.05020983892037, pmra=8, pmdec=-11, teff=4796, mag=14.242"


def test_healpix10000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 10000)
    assert len(records) == 127
    assert records[0] == "ra=40.08268393579995, dec=20.11040692222789, pmra=1, pmdec=-8, teff=5585, mag=15.228"


def test_healpix15000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 15000)
    assert len(records) == 127
    assert records[0] == "ra=35.52980797157148, dec=29.71457497669131, pmra=1, pmdec=-16, teff=5283, mag=14.268"


def test_healpix20000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 20000)
    assert len(records) == 127
    assert records[0] == "ra=63.98942998796209, dec=33.53016683530536, pmra=0, pmdec=-3, teff=5946, mag=16.126"


def test_healpix25000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 25000)
    assert len(records) == 127
    assert records[0] == "ra=56.59781983895125, dec=35.334796959224526, pmra=2, pmdec=-8, teff=4956, mag=15.925"


def test_healpix30000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 30000)
    assert len(records) == 127
    assert records[0] == "ra=84.6517299137319, dec=52.83317989336009, pmra=6, pmdec=-6, teff=0, mag=16.008"


def test_healpix35000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 35000)
    assert len(records) == 127
    assert records[0] == "ra=15.133526853813569, dec=27.64099992236169, pmra=11, pmdec=-6, teff=0, mag=19.094"


def test_healpix40000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 40000)
    assert len(records) == 127
    assert records[0] == "ra=34.8397029167226, dec=43.428706994014185, pmra=-2, pmdec=-1, teff=0, mag=14.734"


def test_healpix45000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 45000)
    assert len(records) == 127
    assert records[0] == "ra=2.6140837569786624, dec=51.65704699775998, pmra=0, pmdec=-2, teff=5761, mag=15.223"


def test_healpix50000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 50000)
    assert len(records) == 127
    assert records[0] == "ra=47.536822914861524, dec=50.51204985497149, pmra=3, pmdec=-2, teff=5784, mag=15.842"


def test_healpix55000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 55000)
    assert len(records) == 127
    assert records[0] == "ra=75.23923989163676, dec=67.57481388169099, pmra=-1, pmdec=0, teff=0, mag=13.462"


def test_healpix60000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 60000)
    assert len(records) == 127
    assert records[0] == "ra=8.151631843369282, dec=65.72076992398163, pmra=-1, pmdec=0, teff=9325, mag=16.797"


def test_healpix65000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 65000)
    assert len(records) == 127
    assert records[0] == "ra=62.40505985096332, dec=85.26288984588481, pmra=3, pmdec=-15, teff=0, mag=10.158"


def test_healpix70000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 70000)
    assert len(records) == 111
    assert records[0] == "ra=150.46024998205723, dec=14.493320944948737, pmra=-1, pmdec=-1, teff=5395, mag=16.125"


def test_healpix75000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 75000)
    assert len(records) == 127
    assert records[0] == "ra=129.02477983805574, dec=18.527947859153127, pmra=-4, pmdec=-6, teff=3659, mag=18.043"


def test_healpix80000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 80000)
    assert len(records) == 127
    assert records[0] == "ra=127.98414996265626, dec=25.965571974388123, pmra=-8, pmdec=-10, teff=4639, mag=16.168"


def test_healpix85000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 85000)
    assert len(records) == 86
    assert records[0] == "ra=157.15325984971284, dec=30.350768971420248, pmra=-16, pmdec=0, teff=0, mag=19.792"


def test_healpix90000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 90000)
    assert len(records) == 78
    assert records[0] == "ra=174.92997997204304, dec=50.49290491756653, pmra=-2, pmdec=-2, teff=0, mag=19.727"


def test_healpix95000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 95000)
    assert len(records) == 59
    assert records[0] == "ra=161.8873399877396, dec=49.34963997888828, pmra=-2, pmdec=-1, teff=0, mag=19.839"


def test_healpix100000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 100000)
    assert len(records) == 127
    assert records[0] == "ra=113.21509994250493, dec=29.33371688673911, pmra=0, pmdec=0, teff=4943, mag=17.337"


def test_healpix105000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 105000)
    assert len(records) == 127
    assert records[0] == "ra=114.25761993707046, dec=39.848723895777354, pmra=8, pmdec=3, teff=3800, mag=16.667"


def test_healpix110000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 110000)
    assert len(records) == 127
    assert records[0] == "ra=98.03558996787089, dec=48.192579936325814, pmra=0, pmdec=-14, teff=5744, mag=13.82"


def test_healpix115000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 115000)
    assert len(records) == 110
    assert records[0] == "ra=137.74250999919255, dec=46.98145997104303, pmra=-1, pmdec=-17, teff=0, mag=19.675"


def test_healpix120000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 120000)
    assert len(records) == 94
    assert records[0] == "ra=165.0947999605419, dec=63.501919891453305, pmra=1, pmdec=-6, teff=5683, mag=14.427"


def test_healpix125000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 125000)
    assert len(records) == 127
    assert records[0] == "ra=107.97916994801683, dec=62.33455295783214, pmra=-3, pmdec=-27, teff=0, mag=13.285"


def test_healpix130000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 130000)
    assert len(records) == 127
    assert records[0] == "ra=106.34537491311569, dec=81.96467598991686, pmra=2, pmdec=-2, teff=5076, mag=17.595"


def test_healpix135000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 135000)
    assert len(records) == 127
    assert records[0] == "ra=226.77204993868807, dec=16.660989903221367, pmra=-6, pmdec=-2, teff=0, mag=19.438"


def test_healpix140000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 140000)
    assert len(records) == 103
    assert records[0] == "ra=210.23777999460592, dec=15.106316849173194, pmra=6, pmdec=-7, teff=4406, mag=18.191"


def test_healpix145000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 145000)
    assert len(records) == 127
    assert records[0] == "ra=228.1784699243393, dec=29.681549942903942, pmra=-16, pmdec=0, teff=5223, mag=16.705"


def test_healpix150000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 150000)
    assert len(records) == 127
    assert records[0] == "ra=244.69828997026116, dec=31.415454908933235, pmra=-1, pmdec=-10, teff=5543, mag=14.206"


def test_healpix155000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 155000)
    assert len(records) == 127
    assert records[0] == "ra=268.42284999248704, dec=48.575709857314685, pmra=-2, pmdec=-16, teff=0, mag=15.537"


def test_healpix160000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 160000)
    assert len(records) == 127
    assert records[0] == "ra=252.0225198436633, dec=45.029902963447334, pmra=-9, pmdec=6, teff=3707, mag=18.439"


def test_healpix165000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 165000)
    assert len(records) == 93
    assert records[0] == "ra=206.37067999987428, dec=26.283992932310323, pmra=-3, pmdec=-1, teff=0, mag=19.037"


def test_healpix170000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 170000)
    assert len(records) == 97
    assert records[0] == "ra=208.83317988777213, dec=36.44446588887109, pmra=16, pmdec=-10, teff=3526, mag=17.882"


def test_healpix175000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 175000)
    assert len(records) == 69
    assert records[0] == "ra=182.20992992734998, dec=44.65138996236556, pmra=27, pmdec=-10, teff=3334, mag=18.325"


def test_healpix180000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 180000)
    assert len(records) == 101
    assert records[0] == "ra=186.91501991213997, dec=61.20864995811537, pmra=0, pmdec=0, teff=6738, mag=18.982"


def test_healpix185000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 185000)
    assert len(records) == 127
    assert records[0] == "ra=234.47460992004468, dec=60.08291996088015, pmra=-6, pmdec=-15, teff=3682, mag=18.055"


def test_healpix190000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 190000)
    assert len(records) == 97
    assert records[0] == "ra=209.12011992610996, dec=64.97390999690346, pmra=-4, pmdec=0, teff=0, mag=19.999"


def test_healpix195000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 195000)
    assert len(records) == 127
    assert records[0] == "ra=206.10346995578308, dec=78.68342995582307, pmra=-6, pmdec=-16, teff=5168, mag=14.201"


def test_healpix200000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 200000)
    assert len(records) == 127
    assert records[0] == "ra=319.2167398329902, dec=13.258213984434592, pmra=0, pmdec=-12, teff=0, mag=14.426"


def test_healpix205000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 205000)
    assert len(records) == 127
    assert records[0] == "ra=303.43395997930037, dec=12.363286899571905, pmra=-7, pmdec=-9, teff=5532, mag=14.23"


def test_healpix210000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 210000)
    assert len(records) == 127
    assert records[0] == "ra=322.7508499486143, dec=26.62712994340208, pmra=-9, pmdec=-7, teff=5672, mag=14.765"


def test_healpix215000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 215000)
    assert len(records) == 127
    assert records[0] == "ra=343.4628599246325, dec=33.88487992523465, pmra=-11, pmdec=-3, teff=0, mag=16.344"


def test_healpix220000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 220000)
    assert len(records) == 127
    assert records[0] == "ra=345.2465198679112, dec=44.21124298321607, pmra=0, pmdec=-2, teff=0, mag=15.973"


def test_healpix225000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 225000)
    assert len(records) == 127
    assert records[0] == "ra=323.682699987517, dec=49.32389999242681, pmra=-1, pmdec=-8, teff=0, mag=13.27"


def test_healpix230000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 230000)
    assert len(records) == 127
    assert records[0] == "ra=291.0783998533517, dec=24.642299965323087, pmra=-2, pmdec=-5, teff=5722, mag=13.81"


def test_healpix235000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 235000)
    assert len(records) == 127
    assert records[0] == "ra=311.6933998613122, dec=43.83944288074944, pmra=-2, pmdec=-9, teff=0, mag=12.445"


def test_healpix240000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 240000)
    assert len(records) == 127
    assert records[0] == "ra=277.0341998883682, dec=40.249939952162066, pmra=-5, pmdec=-4, teff=4622, mag=16.229"


def test_healpix245000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 245000)
    assert len(records) == 127
    assert records[0] == "ra=285.5054999168522, dec=57.79550998369022, pmra=3, pmdec=-14, teff=5553, mag=15.666"


def test_healpix250000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 250000)
    assert len(records) == 127
    assert records[0] == "ra=330.01311995555324, dec=56.660349842468435, pmra=0, pmdec=-2, teff=7349, mag=12.948"


def test_healpix255000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 255000)
    assert len(records) == 127
    assert records[0] == "ra=306.2362399260682, dec=61.57043485975378, pmra=-1, pmdec=-2, teff=0, mag=15.259"


def test_healpix260000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 260000)
    assert len(records) == 127
    assert records[0] == "ra=332.28557997023944, dec=80.51518995338826, pmra=-4, pmdec=-3, teff=4872, mag=17.623"


def test_healpix265000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 265000)
    assert len(records) == 109
    assert records[0] == "ra=353.3237599736656, dec=-28.941506850040287, pmra=10, pmdec=-33, teff=4561, mag=18.794"


def test_healpix270000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 270000)
    assert len(records) == 93
    assert records[0] == "ra=7.034518610236476, dec=-14.44601784201619, pmra=2, pmdec=-10, teff=0, mag=15.792"


def test_healpix275000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 275000)
    assert len(records) == 106
    assert records[0] == "ra=356.8350798621937, dec=-15.390274941637308, pmra=16, pmdec=2, teff=3486, mag=18.866"


def test_healpix280000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 280000)
    assert len(records) == 99
    assert records[0] == "ra=30.93691395173637, dec=-9.567002863421571, pmra=32, pmdec=-26, teff=5656, mag=12.422"


def test_healpix285000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 285000)
    assert len(records) == 107
    assert records[0] == "ra=31.999036870896365, dec=-0.8845328543728835, pmra=2, pmdec=-18, teff=6046, mag=11.893"


def test_healpix290000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 290000)
    assert len(records) == 104
    assert records[0] == "ra=11.951633864991196, dec=2.992393245451335, pmra=2, pmdec=-5, teff=3507, mag=18.377"


def test_healpix295000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 295000)
    assert len(records) == 127
    assert records[0] == "ra=339.2491499610474, dec=-17.24543386010706, pmra=18, pmdec=-20, teff=4380, mag=14.763"


def test_healpix300000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 300000)
    assert len(records) == 127
    assert records[0] == "ra=348.0464999880858, dec=-1.7857106597096242, pmra=-11, pmdec=-9, teff=3737, mag=17.081"


def test_healpix305000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 305000)
    assert len(records) == 127
    assert records[0] == "ra=332.21837990554906, dec=2.0996712716760446, pmra=0, pmdec=-13, teff=3931, mag=17.885"


def test_healpix310000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 310000)
    assert len(records) == 127
    assert records[0] == "ra=329.06286996279977, dec=10.837826957384975, pmra=-1, pmdec=-10, teff=4387, mag=17.85"


def test_healpix315000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 315000)
    assert len(records) == 127
    assert records[0] == "ra=358.25417999096874, dec=14.804259880820409, pmra=6, pmdec=-5, teff=5887, mag=17.792"


def test_healpix320000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 320000)
    assert len(records) == 127
    assert records[0] == "ra=345.9572998927707, dec=12.04719996640794, pmra=-6, pmdec=-9, teff=0, mag=15.629"


def test_healpix325000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 325000)
    assert len(records) == 127
    assert records[0] == "ra=6.690668466822555, dec=28.99450897657988, pmra=-2, pmdec=-2, teff=5432, mag=17.621"


def test_healpix330000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 330000)
    assert len(records) == 127
    assert records[0] == "ra=87.9055399670757, dec=-32.06532984602513, pmra=7, pmdec=-7, teff=4402, mag=15.955"


def test_healpix335000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 335000)
    assert len(records) == 127
    assert records[0] == "ra=100.20994584085882, dec=-17.249822959885847, pmra=-1, pmdec=-4, teff=5938, mag=12.475"


def test_healpix340000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 340000)
    assert len(records) == 127
    assert records[0] == "ra=89.31236999543027, dec=-18.82113492992759, pmra=1, pmdec=2, teff=0, mag=15.054"


def test_healpix345000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 345000)
    assert len(records) == 127
    assert records[0] == "ra=110.03516999540625, dec=-12.324636938201559, pmra=-2, pmdec=6, teff=5770, mag=13.816"


def test_healpix350000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 350000)
    assert len(records) == 127
    assert records[0] == "ra=129.36892988596526, dec=1.2000298691913625, pmra=0, pmdec=-4, teff=5078, mag=16.908"


def test_healpix355000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 355000)
    assert len(records) == 127
    assert records[0] == "ra=91.0618299297345, dec=0.307268500471147, pmra=1, pmdec=0, teff=7602, mag=15.751"


def test_healpix360000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 360000)
    assert len(records) == 127
    assert records[0] == "ra=111.08814993458247, dec=13.255806869480669, pmra=1, pmdec=-4, teff=5646, mag=14.9"


def test_healpix365000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 365000)
    assert len(records) == 127
    assert records[0] == "ra=81.20811987724532, dec=-4.461266456386664, pmra=0, pmdec=-1, teff=5225, mag=16.578"


def test_healpix370000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 370000)
    assert len(records) == 127
    assert records[0] == "ra=66.80095986779824, dec=-0.5762169512809333, pmra=0, pmdec=-13, teff=0, mag=14.269"


def test_healpix375000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 375000)
    assert len(records) == 127
    assert records[0] == "ra=62.23410791821503, dec=8.100405842112565, pmra=0, pmdec=0, teff=4801, mag=18.679"


def test_healpix380000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 380000)
    assert len(records) == 127
    assert records[0] == "ra=90.72011985383932, dec=11.438418986014284, pmra=0, pmdec=0, teff=0, mag=15.127"


def test_healpix385000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 385000)
    assert len(records) == 127
    assert records[0] == "ra=100.20318583594783, dec=28.306209886589183, pmra=4, pmdec=-10, teff=0, mag=14.591"


def test_healpix390000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 390000)
    assert len(records) == 127
    assert records[0] == "ra=91.40333599010637, dec=27.285602869133278, pmra=1, pmdec=-5, teff=6784, mag=14.198"


def test_healpix395000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 395000)
    assert len(records) == 127
    assert records[0] == "ra=182.46161987188347, dec=-28.281634984669104, pmra=-7, pmdec=0, teff=5398, mag=17.298"


def test_healpix400000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 400000)
    assert len(records) == 127
    assert records[0] == "ra=181.404019995315, dec=-20.717896921894464, pmra=-7, pmdec=0, teff=4330, mag=18.596"


def test_healpix405000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 405000)
    assert len(records) == 127
    assert records[0] == "ra=165.57631984612732, dec=-16.62471386446837, pmra=-45, pmdec=13, teff=3314, mag=18.751"


def test_healpix410000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 410000)
    assert len(records) == 127
    assert records[0] == "ra=204.6017599686057, dec=-15.076256997453168, pmra=-2, pmdec=0, teff=5193, mag=16.467"


def test_healpix415000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 415000)
    assert len(records) == 127
    assert records[0] == "ra=222.54511986977658, dec=-1.4815329022153898, pmra=-2, pmdec=-5, teff=3637, mag=18.792"


def test_healpix420000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 420000)
    assert len(records) == 127
    assert records[0] == "ra=183.49917995906395, dec=-2.967570406835326, pmra=-1, pmdec=-31, teff=0, mag=19.6"


def test_healpix425000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 425000)
    assert len(records) == 90
    assert records[0] == "ra=201.44155984811556, dec=10.517764990459087, pmra=-5, pmdec=-3, teff=0, mag=19.647"


def test_healpix430000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 430000)
    assert len(records) == 121
    assert records[0] == "ra=156.07493990849466, dec=-2.3676794405876094, pmra=-12, pmdec=2, teff=0, mag=19.691"


def test_healpix435000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 435000)
    assert len(records) == 127
    assert records[0] == "ra=145.9293498592122, dec=-3.242867143472129, pmra=-5, pmdec=-9, teff=3510, mag=17.363"


def test_healpix440000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 440000)
    assert len(records) == 112
    assert records[0] == "ra=160.30387995778761, dec=9.60287288278475, pmra=-1, pmdec=0, teff=0, mag=19.94"


def test_healpix445000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 445000)
    assert len(records) == 76
    assert records[0] == "ra=172.60961989528016, dec=8.71453296798958, pmra=-1, pmdec=-6, teff=0, mag=19.632"


def test_healpix450000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 450000)
    assert len(records) == 77
    assert records[0] == "ra=194.76140999922592, dec=25.29630298972889, pmra=0, pmdec=-11, teff=0, mag=15.355"


def test_healpix455000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 455000)
    assert len(records) == 77
    assert records[0] == "ra=184.61083992599083, dec=24.339652910986754, pmra=-12, pmdec=0, teff=0, mag=19.995"


def test_healpix460000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 460000)
    assert len(records) == 127
    assert records[0] == "ra=274.92412985997464, dec=-32.08210991326818, pmra=-4, pmdec=-10, teff=0, mag=12.336"


def test_healpix465000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 465000)
    assert len(records) == 127
    assert records[0] == "ra=275.95894990300707, dec=-22.32671489116117, pmra=2, pmdec=-7, teff=5847, mag=13.675"


def test_healpix470000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 470000)
    assert len(records) == 127
    assert records[0] == "ra=253.1297598654077, dec=-15.709266874803818, pmra=-2, pmdec=-5, teff=0, mag=15.223"


def test_healpix475000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 475000)
    assert len(records) == 127
    assert records[0] == "ra=271.05079993188883, dec=-2.0827119993431085, pmra=-5, pmdec=0, teff=4631, mag=14.533"


def test_healpix480000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 480000)
    assert len(records) == 127
    assert records[0] == "ra=303.74719991523176, dec=-4.766442163272966, pmra=-5, pmdec=-2, teff=0, mag=16.069"


def test_healpix485000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 485000)
    assert len(records) == 127
    assert records[0] == "ra=282.34405994524434, dec=-0.8561648059897892, pmra=0, pmdec=-3, teff=0, mag=13.414"


def test_healpix490000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 490000)
    assert len(records) == 127
    assert records[0] == "ra=284.7747198328258, dec=7.809540223241569, pmra=29, pmdec=34, teff=3844, mag=15.787"


def test_healpix495000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 495000)
    assert len(records) == 127
    assert records[0] == "ra=249.2570299139512, dec=-5.07286627081822, pmra=-9, pmdec=-9, teff=5145, mag=16.946"


def test_healpix500000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 500000)
    assert len(records) == 127
    assert records[0] == "ra=238.34885984582306, dec=-6.564244891779611, pmra=6, pmdec=1, teff=4388, mag=17.429"


def test_healpix505000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 505000)
    assert len(records) == 127
    assert records[0] == "ra=250.67907996972977, dec=6.897488910191454, pmra=-14, pmdec=-6, teff=0, mag=15.912"


def test_healpix510000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 510000)
    assert len(records) == 127
    assert records[0] == "ra=264.3801598550659, dec=5.987334254191878, pmra=-2, pmdec=-9, teff=5026, mag=15.712"


def test_healpix515000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 515000)
    assert len(records) == 127
    assert records[0] == "ra=273.8496399455935, dec=22.36536183504637, pmra=-3, pmdec=2, teff=0, mag=14.658"


def test_healpix520000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 520000)
    assert len(records) == 127
    assert records[0] == "ra=260.1421499159849, dec=25.965314985236763, pmra=1, pmdec=6, teff=5832, mag=13.322"


def test_healpix525000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 525000)
    assert len(records) == 127
    assert records[0] == "ra=21.369869849351172, dec=-83.7653999793182, pmra=13, pmdec=-3, teff=5512, mag=15.892"


def test_healpix530000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 530000)
    assert len(records) == 127
    assert records[0] == "ra=78.38390985428538, dec=-67.1760899327584, pmra=0, pmdec=0, teff=0, mag=15.728"


def test_healpix535000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 535000)
    assert len(records) == 127
    assert records[0] == "ra=18.86929986014464, dec=-65.29623993919056, pmra=16, pmdec=-5, teff=5533, mag=18.408"


def test_healpix540000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 540000)
    assert len(records) == 119
    assert records[0] == "ra=49.06547988255763, dec=-48.876796871832006, pmra=2, pmdec=-29, teff=3463, mag=17.809"


def test_healpix545000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 545000)
    assert len(records) == 127
    assert records[0] == "ra=84.08162990775034, dec=-50.05106999075555, pmra=2, pmdec=3, teff=0, mag=12.463"


def test_healpix550000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 550000)
    assert len(records) == 127
    assert records[0] == "ra=64.50342589267689, dec=-44.977879843198636, pmra=1, pmdec=0, teff=4477, mag=18.673"


def test_healpix555000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 555000)
    assert len(records) == 127
    assert records[0] == "ra=72.76912990620785, dec=-25.588666957611526, pmra=3, pmdec=-6, teff=0, mag=19.295"


def test_healpix560000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 560000)
    assert len(records) == 127
    assert records[0] == "ra=7.28948537599737, dec=-52.75378985924357, pmra=2, pmdec=-4, teff=4540, mag=17.758"


def test_healpix565000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 565000)
    assert len(records) == 78
    assert records[0] == "ra=33.40382389417096, dec=-35.27080984565932, pmra=5, pmdec=-1, teff=6232, mag=15.11"


def test_healpix570000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 570000)
    assert len(records) == 92
    assert records[0] == "ra=18.97860088338079, dec=-36.3842898590417, pmra=71, pmdec=-30, teff=0, mag=19.529"


def test_healpix575000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 575000)
    assert len(records) == 110
    assert records[0] == "ra=48.16642789550425, dec=-31.733259964610102, pmra=9, pmdec=-1, teff=0, mag=19.169"


def test_healpix580000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 580000)
    assert len(records) == 93
    assert records[0] == "ra=51.346122981675954, dec=-20.086766937788, pmra=0, pmdec=-4, teff=5450, mag=17.109"


def test_healpix585000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 585000)
    assert len(records) == 79
    assert records[0] == "ra=35.50338687165798, dec=-16.005321897568795, pmra=5, pmdec=-1, teff=3703, mag=17.759"


def test_healpix590000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 590000)
    assert len(records) == 127
    assert records[0] == "ra=112.5428848865176, dec=-87.0739099975088, pmra=-3, pmdec=7, teff=4849, mag=16.265"


def test_healpix595000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 595000)
    assert len(records) == 127
    assert records[0] == "ra=174.81974989865893, dec=-70.51060497318888, pmra=-3, pmdec=-1, teff=0, mag=14.551"


def test_healpix600000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 600000)
    assert len(records) == 127
    assert records[0] == "ra=125.02548985417256, dec=-63.4194798504093, pmra=-3, pmdec=14, teff=5347, mag=15.682"


def test_healpix605000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 605000)
    assert len(records) == 127
    assert records[0] == "ra=129.19983990918837, dec=-52.38947999309258, pmra=-1, pmdec=0, teff=0, mag=13.205"


def test_healpix610000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 610000)
    assert len(records) == 127
    assert records[0] == "ra=157.88752999058343, dec=-47.34463995664596, pmra=-19, pmdec=12, teff=4716, mag=14.451"


def test_healpix615000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 615000)
    assert len(records) == 127
    assert records[0] == "ra=146.7698198867821, dec=-48.50053793215218, pmra=-4, pmdec=4, teff=0, mag=13.888"


def test_healpix620000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 620000)
    assert len(records) == 127
    assert records[0] == "ra=165.23810988536016, dec=-29.30297290408191, pmra=-7, pmdec=4, teff=6546, mag=12.788"


def test_healpix625000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 625000)
    assert len(records) == 127
    assert records[0] == "ra=101.40182984126817, dec=-54.70255686654828, pmra=10, pmdec=-11, teff=4177, mag=17.119"


def test_healpix630000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 630000)
    assert len(records) == 127
    assert records[0] == "ra=123.74647496442611, dec=-37.14713994280768, pmra=-2, pmdec=3, teff=0, mag=12.862"


def test_healpix635000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 635000)
    assert len(records) == 127
    assert records[0] == "ra=113.57768984212431, dec=-38.25728597038299, pmra=-5, pmdec=11, teff=5743, mag=15.74"


def test_healpix640000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 640000)
    assert len(records) == 127
    assert records[0] == "ra=140.64510992758213, dec=-35.661635862505825, pmra=1, pmdec=-1, teff=5274, mag=13.926"


def test_healpix645000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 645000)
    assert len(records) == 127
    assert records[0] == "ra=150.1024798444018, dec=-17.87800989014935, pmra=-2, pmdec=-1, teff=5404, mag=17.278"


def test_healpix650000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 650000)
    assert len(records) == 127
    assert records[0] == "ra=118.82010994424117, dec=-18.819266938985916, pmra=1, pmdec=1, teff=0, mag=14.677"


def test_healpix655000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 655000)
    assert len(records) == 127
    assert records[0] == "ra=131.1224498465296, dec=-5.070068391538257, pmra=-3, pmdec=3, teff=0, mag=15.124"


def test_healpix660000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 660000)
    assert len(records) == 127
    assert records[0] == "ra=248.57289998259995, dec=-74.59666995080032, pmra=4, pmdec=0, teff=5515, mag=15.53"


def test_healpix665000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 665000)
    assert len(records) == 127
    assert records[0] == "ra=214.2876299537195, dec=-66.8040499402229, pmra=-3, pmdec=-2, teff=0, mag=14.215"


def test_healpix670000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 670000)
    assert len(records) == 127
    assert records[0] == "ra=221.06669983876247, dec=-55.855739943615966, pmra=-5, pmdec=-3, teff=0, mag=13.917"


def test_healpix675000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 675000)
    assert len(records) == 127
    assert records[0] == "ra=250.2842299688068, dec=-50.85587686433264, pmra=-1, pmdec=-4, teff=0, mag=14.841"


def test_healpix680000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 680000)
    assert len(records) == 127
    assert records[0] == "ra=241.18815997670782, dec=-52.76785687206679, pmra=-4, pmdec=-3, teff=0, mag=14.564"


def test_healpix685000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 685000)
    assert len(records) == 127
    assert records[0] == "ra=247.14834994038023, dec=-32.42963987981418, pmra=5, pmdec=0, teff=6524, mag=15.48"


def test_healpix690000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 690000)
    assert len(records) == 127
    assert records[0] == "ra=206.52591988748213, dec=-51.988659897813875, pmra=3, pmdec=-8, teff=5724, mag=15.074"


def test_healpix695000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 695000)
    assert len(records) == 127
    assert records[0] == "ra=205.66319993029495, dec=-40.60496987803139, pmra=-14, pmdec=-1, teff=4612, mag=16.016"


def test_healpix700000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 700000)
    assert len(records) == 127
    assert records[0] == "ra=189.13139996544055, dec=-36.39301994647506, pmra=-24, pmdec=-5, teff=6461, mag=10.498"


def test_healpix705000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 705000)
    assert len(records) == 127
    assert records[0] == "ra=226.7464899955068, dec=-34.56966984764192, pmra=-5, pmdec=-7, teff=5043, mag=15.134"


def test_healpix710000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 710000)
    assert len(records) == 127
    assert records[0] == "ra=246.0864099143475, dec=-19.45133385222933, pmra=-12, pmdec=-12, teff=4262, mag=16.756"


def test_healpix715000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 715000)
    assert len(records) == 127
    assert records[0] == "ra=207.76916986693124, dec=-20.40193387325943, pmra=1, pmdec=-8, teff=5337, mag=17.219"


def test_healpix720000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 720000)
    assert len(records) == 127
    assert records[0] == "ra=223.5894798597272, dec=-8.378345392820586, pmra=-9, pmdec=-11, teff=5223, mag=18.444"


def test_healpix725000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 725000)
    assert len(records) == 127
    assert records[0] == "ra=357.23601998632586, dec=-77.89958991012516, pmra=-3, pmdec=-14, teff=5507, mag=17.967"


def test_healpix730000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 730000)
    assert len(records) == 127
    assert records[0] == "ra=286.7406598509944, dec=-70.13529989409041, pmra=-1, pmdec=0, teff=0, mag=16.377"


def test_healpix735000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 735000)
    assert len(records) == 127
    assert records[0] == "ra=322.72619994484177, dec=-53.1825639368885, pmra=6, pmdec=-11, teff=5649, mag=17.942"


def test_healpix740000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 740000)
    assert len(records) == 127
    assert records[0] == "ra=331.27245983633793, dec=-55.10059685218175, pmra=-8, pmdec=-8, teff=4953, mag=18.433"


def test_healpix745000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 745000)
    assert len(records) == 122
    assert records[0] == "ra=344.8641999740452, dec=-37.52374991659249, pmra=5, pmdec=-6, teff=0, mag=19.107"


def test_healpix750000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 750000)
    assert len(records) == 127
    assert records[0] == "ra=338.9330999082574, dec=-35.648715996951196, pmra=6, pmdec=-7, teff=0, mag=19.705"


def test_healpix755000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 755000)
    assert len(records) == 127
    assert records[0] == "ra=295.17055998331426, dec=-55.475886843854504, pmra=-15, pmdec=-94, teff=0, mag=16.253"


def test_healpix760000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 760000)
    assert len(records) == 127
    assert records[0] == "ra=297.0206999858006, dec=-44.95818689696406, pmra=0, pmdec=-1, teff=5096, mag=16.204"


def test_healpix765000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 765000)
    assert len(records) == 127
    assert records[0] == "ra=282.3255599114697, dec=-39.813389982941274, pmra=3, pmdec=-8, teff=5662, mag=14.396"


def test_healpix770000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 770000)
    assert len(records) == 127
    assert records[0] == "ra=293.18649996686094, dec=-21.361889942252024, pmra=4, pmdec=-19, teff=5252, mag=12.147"


def test_healpix775000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 775000)
    assert len(records) == 127
    assert records[0] == "ra=328.00479986146314, dec=-22.335860888630087, pmra=-7, pmdec=-15, teff=0, mag=16.525"


def test_healpix780000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 780000)
    assert len(records) == 127
    assert records[0] == "ra=305.8741799303676, dec=-18.818039995999094, pmra=-2, pmdec=-1, teff=0, mag=16.776"


def test_healpix785000():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, 785000)
    assert len(records) == 127
    assert records[0] == "ra=306.92391992869034, dec=-9.877279964218511, pmra=-7, pmdec=-22, teff=5514, mag=15.274"
