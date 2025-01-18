import psycopg2
import logging
import struct
import numpy as np
import math

# Define your database connection parameters
db_params = {
    'dbname': 'stars2',
    'user': 'stars',
    'password': 'stars',
    'host': 'localhost',
    'port': '5432'
}

MAXHEALPIX = 786431
#MAXHEALPIX = 2
INT32_MAX = 2**31 -1
RADEC_SCALE = INT32_MAX / 360.0

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# SQL queries
dataquery = """
SELECT s.source_id as source_id, s.ra as ra, s.dec as dec, s.pmra as pmra, s.pmdec as pmdec, s.phot_g_mean_mag as phot_g_mean_mag, s.healpix8 as healpix8, s.teff
FROM stars s, astrometry r
where s.source_id = r.source_id AND r.healpix8 = %s
ORDER BY s.source_id;
"""
#where s.source_id = r.source_id AND r.healpix8 between %s and %s

#indexquery = """
#SELECT s.healpix8, COUNT(*) AS entry_count
#FROM stars s, astrometry r where s.source_id = r.source_id and healpix8 = %s
#GROUP BY s.healpix8
#"""

indexquery = """
select count(healpix8) from astrometry where healpix8 = %s
"""
#select healpix8, count(healpix8) from astrometry group by healpix8 order by healpix8


total_records = 0

def writeHeader(file):
    catalogue_title = "Siril Gaia DR3 astrometric extract".ljust(48, '\x00')  # 48 bytes, padded with null bytes
    gaia_version = 3  # DR3
    healpix_level = 8  
    catalogue_type = 1  # astrometric = 1, photometric with xp_sampled data = 2, photometric with xp_continuous data = 3
    spare = b'\x00' * 77  # 77 bytes of reserved space

    # Pack the data into a binary format
    header_format = '48s B B B 77s'
    file.write(struct.pack(
        header_format,
        catalogue_title.encode('ascii'),
        gaia_version,
        healpix_level,
        catalogue_type,
        spare
    ));

def writeIndexRecords(file, conn):
    with conn.cursor() as cursor:
        i = 0
        index_records = 0
        while i <= MAXHEALPIX:
            cursor.execute(indexquery, (i, ))
            record = cursor.fetchone()
            if record is not None:
                index_records += record[0]
            file.write(struct.pack('I', int(index_records)))
            i += 1
            if i % 1000 == 0:
                print(f"Index {i}")
        print(f"Written index records {i} ")

def writeDataRecords(file, conn):
    current_position = file.tell()
    logging.debug(F"Writing Data at {current_position:x}")
    with conn.cursor() as cursor:
        i = 0
        while i <= MAXHEALPIX:
            cursor.execute(dataquery, (i,))
            numrecords = 0
            if i % 1000 == 0:
                print(f"Processing healpix {i}")
            while True:
                record = cursor.fetchone()
                if record is None:
                    break

                # Write our data
                writeDataElement(file, record)
                numrecords += 1
            i += 1

def writeDataRecordsNew(file, conn):
    current_position = file.tell()
    logging.debug(f"Writing Data at {current_position:x}")
    
    with conn.cursor() as cursor:
        for i in range(MAXHEALPIX + 1):
            cursor.execute(dataquery, (i,))
            if i % 1000 == 0:
                print(f"Processing healpix {i}")
            
            # Fetch all rows for this healpix and iterate
            records = cursor.fetchall()
            for record in records:
                writeDataElement(file, record)

def writeDataRecordsOld(file, conn, start, end):
    current_position = file.tell()
    logging.debug(F"Writing Data at {current_position:x}")
    healpix8 = 0
    with conn.cursor() as cursor:
        cursor.execute(dataquery, (start, end))
        numrecords = 0
        while True:
            record = cursor.fetchone()
            if record is None:
                break

            if healpix8 != record[6]:
                healpix8 = record[6]
                if healpix8 % 1000 == 0:
                    print(f"Processing healpix {healpix8}")

            # Write our data
            writeDataElement(file, record)
            numrecords += 1

def writeDataElement(file, record):
    global total_records

    start = file.tell()
    #print(f"Started {record[0]} at {start}")
    total_records += 1;
    #file.write(struct.pack('Q', record[0]))   # Source ID

    #RA = record[1] * 1000000
    #Dec = record[2] * 100000
    RA = record[1] * RADEC_SCALE
    Dec = record[2] * RADEC_SCALE
    #logging.debug(f"RA = {RA}, Dec = {Dec}")
    file.write(struct.pack('ii', int(RA), int(Dec)))

    pmRA = 0
    if record[3] is not None:
        pmRA = record[3]
    #logging.debug(f"pmRA = {pmRA}")
    file.write(struct.pack('h', int(pmRA)))

    pmDec = 0
    if record[4] is not None:
        pmDec = record[4]
    #logging.debug(f"pmDec = {pmDec}")
    file.write(struct.pack('h', int(pmDec)))

    teff = 0
    if record[7] is not None:
        teff = record[7]
    file.write(struct.pack('H', int(teff)))  # teff does not exist yet

    mag = 0
    if record[5] is not None:
        mag = record[5] * 1000
    file.write(struct.pack('h', int(mag)))  # mag

    ## Prep flux data
    #largest_num = max(abs(num) for num in record[4])
    #e = math.ceil(-math.log10(largest_num))
    #file.write(struct.pack('B', e))

    ## Write half-precision, scaled flux data
    #for data in record[4]:
    #    data *= (10 ** e)
    #    file.write(struct.pack('e', np.float16(data)))

    end = file.tell()
    #logging.debug(f"Wrote {end - start} bytes")

def main():
    logging.info("Exporter started")
    with psycopg2.connect(**db_params) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        logging.info("Connected to Postgres")

        # Open output file
        with open('siril_cat_healpix8_astro.dat', 'wb+') as file:
            logging.info("Opened output file")
            writeHeader(file)
            writeIndexRecords(file, conn)
            writeDataRecords(file, conn)
            logging.debug("-----------")

        logging.info(f"Finished {total_records}")
        
if __name__ == "__main__":
    main()
