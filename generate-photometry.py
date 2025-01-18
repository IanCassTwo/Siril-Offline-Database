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

#MAXCHUNKPIXEL = 1 # level 2
#MAXHEALPIX = 2 # level 8

MAXCHUNKPIXEL = 191 # level 2
MAXHEALPIX = 786431 # level 8
INT32_MAX = 2**31 -1
RADEC_SCALE = INT32_MAX / 360.0

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# SQL queries
dataquery = """
SELECT s.source_id as source_id, s.ra as ra, s.dec as dec, s.pmra as pmra, s.pmdec as pmdec, s.phot_g_mean_mag as phot_g_mean_mag, s.flux
FROM stars s, photometry r
where s.source_id = r.source_id AND r.healpix8 = %s AND r.healpix2 = %s
ORDER BY s.source_id;
"""
#where s.source_id = r.source_id AND r.healpix8 between %s and %s

#indexquery = """
#SELECT s.healpix8, COUNT(*) AS entry_count
#FROM stars s, results r where s.source_id = r.source_id and healpix8 = %s
#GROUP BY s.healpix8
#"""

indexquery = """
select count(healpix8) from photometry where healpix8 = %s and healpix2 = %s
"""
#select healpix8, count(healpix8) from results8 group by healpix8 order by healpix8


total_records = 0
current_chunk = 0

def writeHeader(file, first_healpix, last_healpix, chunk_healpix):

    catalogue_title = f"Siril Gaia DR3 photometric extract chunk {chunk_healpix}".ljust(48, '\x00')  # 48 bytes, padded with null bytes
    gaia_version = 3  # DR3
    healpix_level = 8  
    catalogue_type = 2  # astrometric = 1, photometric with xp_sampled data = 2, photometric with xp_continuous data = 3
    chunked = 1
    catalogue_level = 2

    spare = b'\x00' * 63  # reserved space

    logging.debug(f"catalogue_title = {catalogue_title}, gaia_version = {gaia_version}, healpix_level = {healpix_level}, catalogue_type = {catalogue_type}, catalogue_level = {catalogue_level}, chunked = {chunked}, chunk_healpix = {chunk_healpix}, first_healpix = {first_healpix}, last_healpix = {last_healpix}")

    # Pack the data into a binary format
    header_format = '=48s B B B B B I I I 63s'
    file.write(struct.pack(
        header_format,
        catalogue_title.encode('ascii'),
        gaia_version,
        healpix_level,
        catalogue_type,
        chunked,
        catalogue_level,
        chunk_healpix,
        int(first_healpix),
        int(last_healpix),
        spare
    ));

def writeIndexRecords(file, conn, first_healpix, last_healpix, chunk_healpix):
    writeHeader(file, first_healpix, last_healpix, chunk_healpix)
    with conn.cursor() as cursor:

        i = first_healpix
        index_records = 0
        while i <= last_healpix:
            cursor.execute(indexquery, (i, chunk_healpix ))
            record = cursor.fetchone()
            if record is not None:
                index_records += record[0]
            file.write(struct.pack('I', int(index_records)))
            if i % 1000 == 0:
                print(f"Index {i} for chunk {chunk_healpix} is {index_records}")
            i += 1
        print(f"Written index records {i} for chunk {chunk_healpix}")

def writeDataRecords(file, conn, chunk_healpix):
    current_position = file.tell()
    logging.debug(F"Writing Data at {current_position:x} for chunk {chunk_healpix}")
    with conn.cursor() as cursor:
        cursor.execute("select healpix8 from photometry where healpix2 = %s order by healpix8 asc limit 1;", (chunk_healpix, ))
        first_healpix = cursor.fetchone()[0]

        cursor.execute("select healpix8 from photometry where healpix2 = %s order by healpix8 desc limit 1;", (chunk_healpix, ))
        last_healpix = cursor.fetchone()[0]

        writeIndexRecords(file, conn, first_healpix, last_healpix, chunk_healpix)

        i = first_healpix
        while i <= last_healpix:
            cursor.execute(dataquery, (i,chunk_healpix))
            numrecords = 0
            if i % 1000 == 0:
                print(f"Processing healpix {i} for chunk {chunk_healpix}")
            while True:
                record = cursor.fetchone()
                if record is None:
                    break

                # Write our data
                writeDataElement(file, record)
                numrecords += 1
            i += 1
        print(f"Written data records {i} for chunk {chunk_healpix}")

def writeDataElement(file, record):
    global total_records

    start = file.tell()
    #print(f"Started {record[0]} at {start}")
    total_records += 1;
    #file.write(struct.pack('Q', record[0]))   # Source ID

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

    mag = 0
    if record[5] is not None:
        mag = record[5] * 1000
    file.write(struct.pack('h', int(mag)))  # mag

    ## Prep flux data
    flux = record[6]
    largest_num = max(abs(num) for num in flux)
    e = math.ceil(-math.log10(largest_num))
    file.write(struct.pack('B', e))

    # Write half-precision, scaled flux data
    for data in flux:
        data *= (10 ** e)
        file.write(struct.pack('e', np.float16(data)))

    end = file.tell()
    #logging.debug(f"Wrote {end - start} bytes")

def main():
    logging.info("Exporter started")
    with psycopg2.connect(**db_params) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        logging.info("Connected to Postgres")

        i = 0
        while i <= MAXCHUNKPIXEL:    
            # Open output file
            with open(f"siril_cat2_healpix8_xpsamp_{i}.dat", 'wb+') as file:

                logging.info(f"Opened output file siril_cat_healpix_xpsamp_level{i}.dat")
                writeDataRecords(file, conn, i)
                logging.debug("-----------")
            i+=1

        logging.info(f"Finished {total_records}")
        
if __name__ == "__main__":
    main()
