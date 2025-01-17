import psycopg2
import random
import struct

# Database connection details
DB_CONFIG = {
    "dbname": "stars2",
    "user": "stars",
    "password": "stars",
    "host": "localhost",
    "port": 5432,
}

SQL_QUERY = """
SELECT s.ra as ra, 
       s.dec as dec, 
       s.pmra as pmra, 
       s.pmdec as pmdec, 
       s.teff as teff,
       s.phot_g_mean_mag as mag
FROM stars s, astrometry r
WHERE s.source_id = r.source_id 
  AND r.healpix8 = %s
ORDER BY s.source_id;
"""

TEST_TEMPLATE = """
def test_healpix{healpix}():
    catalogue_title, gaia_version, healpix_level, catalogue_type, records = read_record(FILENAME, {healpix})
    assert len(records) == {num_records}
    assert records[0] == "ra={ra}, dec={dec}, pmra={pmra}, pmdec={pmdec}, teff={teff}, mag={mag}"
"""

def fetch_records(healpix8, connection):
    """Fetch records from the database for a given healpix8."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_QUERY, (healpix8,))
        return cursor.fetchall()

def generate_test_case(healpix, record, num_records):
    """Generate a single test case."""
    ra, dec, pmra, pmdec, teff, mag = record

    if pmra is None:
        pmra = 0
    if pmdec is None:
        pmdec = 0
    if teff is None:
        teff = 0
    if mag is None:
        mag = 0

    ra *= 1000000
    dec *= 100000
    mag *= 1000

    record = struct.pack('iihhHh', int(ra), int(dec), int(pmra), int(pmdec), int(teff), int(mag))
    
    urecord = struct.unpack('iihhHh', record)
    ura, udec, upmra, upmdec, uteff, umag  = urecord

    ura /= 1000000
    udec /= 100000
    umag /= 1000

    return TEST_TEMPLATE.format(
        healpix=healpix,
        num_records=num_records,
        ra=ura,
        dec=udec,
        pmra=upmra,
        pmdec=upmdec,
        teff=uteff,
        mag=umag
    )

def main():
    connection = psycopg2.connect(**DB_CONFIG)
    test_cases = []

    try:
        for healpix in range(0, 786432, 5000):
            records = fetch_records(healpix, connection)
            num_records = len(records)

            if num_records == 0:
                continue  # Skip if no records found

            test_case = generate_test_case(healpix, records[0], num_records)
            test_cases.append(test_case)

    finally:
        connection.close()

    # Write test cases to a file
    with open("test_healpix.py", "w") as test_file:
        test_file.write("\n".join(test_cases))
        print("Test cases written to test_healpix.py")

if __name__ == "__main__":
    main()

