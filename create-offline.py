import psycopg2
import logging
import struct
import numpy as np
import math

class DataType:
    DT_CHAR = 0    # Character
    DT_INT8 = 1    # 8-bit Integer
    DT_UINT8 = 2   # 8-bit Unsigned Integer
    DT_INT16 = 3   # 16-bit Integer
    DT_UINT16 = 4  # 16-bit Unsigned Integer
    DT_INT32 = 5   # 32-bit Integer
    DT_UINT32 = 6  # 32-bit Unsigned Integer
    DT_CHARV = 7   # Fixed-length array of characters
    DT_STR = 8     # Variable length array of characters
    DT_SPCL = 128  # Special treatment flag

    @staticmethod
    def from_datasize(size):
        if size == DataSize.INTEGER:
            return DataType.DT_INT32
        elif size == DataSize.SMALLINT:
            return DataType.DT_INT16
        elif size == DataSize.BIGINT:
            return DataType.DT_UINT32
        elif size == DataSize.REAL:
            return DataType.DT_INT32  # Assuming REAL maps to 32-bit integer
        elif size == DataSize.DOUBLE_PRECISION:
            return DataType.DT_UINT32  # Assuming DOUBLE PRECISION maps to 64-bit unsigned integer
        elif size == DataSize.SERIAL:
            return DataType.DT_INT32
        elif size == DataSize.BIGSERIAL:
            return DataType.DT_UINT32
        elif size == DataSize.BOOLEAN:
            return DataType.DT_UINT8
        elif size == DataSize.CHAR:
            return DataType.DT_CHAR
        elif size == DataSize.VARCHAR:
            return DataType.DT_STR
        elif size == DataSize.TEXT:
            return DataType.DT_STR
        elif size == DataSize.DATE:
            return DataType.DT_INT32
        elif size == DataSize.TIMESTAMP:
            return DataType.DT_UINT32
        elif size == DataSize.TIMESTAMPTZ:
            return DataType.DT_UINT32
        elif size == DataSize.BYTEA:
            return DataType.DT_CHARV
        else:
            return DataType.DT_SPCL  # Default to special treatment flag if type is unknown

class DataSize:
    INTEGER = 4  # INTEGER is 4 bytes
    SMALLINT = 2  # SMALLINT is 2 bytes
    BIGINT = 8  # BIGINT is 8 bytes
    REAL = 4  # REAL is 4 bytes (single-precision floating-point)
    DOUBLE_PRECISION = 8  # DOUBLE PRECISION is 8 bytes (double-precision floating-point)
    SERIAL = 4  # SERIAL is an auto-incrementing integer, equivalent to INTEGER
    BIGSERIAL = 8  # BIGSERIAL is an auto-incrementing bigint, equivalent to BIGINT
    BOOLEAN = 1  # BOOLEAN is 1 byte
    CHAR = 1  # CHAR is 1 byte per character (fixed-length)
    VARCHAR = 0  # VARCHAR has a variable length, not fixed
    TEXT = 0  # TEXT has a variable length, not fixed
    DATE = 4  # DATE is 4 bytes
    TIMESTAMP = 8  # TIMESTAMP is 8 bytes
    TIMESTAMPTZ = 8  # TIMESTAMPTZ (TIMESTAMP WITH TIME ZONE) is 8 bytes
    BYTEA = 0  # BYTEA has a variable length, not fixed

# Define your database connection parameters
db_params = {
    'dbname': 'stars',
    'user': 'stars',
    'password': 'stars',
    'host': 'localhost',
    'port': '5432'
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

INDEX_START = 0
total_records = 0

# SQL queries
dataquery = """
SELECT trixel, source_id, ra, dec, pmra, pmdec, mag, flux
FROM stars
WHERE trixel IS NOT NULL
ORDER BY trixel
"""
#WHERE trixel IN (8177, 8178)

def process_record(record):
    # Placeholder function to process each record
    print(record)

def writeHeader(file):
    writePreamble(file)
    logging.debug("Written preamble")
    writeFieldDescriptor(file)
    logging.debug("Written field descriptor")

def writePreamble(file):
    current_position = file.tell()
    logging.debug(F"Writing preamble at {current_position:x}")
    file.write(struct.pack('124s',"Siril Spectral Data".encode('ascii'))) # Description
    file.write(struct.pack('h', 0x4B53)) # BOM
    file.write(struct.pack('B', 0x01)) # Version

def writeFieldDescriptor(file):
    current_position = file.tell()
    logging.debug(F"Writing fieldDescriptor at {current_position:x}")
    file.write(struct.pack('H', 350)) # Num Fields
    #writeDataElementDescriptor(file, 'Trixel', DataSize.SMALLINT, DataType.from_datasize(DataSize.INTEGER), 0)
    writeDataElementDescriptor(file, 'Source ID', DataSize.BIGINT, DataType.from_datasize(DataSize.BIGINT), 0) 
    writeDataElementDescriptor(file, 'RA', DataSize.REAL, DataType.from_datasize(DataSize.REAL), 1000000) 
    writeDataElementDescriptor(file, 'Dec', DataSize.REAL, DataType.from_datasize(DataSize.REAL), 100000) 
    writeDataElementDescriptor(file, 'pmRA', DataSize.REAL, DataType.from_datasize(DataSize.REAL), 1000) 
    writeDataElementDescriptor(file, 'pmDec', DataSize.REAL, DataType.from_datasize(DataSize.REAL), 1000) 
    writeDataElementDescriptor(file, 'Magnitude', DataSize.SMALLINT, DataType.from_datasize(DataSize.REAL), 100) 
    writeDataElementDescriptor(file, 'FExponent', DataSize.CHAR, DataType.DT_INT8, 0) 

    for i in range(0, 343):
        writeDataElementDescriptor(file, f"Flux {i}", DataSize.SMALLINT, DataType.DT_INT16, 1000000000) # Note: We want 1e10 here but it deesn't fit

def writeDataElementDescriptor(file, name, field_size, data_type, scale_factor):
    file.write(struct.pack('10sbBi', name.encode('ascii'), field_size, data_type, scale_factor))

def writeIndex(file):
    index_start = file.tell()

    logging.debug(f"Writing placeholder index at {index_start:x}")
    file.write(struct.pack('I', 8192))  # Num Trixels

    global INDEX_START
    INDEX_START = file.tell()
    logging.debug(f"Index entries start at {INDEX_START:x}")

    # write placeholder index entry
    for i in range(0, 8192):
        data = struct.pack('III', i, 0, 0)
        file.write(data)

def updateIndex(file, trixel_id, numrecords, offset):
    current_position = file.tell() # Somewhere in the data partition
    logging.info(F"Updating Index for trixel {trixel_id} for {numrecords} records")
    file.seek(INDEX_START)

    global total_records
    total_records += numrecords

    for i in range(0, 8192):
        trixel_id_bytes = file.read(4)
        trixel_id_value = struct.unpack('I', trixel_id_bytes)[0]
        if trixel_id_value == trixel_id:
            idxp = file.tell()
            logging.debug(f"Located index entry for trixel {trixel_id} at file position {idxp:x}, updating numrecords to {numrecords} and offset to {offset:x}")
            file.write(struct.pack('II', offset, numrecords))
            break
        file.seek(file.tell() + 8) # Skip to next trixel_id

    file.seek(current_position) # return to the data partition

def writeExpansion(file):
    current_position = file.tell()
    logging.debug(F"Writing Expansion at {current_position:x}")
    file.write(struct.pack('hBH', 15 * 100, 6, 3177)) # mag limit, htm level, stars per trixel
    # TODO update mag limit and stars per trixel

def writeData(file, conn):
    writeExpansion(file)
    logging.debug("Written expansion")
    writeDataRecords(file, conn)
    logging.debug("Written data")


def writeDataRecords(file, conn):
    current_position = file.tell()
    logging.debug(F"Writing Data at {current_position:x}")
    with conn.cursor(name='streaming_cursor') as cursor:
        cursor.execute(dataquery)
        new_trixel_id = 0
        old_trixel_id = -1
        numrecords = 0
        while True:
            record = cursor.fetchone()
            if record is None:
                break

            new_trixel_id = record[0]

            # First record?
            if old_trixel_id < 0:
                old_trixel_id = new_trixel_id
            
            # Do we need to update the index on trixel change?
            if new_trixel_id != old_trixel_id:
                logging.debug(f"Got a new trixel old is {old_trixel_id} and new is {new_trixel_id}")
                updateIndex(file, old_trixel_id, numrecords, current_position)
                old_trixel_id = new_trixel_id
                numrecords = 0
                current_position = file.tell() # position at the start of the new trixel data

            # Write our data
            writeDataElement(file, record)
            numrecords += 1

        # Update remainder
        updateIndex(file, new_trixel_id, numrecords, current_position)

def writeDataElement(file, record):
    start = file.tell()
    #print(f"Started {record[0]} at {start}")
    #file.write(struct.pack('H', record[0]))   # Trixel
    file.write(struct.pack('Q', record[1]))   # Source ID

    RA = record[2] * 1000000
    Dec = record[3] * 100000
    #logging.debug(f"RA = {RA}, Dec = {Dec}")
    file.write(struct.pack('ii', int(RA), int(Dec)))

    pmRA = 0
    if record[4] is not None:
        pmRA = record[4] * 1000
    #logging.debug(f"pmRA = {pmRA}")
    file.write(struct.pack('i', int(pmRA)))

    pmDec = 0
    if record[5] is not None:
        pmDec = record[5] * 1000
    #logging.debug(f"pmDec = {pmDec}")
    file.write(struct.pack('i', int(pmDec)))

    file.write(struct.pack('e', np.float16(record[6])))  # mag

    # Prep flux data
    largest_num = max(abs(num) for num in record[7])
    e = math.ceil(-math.log10(largest_num))
    file.write(struct.pack('B', e))

    # Write half-precision, scaled flux data
    for data in record[7]:
        data *= (10 ** e)
        file.write(struct.pack('e', np.float16(data)))

    end = file.tell()
    #logging.debug(f"Wrote {end - start} bytes")

def main():
    logging.info("Exporter started")
    with psycopg2.connect(**db_params) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        logging.info("Connected to Postgres")

        # Open output file
        with open('data.bin', 'wb+') as file:
            logging.info("Opened output file")
            writeHeader(file)
            logging.debug("-----------")
            writeIndex(file)
            logging.debug("-----------")
            writeData(file, conn)
            logging.debug("-----------")

        logging.info(f"Finished. Wrote {total_records} records")
        
if __name__ == "__main__":
    main()
