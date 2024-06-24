# This is for Postgres with pgsphere extension

CREATE TABLE stars (
    source_id BIGINT PRIMARY KEY,
    random_index BIGINT,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    pmra DOUBLE PRECISION,
    pmdec DOUBLE PRECISION,
    phot_g_mean_mag REAL,
    phot_bp_mean_mag REAL,
    teff_gspphot REAL,
    sphere_point spoint
);


COPY stars (random_index, ra, dec, pmra, pmdec, phot_g_mean_mag, phot_bp_mean_mag, teff_gspphot, source_id) FROM '/path/to/your/file.csv' DELIMITER ',' CSV HEADER;

UPDATE stars SET sphere_point = spoint(radians(ra), radians(dec));

CREATE INDEX stars_sidx ON stars USING gist (sphere_point);

