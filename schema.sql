# This is for Postgres with pgsphere extension

CREATE TABLE stars (
    source_id BIGINT PRIMARY KEY,
    trixel INTEGER,
    ra REAL,
    dec REAL,
    pmra REAL,
    pmdec REAL,
    phot_g_mean_mag REAL,
    sphere_point spoint,
    flux REAL[],
);

COPY stars (ra, dec, pmra, pmdec, phot_g_mean_mag, phot_bp_mean_mag, source_id) FROM '/path/to/your/file.csv' DELIMITER ',' CSV HEADER;

UPDATE stars
SET sphere_point = spoint(radians(ra), radians(dec));

CREATE INDEX stars_sidx ON stars USING gist (sphere_point);
CREATE INDEX stars_phot_g_mean_mag_idx ON stars (phot_g_mean_mag);

ALTER TABLE stars drop column ra;
ALTER TABLE stars drop column dec;
CREATE INDEX stars_trixel_idx ON stars (trixel);

