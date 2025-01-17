CREATE TABLE stars (
    source_id BIGINT PRIMARY KEY,
    healpix8 INTEGER,
    ra REAL,
    dec REAL,
    pmra REAL,
    pmdec REAL,
    phot_g_mean_mag REAL,
    teff REAL,
    has_xp_sampled BOOLEAN,
    flux REAL[]
);

-- Run this to import data from the GAIA extract
COPY stars (ra, dec, pmra, pmdec, phot_g_mean_mag, source_id, has_xp_sampled, teff) FROM '/path/to/your/file.csv' DELIMITER ',' CSV HEADER NULL "null";

-- Create these after you populate data into stars
CREATE INDEX idx_healpix8_phot_g ON stars (healpix8, phot_g_mean_mag);
CREATE INDEX stars_xpsampled ON stars (has_xp_sampled);

-- Create the astrometry table
CREATE TABLE astrometry (
    source_id bigint NOT NULL,
    healpix8 real,
    PRIMARY KEY (source_id)
);
CREATE INDEX idx_results2_healpix8
ON astrometry (healpix8);

-- Create the photometry table
CREATE TABLE public.photometry (
    source_id bigint NOT NULL,
    healpix8 real,
    healpix2 real,
    PRIMARY KEY (source_id)
);
CREATE INDEX idx_photometry_healpix2_healpix8
ON public.photometry (healpix2, healpix8);


