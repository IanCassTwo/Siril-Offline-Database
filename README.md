Steps needed to create database:-

1) Run fetch_gaia_source.py. This will create a bunch of csv files
2) Create your stars table according to create.sql
3) Run the copy command for each of the csv files to import your data into stars
4) Run fetch_spectra.py to update your database with spectra data
5) Run update-healpix8.pl to populate this column. This is necessary for indexing
6) Create your indexes on stars
7) Create astrometry and photometry tables
8) Run find-127-astrometry.pl and find-127-photometry.pl to find the 127 brightest stars for each healpixel and level 8
9) Now you're ready to create your binary files. You may now run generate-astrometry.py and generate-photometry.py

This entire process will take a LONG time and it will consume a lot of disk space. The stars database will contain
1,060,363,318 entries of which 34,445,873 have flux data. This assume you're populating up to mag 20.

Once your binary catalogues are written, you can query them using the index_read scripts.

