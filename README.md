# GAIA Web Service for Siril (Proof of Concept)

This repo is a proof of concept for a GAIA Web Service for Siril

## Fetcher
There are two python scripts

### fetch_gaia_source.py
This is a python script which parses the html page at https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/ to get a list of csv file urls. It then downloads each file and processes it to ignore any line that doesn't meet the following criteria :-

* has_xp_sampled = True OR has_xp_continuous = True
* phot_g_mean_mag <= 15

It does this with a worker pool (default size 8). Each worker downloads and processes a file, deleting the file after it's complete. The worker outputs csv lines to a queue. A separate thread watches the queue and writes the lines to the output file. This is much more efficient than locking the file for write on multiple threads. The output file can then be bulk loaded into Postgres. This script does not do this step (yet)

### fetch_spectra.py
This is a pythong script which parses the html page at https://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_sampled_mean_spectrum/ to get a list of csv file urls. It then downloads each file and processes it to ignore comments, convert arrays to postgres format, and use the bulk loader to import into a Postgres temporary table. It then prunes this table to remove any lines that are not present in the star table as populated by the previous script and copies the remainder into the spectra table.

## The Database
This is a PostgresQL database with pgSphere extension. This allows us to populate the astro data and create an index for the astro coordinates. This means we can then look up stars within a certain radius.

## The Schema
Specifies a schema for the PostgresQL database with pgSphere extension

## The Web App
This is an asynchronous FastAPI app that exposes 2 GET endpoints:-

* /star/<source_id>
* /stars?ra=<ra>&dec=<dec>&radius=<radius>&mag=<magnitude>
* /spectra/<source_id>
* /spectras?ra=<ra>&dec=<dec>&radius=<radius>&mag=<magnitude>

Run with ```/usr/bin/uvicorn main:app --host 0.0.0.0 --port 8000```
