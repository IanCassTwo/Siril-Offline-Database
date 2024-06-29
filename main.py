#
# Run with 'uvicorn main:app --reload' when developing
# or use the systemctl service for production
#
# Always run behind Nginx or Apache
#
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List
from typing import Optional

app = FastAPI()

class Star(BaseModel):
    source_id: int
    ra: float
    dec: float
    pmra: Optional[float]
    pmdec: Optional[float]
    phot_g_mean_mag: float
    phot_bp_mean_mag: float

class Spectra(BaseModel):
    source_id: int
    wavelength: List[float]
    flux: List[float]
    flux_error: List[float]

class StarsAndSpectra(BaseModel):
    source_id: int
    ra: float
    dec: float
    pmra: Optional[float]
    pmdec: Optional[float]
    phot_g_mean_mag: float
    phot_bp_mean_mag: float
    wavelength: List[float]
    flux: List[float]
    flux_error: List[float]

def get_db_connection():
    conn = psycopg2.connect(
        dbname='stars',
        user='stars',
        password='stars',
        host='localhost'
    )
    return conn

@app.get("/star/{source_id}", response_model=Star)
def get_star_by_id(source_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT source_id, ra, dec, pmra, pmdec, phot_g_mean_mag, phot_bp_mean_mag FROM stars WHERE source_id = %s", (source_id,))
    star = cursor.fetchone()
    cursor.close()
    conn.close()
    if star is None:
        raise HTTPException(status_code=404, detail="Star data not found")
    return star

@app.get("/spectra/{source_id}", response_model=Spectra)
def get_spectra_by_id(source_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM spectra WHERE source_id = %s", (source_id,))
    spectra = cursor.fetchone()
    cursor.close()
    conn.close()
    if spectra is None:
        raise HTTPException(status_code=404, detail="Spectra data not found")

    return spectra;

@app.get("/stars", response_model=List[Star])
def star_search(ra: float, dec: float, radius: float, mag: float):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = """
    SELECT source_id, ra, dec, pmra, pmdec, phot_g_mean_mag, phot_bp_mean_mag
    FROM stars
    WHERE sphere_point <@ scircle(spoint(radians(%s), radians(%s)), radians(%s))
    AND phot_g_mean_mag <= %s
    LIMIT 5000
    """
    cursor.execute(query, (ra, dec, radius, mag))
    stars = cursor.fetchall()
    cursor.close()
    conn.close()
    return stars

@app.get("/spectras", response_model=List[StarsAndSpectra])
def spectra_search(ra: float, dec: float, radius: float, mag: float):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = """
    SELECT st.source_id, st.ra, st.dec, st.pmra, st.pmdec, st.phot_g_mean_mag, st.phot_bp_mean_mag, sp.wavelength, sp.flux, sp.flux_error
    FROM stars st, spectra sp
    WHERE st.sphere_point <@ scircle(spoint(radians(%s), radians(%s)), radians(%s))
    AND st.phot_g_mean_mag <= %s
    AND st.source_id = sp.source_id
    LIMIT 5000
    """
    cursor.execute(query, (ra, dec, radius, mag))
    stars = cursor.fetchall()
    cursor.close()
    conn.close()
    return stars

# Run the FastAPI application with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

