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
    random_index: int
    ra: float
    dec: float
    pmra: Optional[float]
    pmdec: Optional[float]
    phot_g_mean_mag: float
    phot_bp_mean_mag: float

def get_db_connection():
    conn = psycopg2.connect(
        dbname='stars',
        user='stars',
        password='stars',
        host='localhost'
    )
    return conn

@app.get("/star", response_model=Star)
def get_star_by_id(source_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM stars WHERE source_id = %s", (source_id,))
    star = cursor.fetchone()
    cursor.close()
    conn.close()
    if star is None:
        raise HTTPException(status_code=404, detail="Star not found")
    return star

@app.get("/search", response_model=List[Star])
def cone_search(ra: float, dec: float, radius: float, mag: float):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = """
    SELECT source_id, random_index, ra, dec, pmra, pmdec, phot_g_mean_mag, phot_bp_mean_mag
    FROM stars
    WHERE sphere_point @ scircle(spoint(radians(%s), radians(%s)), radians(%s))
    AND phot_g_mean_mag <= %s
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

