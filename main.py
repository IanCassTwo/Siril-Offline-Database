from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Type, TypeVar
import asyncpg
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import json

app = FastAPI()

ModelType = TypeVar('ModelType', bound=BaseModel)

class Star(BaseModel):
    source_id: int
    ra: float
    dec: float
    pmra: Optional[float]
    pmdec: Optional[float]
    phot_g_mean_mag: float

class Spectra(Star):
    flux: List[float]

async def get_db_connection():
    return await asyncpg.connect('postgresql://stars:stars@localhost:5432/stars')

async def fetch_many(conn, query: str, model_class: Type[ModelType], *args):
    try:
        async with conn.transaction():
            yield b"["
            first = True
            async for row in conn.cursor(query, *args):
                if not first:
                    yield b","
                instance = model_class.parse_obj(row)
                yield json.dumps(instance.dict()).encode('utf-8')
                first = False
            yield b"]"
    except asyncpg.exceptions.InterfaceError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

async def fetch_one(conn, query: str, model_class: Type[ModelType], *args):
    try:
        async with conn.transaction():
            row = await conn.fetchrow(query, *args)
            if row:
                return model_class.parse_obj(row)
            else:
                raise HTTPException(status_code=404, detail="Result not found")
    except asyncpg.exceptions.InterfaceError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

@app.get("/spectra/{source_id}", response_model=Spectra)
async def get_spectra_by_id(source_id: int):
    conn = await get_db_connection()
    query = "SELECT source_id, degrees(long(sphere_point)) AS ra, degrees(lat(sphere_point)) AS dec, pmra, pmdec, phot_g_mean_mag, flux FROM stars WHERE source_id = $1"
    return await fetch_one(conn, query, Spectra, source_id)

@app.get("/star/{source_id}", response_model=Star)
async def get_star_by_id(source_id: int):
    conn = await get_db_connection()
    query = "SELECT source_id, degrees(long(sphere_point)) AS ra, degrees(lat(sphere_point)) AS dec, pmra, pmdec, phot_g_mean_mag  FROM stars WHERE source_id = $1"
    return await fetch_one(conn, query, Star, source_id)

@app.get("/stars", response_model=List[Star])
async def star_search(ra: float, dec: float, radius: float, mag: float):
    conn = await get_db_connection()
    query = """
    SELECT source_id, degrees(long(sphere_point)) AS ra, degrees(lat(sphere_point)) AS dec, pmra, pmdec, phot_g_mean_mag
    FROM stars
    WHERE sphere_point <@ scircle(spoint(radians($1), radians($2)), radians($3))
    AND phot_g_mean_mag <= $4
    LIMIT 5000
    """
    return StreamingResponse(fetch_many(conn, query, Star, ra, dec, radius, mag), media_type="application/json")

@app.get("/spectras", response_model=List[Spectra])
async def spectra_search(ra: float, dec: float, radius: float, mag: float):
    conn = await get_db_connection()
    query = """
    SELECT source_id, degrees(long(sphere_point)) AS ra, degrees(lat(sphere_point)) AS dec, pmra, pmdec, phot_g_mean_mag, flux
    FROM stars
    WHERE sphere_point <@ scircle(spoint(radians($1), radians($2)), radians($3))
    AND phot_g_mean_mag <= $4
    LIMIT 5000
    """
    return StreamingResponse(fetch_many(conn, query, Spectra, ra, dec, radius, mag), media_type="application/json")
