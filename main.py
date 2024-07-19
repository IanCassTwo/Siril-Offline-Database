from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import List, Optional, Type, TypeVar
import asyncpg
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import json

app = FastAPI()

ModelType = TypeVar('ModelType', bound=BaseModel)

class Star(BaseModel):
    source_id: str
    trixel: int
    ra: float
    dec: float
    pmra: Optional[float]
    pmdec: Optional[float]
    mag: float

class Spectra(Star):
    flux: List[float]

async def get_db_connection():
    return await asyncpg.connect('postgresql://stars:stars@localhost:5432/stars')

def validate_parameters(
    ra: Optional[float] = Query(None),
    dec: Optional[float] = Query(None),
    radius: Optional[float] = Query(None),
    mag: Optional[float] = Query(None),
    trixel: Optional[int] = Query(None)
):
    if trixel is not None and mag is not None:
        # If trixel and mag are provided, they are valid
        return {"trixel": trixel, "mag": mag}
    elif ra is not None and dec is not None and radius is not None and mag is not None:
        # If ra, dec, radius, and mag are provided, they are valid
        return {"ra": ra, "dec": dec, "radius": radius, "mag": mag}
    else:
        # If neither set of parameters is fully provided, raise an exception
        raise HTTPException(
            status_code=400,
            detail="Either (ra, dec, radius, mag) or (trixel, mag) must be provided."
        )


async def fetch_many(conn, query: str, model_class: Type[ModelType], *args):
    try:
        async with conn.transaction():
            yield b"["
            first = True
            async for row in conn.cursor(query, *args):
                if not first:
                    yield b","
                row_dict = dict(row)
                instance = model_class.parse_obj(row_dict)
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
                row_dict = dict(row)
                return model_class.parse_obj(row_dict)
            else:
                raise HTTPException(status_code=404, detail="Result not found")
    except asyncpg.exceptions.InterfaceError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

@app.get("/spectra/{source_id}", response_model=Spectra)
async def get_spectra_by_id(source_id: int):
    conn = await get_db_connection()
    query = "SELECT source_id::text, trixel, ra, dec, pmra, pmdec, mag, flux FROM stars WHERE source_id = $1"
    return await fetch_one(conn, query, Spectra, source_id)

@app.get("/star/{source_id}", response_model=Star)
async def get_star_by_id(source_id: int):
    conn = await get_db_connection()
    query = "SELECT source_id::text, trixel, ra, dec, pmra, pmdec, mag  FROM stars WHERE source_id = $1"
    return await fetch_one(conn, query, Star, source_id)

@app.get("/stars", response_model=List[Star])
async def star_search(params: dict = Depends(validate_parameters)):
    conn = await get_db_connection()
    query = ""
    if "trixel" in params:
        query = """
        SELECT source_id::text,trixel, ra, dec, pmra, pmdec, mag
        FROM stars
        WHERE trixel = $1
        AND mag <= $2
        LIMIT 5000
        """
        return StreamingResponse(fetch_many(conn, query, Star, params["trixel"], params["mag"]), media_type="application/json")
    else:
        query = """
        SELECT source_id::text,trixel, ra, dec, pmra, pmdec, mag
        FROM stars
        WHERE sphere_point <@ scircle(spoint(radians($1), radians($2)), radians($3))
        AND mag <= $4
        LIMIT 5000
        """
        return StreamingResponse(fetch_many(conn, query, Star, params["ra"], params["dec"], params["radius"], params["mag"]), media_type="application/json")

@app.get("/spectras", response_model=List[Spectra])
async def spectra_search(params: dict = Depends(validate_parameters)):
    conn = await get_db_connection()
    query = ""
    if "trixel" in params:
        query = """
        SELECT source_id::text,trixel, ra, dec, pmra, pmdec, mag, flux
        FROM stars
        WHERE trixel = $1
        AND mag <= $2
        LIMIT 5000
        """
        return StreamingResponse(fetch_many(conn, query, Spectra, params["trixel"], params["mag"]), media_type="application/json")
    else:
        query = """
        SELECT source_id::text,trixel, ra, dec, pmra, pmdec, mag, flux
        FROM stars
        WHERE sphere_point <@ scircle(spoint(radians($1), radians($2)), radians($3))
        AND mag <= $4
        LIMIT 5000
        """
        return StreamingResponse(fetch_many(conn, query, Spectra, params["ra"], params["dec"], params["radius"], params["mag"]), media_type="application/json")

