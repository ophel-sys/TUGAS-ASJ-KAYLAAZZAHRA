from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

import os
import shutil
import psycopg2
from psycopg2.extras import RealDictCursor

from minio import Minio
from minio.error import S3Error

app = FastAPI()

templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_FOLDER = "uploads"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "minidrive")
DB_USER = os.getenv("POSTGRES_USER", "minio")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "minio123")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )


def create_users_table():
    conn = get_db_connection()
    cur = conn.cursor() 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            photo_filename VARCHAR(255) NOT NULL
        )
    """)

    conn.commit()
    cur.close()
    conn.close()


create_users_table()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "uploads")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
      secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, email, photo_filename FROM users ORDER BY id ASC"
    )

    users = cur.fetchall()

    cur.close()
    conn.close()

    return templates.TemplateResponse(
              "index.html",
        {
            "request": request,
            "users": users
        }
    )

@app.post("/users")
async def create_user(
    name: str = Form(...),
    email: str = Form(...),
    photo: UploadFile = File(...)
):

    local_path = f"{UPLOAD_FOLDER}/{photo.filename}"

    with open(local_path, "wb") as buffer:
        shutil.copyfileobj(photo.file, buffer)

    try:
        minio_client.fput_object(
            MINIO_BUCKET,
            photo.filename,
            local_path
                 )

    except S3Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal upload ke MinIO: {e}"
        )

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO users (name, email, photo_filename)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (name, email, photo.filename)
    )

    user_id = cur.fetchone()["id"]

    conn.commit() 
      cur.close()
    conn.close()

    return {
        "id": user_id,
        "name": name,
        "email": email,
        "photo": photo.filename
    }

@app.get("/users")
def read_users():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, email, photo_filename FROM users ORDER BY id ASC"
    )

    rows = cur.fetchall()

    cur.close()
    conn.close() 
    result = []

    for r in rows:
        result.append({
            "id": r["id"],
            "name": r["name"],
            "email": r["email"],
            "photo": r["photo_filename"]
        })

    return result

@app.get("/download/{filename}")
async def download_file(filename: str):

    temp_path = f"{UPLOAD_FOLDER}/{filename}"

    try:
        minio_client.fget_object(
            MINIO_BUCKET,
            filename,
            temp_path
        ) 
      
    except S3Error:
        raise HTTPException(
            status_code=404,
            detail="File tidak ditemukan"
        )

    return FileResponse(
        temp_path,
        media_type="application/octet-stream",
        filename=filename
    )

  
