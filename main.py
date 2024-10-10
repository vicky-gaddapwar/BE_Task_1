import csv
import os
import uuid
import asyncio
import aiohttp
from fastapi import FastAPI, UploadFile, BackgroundTasks
from pydantic import BaseModel
from PIL import Image
from io import BytesIO
from databases import Database
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select

app = FastAPI()

DATABASE_URL = "postgresql+asyncpg://user:password@localhost/image_processing"
database = Database(DATABASE_URL)
metadata = MetaData()

# Database schema setup
requests_table = Table(
    'requests', metadata,
    Column('id', String, primary_key=True),
    Column('status', String),
    Column('input_urls', String),
    Column('output_urls', String)
)

engine = create_engine(DATABASE_URL.replace("asyncpg", "psycopg2"))
metadata.create_all(engine)

# Helper function for async image processing
async def download_and_compress_image(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            image_data = await response.read()
            image = Image.open(BytesIO(image_data))
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=50)
            return buffer.getvalue()

# Process CSV and compress images
async def process_images(request_id: str, file_path: str):
    output_urls = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            product_name = row[1]
            input_urls = row[2].split(',')
            output_images = []

            for url in input_urls:
                compressed_image = await download_and_compress_image(url)
                # Save the image (in real case, this should be saved to S3 or similar storage)
                output_image_path = f"output/{uuid.uuid4()}.jpg"
                with open(output_image_path, "wb") as img_file:
                    img_file.write(compressed_image)
                output_images.append(output_image_path)

            output_urls.append(",".join(output_images))

    # Update database with the result
    query = requests_table.update().where(requests_table.c.id == request_id).values(
        output_urls=",".join(output_urls), status="Completed"
    )
    await database.execute(query)

# API to upload CSV
@app.post("/upload")
async def upload_csv(file: UploadFile, background_tasks: BackgroundTasks):
    request_id = str(uuid.uuid4())
    
    # Save the uploaded file
    file_location = f"uploads/{file.filename}"
    with open(file_location, "wb") as f:
        f.write(await file.read())

    # Insert into database
    query = requests_table.insert().values(id=request_id, status="Processing", input_urls=file_location, output_urls="")
    await database.execute(query)

    # Process the file in the background
    background_tasks.add_task(process_images, request_id, file_location)

    return {"request_id": request_id}

# API to check status
@app.get("/status/{request_id}")
async def check_status(request_id: str):
    query = select([requests_table]).where(requests_table.c.id == request_id)
    result = await database.fetch_one(query)
    
    if result:
        return {"request_id": request_id, "status": result["status"], "output_urls": result["output_urls"]}
    return {"error": "Request not found"}

# Webhook (bonus)
@app.post("/webhook")
async def webhook_callback(request_id: str):
    # Process the webhook callback
    return {"status": "Webhook received"}

# Connect to the database on startup
@app.on_event("startup")
async def startup():
    await database.connect()

# Disconnect from the database on shutdown
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
