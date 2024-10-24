import asyncio
import asyncpg
from dotenv import load_dotenv
import os

load_dotenv()

async def create_database():
    # Connect to 'postgres' database to create a new database
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database='postgres'
    )

    # Check if the database exists
    exists = await conn.fetchval(
        "SELECT 1 FROM pg_database WHERE datname = $1",
        os.getenv('DB_NAME')
    )

    if not exists:
        # Create the database
        await conn.execute(f"CREATE DATABASE {os.getenv('DB_NAME')}")
        print(f"Database {os.getenv('DB_NAME')} created successfully.")
    else:
        print(f"Database {os.getenv('DB_NAME')} already exists.")

    await conn.close()

asyncio.get_event_loop().run_until_complete(create_database())
