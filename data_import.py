import csv
import os
import asyncpg
from dotenv import load_dotenv
import asyncio
import sys
import logging
from asyncpg.exceptions import PostgresError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Database connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

async def connect_to_db():
    try:
        conn = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logging.info(f"Successfully connected to database: {DB_NAME}")
        return conn
    except PostgresError as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise

async def import_questions(csv_path):
    try:
        conn = await connect_to_db()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    try:
        with open(csv_path, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                try:
                    await conn.execute('''
                        INSERT INTO questions 
                        (question, answer, datasets, difficulty, hint)
                        VALUES ($1, $2, $3, $4, $5)
                    ''', row['question'], row['answer'], row['datasets'], row['difficulty'], row['hint'])
                except PostgresError as e:
                    logging.error(f"Error inserting row: {row}. Error: {e}")
                    continue  # Skip this row and continue with the next
        
        logging.info("Data import completed successfully!")
    except csv.Error as e:
        logging.error(f"CSV error: {e}")
    except IOError as e:
        logging.error(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        logging.error(f"Unexpected error during data import: {e}")
    finally:
        if conn:
            await conn.close()
            logging.info("Database connection closed.")

async def main(csv_path):
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"The specified file does not exist: {csv_path}")
        
        await import_questions(csv_path)
    except FileNotFoundError as e:
        logging.error(e)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_import.py <path_to_csv_file>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    logging.info(f"Using CSV file: {csv_path}")
    asyncio.run(main(csv_path))
