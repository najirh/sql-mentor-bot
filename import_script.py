import csv
import sys
import psycopg2
from psycopg2 import sql

def import_csv(file_path):
    # Database connection parameters
    # You may want to load these from environment variables
    conn_params = {
        'dbname': 'your_database_name',
        'user': 'your_username',
        'password': 'your_password',
        'host': 'db',  # This should match the service name in docker-compose.yml
        'port': '5432'
    }

    try:
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Open and read the CSV file
        with open(file_path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            headers = next(csvreader)  # Assuming the first row is headers
            
            # Create a table if it doesn't exist
            table_name = 'imported_data'
            create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(sql.Identifier(h) + sql.SQL(' TEXT') for h in headers)
            )
            cur.execute(create_table_query)

            # Insert data
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, headers)),
                sql.SQL(', ').join(sql.Placeholder() * len(headers))
            )
            
            for row in csvreader:
                cur.execute(insert_query, row)

        # Commit the transaction
        conn.commit()
        print(f"Data from {file_path} has been successfully imported.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or importing data: {error}")

    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_script.py <path_to_csv_file>")
    else:
        csv_file_path = sys.argv[1]
        import_csv(csv_file_path)