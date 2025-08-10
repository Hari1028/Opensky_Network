import sqlite3
import logging

# Configure logging to provide clear output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_database(db_name="opensky.db"):
    """
    Sets up the SQLite database, creating the main data table and performance indexes.
    This function is idempotent, meaning it can be run multiple times without causing errors.
    """
    
    # --- SQL Definitions ---

    # Main table to store flight data.
    # PRIMARY KEY changed to (icao24, last_contact) for better reliability, as time_position can be null.
    create_table_query = """
    CREATE TABLE IF NOT EXISTS opensky_data (
        icao24 TEXT NOT NULL,
        callsign TEXT,
        origin_country TEXT NOT NULL,
        time_position INTEGER,
        last_contact INTEGER NOT NULL,
        longitude REAL CHECK(longitude BETWEEN -180 AND 180),
        latitude REAL CHECK(latitude BETWEEN -90 AND 90),
        baro_altitude REAL,
        on_ground BOOLEAN NOT NULL CHECK(on_ground IN (0, 1)),
        velocity REAL,
        true_track REAL CHECK(true_track BETWEEN 0 AND 360),
        vertical_rate REAL,
        sensors TEXT,
        geo_altitude REAL,
        squawk TEXT,
        spi BOOLEAN NOT NULL CHECK(spi IN (0, 1)),
        position_source INTEGER CHECK(position_source IN (0, 1, 2)),
        fetch_time TEXT NOT NULL,
        PRIMARY KEY (icao24, last_contact)
    );
    """

    # Index on origin_country to dramatically speed up GROUP BY queries for insights.
    create_country_index_query = """
    CREATE INDEX IF NOT EXISTS idx_origin_country 
    ON opensky_data (origin_country);
    """

    # Index on time to dramatically speed up time-based queries and analysis.
    create_time_index_query = """
    CREATE INDEX IF NOT EXISTS idx_last_contact 
    ON opensky_data (last_contact);
    """

    # --- Database Connection and Execution ---
    conn = None  # Initialize conn to None
    try:
        logging.info(f"Connecting to database '{db_name}'...")
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        logging.info("Executing CREATE TABLE IF NOT EXISTS...")
        cursor.execute(create_table_query)

        logging.info("Executing CREATE INDEX IF NOT EXISTS for origin_country...")
        cursor.execute(create_country_index_query)

        logging.info("Executing CREATE INDEX IF NOT EXISTS for last_contact...")
        cursor.execute(create_time_index_query)

        conn.commit()
        logging.info("Database setup successful. Table and indexes are ready.")

    except sqlite3.Error as e:
        logging.error(f"An error occurred during database setup: {e}")

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

# This block allows the script to be run directly to set up the database
if __name__ == "__main__":
    setup_database()