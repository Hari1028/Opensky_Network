import sqlite3
import pandas as pd
import logging

# --- Configuration ---
DB_NAME = "opensky.db"
OUTPUT_CSV_FILE = "insight_1_high_density_zones.csv"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_high_density_zones():
    """
    Runs the query for high-density airspace zones and saves the result to a CSV file.
    """
    # The same query we designed for Insight 1
    query = """
    SELECT
        CAST(latitude AS INT) AS lat_grid,
        CAST(longitude AS INT) AS lon_grid,
        COUNT(DISTINCT icao24) AS aircraft_count
    FROM
        opensky_data
    WHERE
        on_ground = 0
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    GROUP BY
        lat_grid,
        lon_grid
    HAVING
        aircraft_count > 2 -- Optional filter to keep the data clean
    ORDER BY
        aircraft_count DESC;
    """

    conn = None
    try:
        logging.info(f"Connecting to database '{DB_NAME}'...")
        conn = sqlite3.connect(DB_NAME)

        logging.info("Executing query to find high-density zones...")
        # Use pandas to run the query and load data directly into a DataFrame
        df = pd.read_sql_query(query, conn)

        logging.info(f"Saving {len(df)} records to '{OUTPUT_CSV_FILE}'...")
        # Save the DataFrame to a CSV file
        df.to_csv(OUTPUT_CSV_FILE, index=False)

        logging.info("Successfully exported the data to CSV.")

    except sqlite3.Error as e:
        logging.error(f"A database error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

# --- Run the Export ---
if __name__ == "__main__":
    export_high_density_zones()