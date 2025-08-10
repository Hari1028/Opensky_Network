import sqlite3
import pandas as pd
import logging

# --- Configuration ---
DB_NAME = "opensky_1.db"
OUTPUT_CSV_FILE = "insight_2_busiest_airlines.csv"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_busiest_airlines():
    """
    Identifies the busiest airlines by their callsign prefix and exports the result to a CSV.
    """
    # This query extracts the 3-letter airline code from the callsign
    # and counts the number of position updates for each.
    query = """
    SELECT
        -- Extract the first 3 letters of the callsign as the airline code
        SUBSTR(UPPER(callsign), 1, 3) AS airline_code,
        -- Count the number of updates as a measure of activity
        COUNT(*) AS position_updates
    FROM
        opensky_data
    WHERE
        -- Ensure callsign is not null and is a typical length
        callsign IS NOT NULL AND LENGTH(callsign) > 3 AND on_ground = 0
    GROUP BY
        airline_code
    ORDER BY
        position_updates DESC
    LIMIT 20; -- Get the top 20 busiest airlines
    """

    conn = None
    try:
        logging.info(f"Connecting to database '{DB_NAME}'...")
        conn = sqlite3.connect(DB_NAME)

        logging.info("Executing query to find the busiest airlines...")
        df = pd.read_sql_query(query, conn)

        logging.info(f"Saving {len(df)} records to '{OUTPUT_CSV_FILE}'...")
        df.to_csv(OUTPUT_CSV_FILE, index=False)

        logging.info("Successfully exported busiest airline data to CSV.")

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
    export_busiest_airlines()