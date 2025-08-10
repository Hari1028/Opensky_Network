import sqlite3
import pandas as pd
import logging

# --- Configuration ---
DB_NAME = "opensky.db"
OUTPUT_CSV_FILE = "final_insight_hourly_rhythm.csv"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_hourly_rhythm():
    """
    Analyzes flight activity on an hourly basis to reveal the daily rhythm of aviation.
    """
    # This query uses strftime to extract the hour from the Unix timestamp.
    # It will work perfectly on your 17 hours of data.
    query = """
    SELECT
        strftime('%H', last_contact, 'unixepoch') AS hour_of_day,
        COUNT(*) AS flight_activity_count
    FROM
        opensky_data
    WHERE
        on_ground = 0
    GROUP BY
        hour_of_day
    ORDER BY
        hour_of_day;
    """

    conn = None
    try:
        logging.info(f"Connecting to database '{DB_NAME}'...")
        conn = sqlite3.connect(DB_NAME)

        logging.info("Executing query to analyze the hourly rhythm of the skies...")
        df = pd.read_sql_query(query, conn)
        
        # Add a formatted hour column for better sorting and display in Power BI
        df['hour_formatted'] = df['hour_of_day'].astype(int).apply(lambda x: f"{x:02d}:00")

        logging.info(f"Saving {len(df)} records to '{OUTPUT_CSV_FILE}'...")
        df.to_csv(OUTPUT_CSV_FILE, index=False)

        logging.info("Successfully exported hourly rhythm data to CSV.")

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
    export_hourly_rhythm()