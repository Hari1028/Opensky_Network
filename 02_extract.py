import requests
import sqlite3
import logging
import time
from datetime import datetime, timedelta

# --- Configuration ---
API_URL = "https://opensky-network.org/api/states/all"
DB_NAME = "opensky.db"
FETCH_INTERVAL_SECONDS = 600  # 10 minutes
RUN_DURATION_HOURS = 6

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Main fetch and insert function ---
def fetch_and_append_data():
    conn = None
    try:
        # Connect to database
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        logging.info("Fetching data from OpenSky API...")
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
        states = data.get("states", [])

        if not states:
            logging.warning("No flight data received.")
            return

        fetch_time = datetime.utcnow().isoformat()
        records_to_insert = []
        skipped = 0

        for state in states:
            if len(state) >= 17:
                try:
                    # Extract fields
                    icao24 = state[0]
                    callsign = state[1].strip().upper() if state[1] else None
                    origin_country = state[2]
                    time_position = state[3]
                    last_contact = state[4]
                    longitude = state[5]
                    latitude = state[6]
                    baro_altitude = state[7]
                    on_ground = state[8]
                    velocity = state[9]
                    true_track = state[10]
                    vertical_rate = state[11]
                    sensors = state[12]
                    geo_altitude = state[13]
                    squawk = state[14]
                    spi = state[15]
                    position_source = state[16]

                    # Validate fields
                    if not icao24 or not isinstance(last_contact, int):
                        skipped += 1
                        continue

                    if longitude is not None and not (-180 <= longitude <= 180):
                        skipped += 1
                        continue

                    if latitude is not None and not (-90 <= latitude <= 90):
                        skipped += 1
                        continue

                    if true_track is not None and not (0 <= true_track <= 360):
                        skipped += 1
                        continue

                    if on_ground not in (0, 1) or spi not in (0, 1):
                        skipped += 1
                        continue

                    if squawk is not None:
                        squawk = squawk.strip()
                        if len(squawk) != 4:
                            squawk = None

                    record = (
                        icao24, callsign, origin_country, time_position, last_contact,
                        longitude, latitude, baro_altitude, on_ground, velocity,
                        true_track, vertical_rate, sensors, geo_altitude, squawk,
                        spi, position_source, fetch_time
                    )
                    records_to_insert.append(record)

                except Exception:
                    skipped += 1
                    continue

        # Insert valid records
        if records_to_insert:
            insert_query = """
                INSERT OR IGNORE INTO opensky_data (
                    icao24, callsign, origin_country, time_position, last_contact,
                    longitude, latitude, baro_altitude, on_ground, velocity,
                    true_track, vertical_rate, sensors, geo_altitude, squawk,
                    spi, position_source, fetch_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
            logging.info(f"Inserted {len(records_to_insert)} records. Skipped {skipped} invalid records.")
        else:
            logging.info("No valid records to insert.")

    except requests.exceptions.RequestException as e:
        logging.error(f"API error: {e}")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# --- Main loop (runs for 6 hours) ---
if __name__ == "__main__":
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=RUN_DURATION_HOURS)
    cycle = 1

    logging.info(f"Starting OpenSky ETL job for 6 hours until {end_time.strftime('%H:%M:%S')}...")

    while datetime.now() < end_time:
        logging.info(f"Cycle {cycle} starting...")
        cycle_start = time.monotonic()

        fetch_and_append_data()

        cycle_end = time.monotonic()
        duration = cycle_end - cycle_start
        sleep_time = max(0, FETCH_INTERVAL_SECONDS - duration)

        logging.info(f"Cycle {cycle} finished. Sleeping for {sleep_time:.1f} seconds...\n")
        time.sleep(sleep_time)
        cycle += 1

    logging.info("ETL session complete. 6-hour window ended.")
