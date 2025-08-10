import requests
import sqlite3
import logging
import time
from datetime import datetime, timedelta

# --- Configuration ---
API_URL = "https://opensky-network.org/api/states/all"
DB_NAME = "opensky.db"
FETCH_INTERVAL_SECONDS = 600  # 10 minutes
RUN_DURATION_HOURS = 24
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2  # Exponential backoff base

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Robust API fetch with retry logic ---
def fetch_data_with_retries():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            
            response = requests.get(API_URL, timeout=15, verify=False)

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            wait_time = RETRY_BACKOFF_FACTOR ** attempt
            logging.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    logging.error("Max retries reached. Skipping this fetch cycle.")
    return None

# --- Main fetch and upsert function ---
def fetch_and_upsert_data():
    conn = None
    try:
        data = fetch_data_with_retries()
        if not data:
            return
        states = data.get("states", [])
        if not states:
            logging.warning("No flight data received.")
            return

        fetch_time = datetime.utcnow().isoformat()
        records_to_insert = []
        skipped = 0

        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        for state in states:
            if len(state) >= 17:
                try:
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

                except Exception as e:
                    logging.debug(f"Record skipped due to error: {e}")
                    skipped += 1
                    continue

        if records_to_insert:
            upsert_query = """
                INSERT INTO opensky_data (
                    icao24, callsign, origin_country, time_position, last_contact,
                    longitude, latitude, baro_altitude, on_ground, velocity,
                    true_track, vertical_rate, sensors, geo_altitude, squawk,
                    spi, position_source, fetch_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(icao24, last_contact) DO UPDATE SET
                    callsign=excluded.callsign,
                    origin_country=excluded.origin_country,
                    time_position=excluded.time_position,
                    longitude=excluded.longitude,
                    latitude=excluded.latitude,
                    baro_altitude=excluded.baro_altitude,
                    on_ground=excluded.on_ground,
                    velocity=excluded.velocity,
                    true_track=excluded.true_track,
                    vertical_rate=excluded.vertical_rate,
                    sensors=excluded.sensors,
                    geo_altitude=excluded.geo_altitude,
                    squawk=excluded.squawk,
                    spi=excluded.spi,
                    position_source=excluded.position_source,
                    fetch_time=excluded.fetch_time;
            """
            cursor.executemany(upsert_query, records_to_insert)
            conn.commit()
            logging.info(f"Upserted {len(records_to_insert)} records. Skipped {skipped} invalid records.")
        else:
            logging.info("No valid records to upsert.")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# --- Main loop (runs for 24 hours) ---
if __name__ == "__main__":
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=RUN_DURATION_HOURS)

    while datetime.now() < end_time:
        fetch_and_upsert_data()
        logging.info(f"Sleeping for {FETCH_INTERVAL_SECONDS} seconds...")
        time.sleep(FETCH_INTERVAL_SECONDS)

    logging.info("Script finished running for 24 hours.")
