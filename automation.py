import asyncio
import aiohttp
import json
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta
import time
import os
import sys

# --- Configuration ---
# Your database connection string from the environment variable
DB_CONN_STRING = os.environ.get("DATABASE_URL")

# --- Assumed configuration based on your use case ---
DAYS = 30  # Number of days to scrape data for
CONCURRENCY = 5  # Number of concurrent requests

try:
    # This script now reads from the 'routes_id.json' file directly
    routes_data = json.load(open("routes_id.json", encoding="utf-8"))
except FileNotFoundError:
    print("Error: routes_id.json not found. Please create this file.")
    sys.exit(1)

# Assumed headers for making requests to the Bookaway API
BROWSER_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "origin": "https://www.bookaway.com",
    "referer": "https://www.bookaway.com/",
}

# --- Helper Functions ---
def parse_duration_minutes(v):
    """
    Parses a duration string (e.g., '1h 30m') into total minutes.
    """
    if not v:
        return None
    s = str(v).lower().strip()
    total = 0
    h_match = s.find('h')
    m_match = s.find('m')
    if h_match != -1:
        try:
            total += int(s[:h_match].strip()) * 60
        except ValueError:
            pass
    if m_match != -1:
        try:
            start_pos = h_match + 1 if h_match != -1 else 0
            total += int(s[start_pos:m_match].strip())
        except ValueError:
            pass
    if total > 0:
        return total
    
    hm_match = s.split(':')
    if len(hm_match) == 2:
        try:
            return int(hm_match[0]) * 60 + int(hm_match[1])
        except ValueError:
            pass

    try:
        num = int(s)
        return num
    except ValueError:
        pass
    return None

def parse_iso_datetime(dt_str):
    """Parses an ISO 8601 string into a datetime object."""
    try:
        if dt_str:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None
    return None

# --- Main Scraper & Data Processor ---
async def fetch_route(session: aiohttp.ClientSession, route, date_str, sem, results):
    """
    Fetches data for a single route and date from Bookaway's API.
    """
    async with sem:
        from_city = route["from_title"]
        to_city = route["to_title"]
        from_slug = route["from_slug"]
        to_slug = route["to_slug"]
        
        # Bookaway's API is a POST request with a JSON payload
        api_url = "https://www.bookaway.com/api/v1/search"
        payload = {
            "from": {"slug": from_slug, "type": "city"},
            "to": {"slug": to_slug, "type": "city"},
            "date": date_str,
            "direction": "one-way",
            "people": {"adults": 1, "children": 0, "infants": 0, "seniors": 0},
            "travelOptions": ["bus", "train", "ferry", "minivan", "taxi"]
        }

        try:
            async with session.post(api_url, headers=BROWSER_HEADERS, json=payload) as resp:
                if resp.status != 200:
                    print(f"Failed for {from_city} -> {to_city} on {date_str} with status {resp.status}")
                    return
                data = await resp.json()
        except Exception as e:
            print(f"Error fetching {api_url}: {e}")
            return

        for trip in data.get("results", []):
            try:
                # Extracting data from the Bookaway API response structure
                price_data = trip.get("price", {})
                price = price_data.get("value")
                currency = price_data.get("currencyCode")
                
                # We need a separate API for currency conversion, but for now we'll just store the original
                price_inr = None

                dep_dt = parse_iso_datetime(trip.get("departureDate"))
                arr_dt = parse_iso_datetime(trip.get("arrivalDate"))
                
                duration_str = trip.get("duration")
                operator_name = trip.get("operator", {}).get("name")
                transport_type = trip.get("transportType")

                if not price or price <= 0:
                    continue

                results.append({
                    "route_url": f"https://www.bookaway.com/en/travel/{from_slug}/to/{to_slug}/on/{date_str}",
                    "origin": from_city,
                    "destination": to_city,
                    "travel_date": date_str,
                    "departure_time": dep_dt,
                    "arrival_time": arr_dt,
                    "transport_type": transport_type,
                    "duration_min": parse_duration_minutes(duration_str),
                    "price": price,
                    "currency": currency,
                    "price_inr": price_inr,
                    "operator_name": operator_name,
                    "provider": "bookaway",
                })
            except Exception as e:
                print(f"Error parsing trip data: {e}")
                continue

def get_db_connection():
    """Establishes and returns a database connection."""
    if not DB_CONN_STRING:
        print("Error: DATABASE_URL environment variable is not set.")
        return None
    try:
        return psycopg2.connect(DB_CONN_STRING)
    except psycopg2.DatabaseError as e:
        print(f"Error connecting to the database: {e}")
        return None

async def main():
    start_time = time.time()
    print("Scraper started...")
    
    results = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for route in routes_data:
            for day_offset in range(DAYS):
                date_str = (datetime.now() + timedelta(days=day_offset)).strftime('%Y-%m-%d')
                tasks.append(fetch_route(session, route, date_str, sem, results))
        
        print(f"Fetching data for {len(tasks)} routes over {DAYS} days...")
        await asyncio.gather(*tasks)

    # --- Database Insertion Logic ---
    if not results:
        print("❌ ERROR: The list of scraped records is empty. No data to insert.")
        return

    conn = None
    try:
        print("Connecting to the database...")
        conn = get_db_connection()
        if not conn:
            return
            
        cur = conn.cursor()

        print("Truncating the bookaway_trips table to delete data and reset the ID sequence...")
        cur.execute("TRUNCATE TABLE bookaway_trips RESTART IDENTITY;")
        conn.commit()
        print("Table successfully truncated and ID sequence reset. ✅")
        
        records_to_insert = [
            (
                r["route_url"],
                r["origin"],
                r["destination"],
                r["departure_time"],
                r["arrival_time"],
                r["transport_type"],
                r["duration_min"] or 0,
                r["price"] or 0,
                r["price_inr"] or 0,
                r["currency"],
                r["travel_date"],
                r["operator_name"],
                r["provider"]
            )
            for r in results if r["price"] is not None and r["price"] > 0
        ]
        
        print(f"Importing {len(records_to_insert)} new records...")
        
        chunk_size = 400
        for i in range(0, len(records_to_insert), chunk_size):
            chunk = records_to_insert[i:i + chunk_size]
            extras.execute_values(cur, """
            INSERT INTO bookaway_trips (
                route_url, origin, destination,
                departure_time, arrival_time, transport_type,
                duration_min, price, price_inr, currency,
                travel_date, operator_name, provider
            ) VALUES %s
            """, chunk, page_size=chunk_size)
            conn.commit()
            print(f"Processed {i + len(chunk)} records...")
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"✅ Import completed! Imported {len(records_to_insert)} records in {duration:.2f} seconds.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error during database operation: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed. 👋")

if __name__ == "__main__":
    asyncio.run(main())