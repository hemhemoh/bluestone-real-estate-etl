import os
import csv
import json
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("RENT_CAST_API_KEY")

BASE_URL   = "https://api.rentcast.io/v1/"
OUTPUT_DIR = "./data/raw_data"

params_list = [
    {"city": "Austin",       "state": "TX", "limit": 4980},
    {"city": "Houston",      "state": "TX", "limit": 2500},
    {"city": "Phoenix",      "state": "AZ", "limit": 5000},
    {"city": "Denver",       "state": "CO", "limit": 4500},
    {"city": "Atlanta",      "state": "GA", "limit": 6000},
    {"city": "Chicago",      "state": "IL", "limit": 6780},
    {"city": "Charlotte",    "state": "NC", "limit": 1000},
]

def fetch_listings(param: dict, endpoint: str) -> list:
    url     = BASE_URL + endpoint
    headers = {"X-Api-Key": API_KEY}
    
    all_results = []
    offset      = 0
    limit       = 500 

    target = param.get("limit", 500)  

    while len(all_results) < target:
        paginated_param = {**param, "limit": limit, "offset": offset}
        response = requests.get(url, headers=headers, params=paginated_param, timeout=10)
        response.raise_for_status()

        batch = response.json()
        batch = batch if isinstance(batch, list) else batch.get("data", [])

        if not batch:
            break  

        all_results.extend(batch)
        offset += limit

        if len(batch) < limit:
            break  # last page was partial, nothing more to fetch

    return all_results[:target]  # trim to exactly what was requested

def fetch_market_data(zip_codes: list[str], endpoint: str = "markets") -> dict:
    """
    Fetch market data once per unique zip code.
    Returns a dict keyed by zip code: { "78701": {...}, "78702": {...} }
    """
    url     = BASE_URL + endpoint
    headers = {"X-Api-Key": API_KEY}
    results = {}

    for zip_code in zip_codes:
        params   = {"zipCode": zip_code}
        response = requests.get(url, headers=headers, params=params, timeout=10)

        if response.status_code == 200:
            results[zip_code] = response.json()
        else:
            results[zip_code] = {
                "error":   response.status_code,
                "message": response.text,
            }

    return results

def fetch_all(params_list: list[dict]) -> tuple[list, list]:
    all_rental = []
    all_sale   = []

    for param in params_list:
        city  = param.get("city")
        state = param.get("state")
        print(f"Fetching listings for {city}, {state}...")

        try:
            rentals = fetch_listings(param, "listings/rental/long-term")
            sales   = fetch_listings(param, "listings/sale")

            all_rental.extend(rentals)
            all_sale.extend(sales)

            print(f"  → {len(rentals)} rentals, {len(sales)} sales")

        except Exception as e:
            print(f"  ✗ Error fetching {city}, {state}: {e}")

    return all_rental, all_sale

def fetch_and_join(all_rental: list, all_sale: list) -> list:
    """
    1. Collect unique zip codes from both listing sets.
    2. Fetch market data once per zip.
    3. Attach market data to every listing and tag listing_type.
    Returns a single flat list of enriched records.
    """
    all_listings = (
        [{"listing_type": "rental", **l} for l in all_rental] +
        [{"listing_type": "sale",   **l} for l in all_sale]
    )

    # Deduplicate zip codes
    unique_zips = set(
        l["zipCode"] for l in all_listings if l.get("zipCode")
    )
    print(f"\nFetching market data for {len(unique_zips)} unique zip codes...")
    market_data_by_zip = fetch_market_data(list(unique_zips))

    # Join
    enriched = []
    for listing in all_listings:
        zip_code = listing.get("zipCode")
        enriched.append({
            "listing": listing,
            "market":  market_data_by_zip.get(zip_code, {}),
        })

    return enriched

def flatten_record(enriched_record: dict) -> dict:
    """
    Flatten an enriched listing (listing + market) into a single
    dict suitable for a CSV row. Nested dicts are serialised to JSON strings.
    """
    listing = enriched_record["listing"]
    market  = enriched_record["market"]

    row = {}

    # --- Listing fields (direct) ---
    for key, value in listing.items():
        if isinstance(value, (dict, list)):
            row[f"listing_{key}"] = json.dumps(value)
        else:
            row[f"listing_{key}"] = value

    # listing_type is already on the listing dict from fetch_and_join
    row["listing_type"] = listing.get("listing_type", "")

    # --- Market fields (flattened one level) ---
    sale_data   = market.get("saleData",   {}) if isinstance(market.get("saleData"),   dict) else {}
    rental_data = market.get("rentalData", {}) if isinstance(market.get("rentalData"), dict) else {}

    row["market_avgSalePrice"]      = sale_data.get("averagePrice")
    row["market_avgDaysOnMarket"]   = sale_data.get("averageDaysOnMarket")
    row["market_avgRent"]           = rental_data.get("averagePrice")

    return row


def save_csv(enriched_records: list, filepath: str):
    """
    Save all enriched records to a single CSV.
    Each row includes a listing_type column ('rental' or 'sale').
    """
    if not enriched_records:
        print("No records to save.")
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    rows = [flatten_record(r) for r in enriched_records]

    # Union of all keys across all rows (different endpoints return different fields)
    all_keys = []
    seen     = set()
    for row in rows:
        for key in row:
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    # Make sure listing_type is the first column
    if "listing_type" in all_keys:
        all_keys.remove("listing_type")
    all_keys = ["listing_type"] + all_keys

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in all_keys})

    print(f"\nSaved {len(rows)} rows to {filepath}")

if __name__ == "__main__":
    # 1. Fetch listings across all city/state params
    all_rental, all_sale = fetch_all(params_list)

    # 2. Fetch market data and join
    enriched_listings = fetch_and_join(all_rental, all_sale)

    # 4. Save final combined CSV — one row per listing, with listing_type column
    save_csv(enriched_listings, f"{OUTPUT_DIR}/rentcast_properties.csv")