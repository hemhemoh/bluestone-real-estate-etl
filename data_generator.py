"""
Synthetic Inquiry & Transaction Generator
Based on real data from Rentcast Listings API + Market API

Reads enriched listing data from rentcast_properties.csv (output of rentcast_fetch.py)
and saves inquiries, sale transactions, and rental transactions to separate CSVs.
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

OUTPUT_DIR = "./data"

# ---------------------------------------------------------------------------
# CONSTANTS & LOOKUP TABLES
# ---------------------------------------------------------------------------

INQUIRY_CHANNELS = ["Zillow", "Realtor.com", "Direct Website", "Agent Referral", "Walk-in", "Phone Call"]
RENTAL_CHANNELS  = ["Zillow Rentals", "Apartments.com", "Facebook Marketplace", "Direct Website", "Agent Referral"]

FINANCING_TYPES  = ["Conventional", "FHA", "VA", "Cash", "USDA"]
FINANCING_WEIGHTS = [0.45, 0.25, 0.15, 0.12, 0.03]

LISTING_TYPE_URGENCY = {
    "Foreclosure":        1.5,   # more urgency / more inquiries
    "Short Sale":         1.3,
    "Standard":           1.0,
    "New Construction":   0.8,   # buyers are more deliberate
}

PROPERTY_TYPE_PROFILE = {
    # (avg_inquiries_base, conversion_rate, lease_term_months_range)
    "Single Family":  (6,  0.20, (12, 24)),
    "Condo":          (8,  0.18, (12, 18)),
    "Townhouse":      (7,  0.19, (12, 18)),
    "Multi-Family":   (10, 0.22, (12, 24)),
    "Apartment":      (12, 0.25, (6,  12)),
    "Mobile Home":    (4,  0.15, (6,  12)),
    "Land":           (3,  0.10, (12, 12)),
}
DEFAULT_PROPERTY_PROFILE = (5, 0.18, (12, 18))


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_market_stats_for_property(market: dict, property_type: str, listing_type: str) -> dict:
    """
    Extract the most relevant market stats for a listing.
    Prefers property-type-specific stats from dataByPropertyType if available,
    falls back to top-level saleData or rentalData averages.
    """
    sale_data   = market.get("saleData", {})
    rental_data = market.get("rentalData", {})

    # Try to get property-type-specific stats
    by_type = sale_data.get("dataByPropertyType", [])
    matched = next((d for d in by_type if d.get("propertyType") == property_type), None)

    if matched:
        avg_price  = matched.get("averagePrice") or sale_data.get("averagePrice", 0)
        avg_dom    = matched.get("averageDaysOnMarket") or sale_data.get("averageDaysOnMarket", 30)
    else:
        avg_price  = sale_data.get("averagePrice", 0)
        avg_dom    = sale_data.get("averageDaysOnMarket", 30)

    avg_rent = rental_data.get("averagePrice", 0)

    return {
        "avgPrice":  avg_price,
        "avgDom":    avg_dom or 30,
        "avgRent":   avg_rent,
    }


def compute_inquiry_rate_multiplier(listing: dict, market_stats: dict) -> float:
    """
    Derive how 'hot' this listing is relative to its market.
    Returns a multiplier applied to the base inquiry count.
    """
    price      = listing.get("price", 0)
    avg_price  = market_stats["avgPrice"]
    dom        = listing.get("daysOnMarket", 0) or 0
    avg_dom    = market_stats["avgDom"]
    listing_type = listing.get("listingType", "Standard")

    # Price ratio: underpriced listings attract more interest
    price_ratio = (price / avg_price) if avg_price else 1.0
    price_multiplier = max(0.5, 2.0 - price_ratio)  # e.g. 0.9 ratio → 1.1x

    # Days on market: fresh listings get more inquiries
    dom_ratio = (dom / avg_dom) if avg_dom else 1.0
    dom_multiplier = max(0.3, 1.5 - (dom_ratio * 0.5))

    # Listing type urgency
    type_multiplier = LISTING_TYPE_URGENCY.get(listing_type, 1.0)

    return round(price_ratio * dom_multiplier * type_multiplier, 3)


def random_date_after(start_date_str: str, max_days: int = 90) -> datetime:
    """Return a random datetime between start_date and start_date + max_days."""
    try:
        start = datetime.fromisoformat(start_date_str.replace("Z", ""))
    except Exception:
        start = datetime.now() - timedelta(days=max_days)
    offset = random.randint(0, max_days)
    hour   = random.choices(
        range(8, 21),
        weights=[1, 2, 3, 4, 5, 5, 4, 4, 3, 3, 2, 1, 1],  # peak 11am–3pm
        k=1
    )[0]
    return start + timedelta(days=offset, hours=hour, minutes=random.randint(0, 59))


# ---------------------------------------------------------------------------
# INQUIRY GENERATOR
# ---------------------------------------------------------------------------

def generate_inquiries(enriched_listing: dict) -> list[dict]:
    """
    Generate a realistic list of inquiry records for one enriched listing.
    Works for both sale and rental listings.
    """
    listing      = enriched_listing["listing"]
    market       = enriched_listing["market"]
    listing_id   = listing.get("id", str(uuid.uuid4()))
    listed_date  = listing.get("listedDate", datetime.now().isoformat())
    property_type = listing.get("propertyType", "Single Family")
    is_rental    = "price" not in listing or listing.get("status", "").lower() == "active" and listing.get("price", 0) < 10000

    # Determine listing mode (sale vs rental) by checking for rentPrice field
    is_rental = "rentPrice" in listing

    channels = RENTAL_CHANNELS if is_rental else INQUIRY_CHANNELS

    market_stats   = get_market_stats_for_property(market, property_type, listing.get("listingType", "Standard"))
    rate_multiplier = compute_inquiry_rate_multiplier(listing, market_stats)

    profile = PROPERTY_TYPE_PROFILE.get(property_type, DEFAULT_PROPERTY_PROFILE)
    base_count = profile[0]

    # Final inquiry count: base × market multiplier, with some randomness
    num_inquiries = max(1, int(round(base_count * rate_multiplier * random.uniform(0.7, 1.3))))

    inquiries = []
    for _ in range(num_inquiries):
        inquiry_date = random_date_after(listed_date, max_days=90)
        inquiries.append({
            "inquiryId":       str(uuid.uuid4()),
            "listingId":       listing_id,
            "inquiryDate":     inquiry_date.isoformat(),
            "channel":         random.choice(channels),
            "inquirerName":    fake.name(),
            "inquirerEmail":   fake.email(),
            "inquirerPhone":   fake.phone_number(),
            "message":         fake.sentence(nb_words=random.randint(10, 25)),
            "responseTimeHrs": round(random.uniform(0.5, 48), 1),
            "followUpCount":   random.randint(0, 5),
            "leadScore":       random.randint(1, 10),   # 1=cold, 10=hot
            "converted":       False,  # updated downstream by transaction generator
        })

    return inquiries

#can we use llms to generate message, so it is more realistics?
#are the transactions data from inquiries?
# ---------------------------------------------------------------------------
# TRANSACTION GENERATOR — SALE
# ---------------------------------------------------------------------------

def generate_sale_transaction(enriched_listing: dict, inquiry: dict):
    """
    Given an enriched listing and a converted inquiry, generate a sale transaction.
    Returns None if the listing is not a sale listing.
    """
    listing  = enriched_listing["listing"]
    market   = enriched_listing["market"]

    list_price    = listing.get("price", 0)
    property_type = listing.get("propertyType", "Single Family")
    listing_id    = listing.get("id")
    listed_date   = listing.get("listedDate", datetime.now().isoformat())

    if not list_price:
        return None

    market_stats = get_market_stats_for_property(market, property_type, listing.get("listingType", "Standard"))
    avg_price    = market_stats["avgPrice"]
    avg_dom      = market_stats["avgDom"]

    # Offer price: competitive market → tighter spread, may go over asking
    price_ratio  = (list_price / avg_price) if avg_price else 1.0
    if price_ratio < 0.95:
        # Underpriced — likely to get offers at or above asking
        offer_range = (0.98, 1.06)
    elif price_ratio > 1.05:
        # Overpriced — offers come in below asking
        offer_range = (0.91, 0.97)
    else:
        offer_range = (0.95, 1.02)

    offer_price      = round(list_price * random.uniform(*offer_range), -2)
    negotiated_price = round(offer_price * random.uniform(0.97, 1.0), -2)

    # Closing timeline based on financing type
    financing = random.choices(FINANCING_TYPES, weights=FINANCING_WEIGHTS, k=1)[0]
    close_days = {
        "Cash":         random.randint(7,  21),
        "Conventional": random.randint(28, 45),
        "FHA":          random.randint(35, 50),
        "VA":           random.randint(35, 55),
        "USDA":         random.randint(45, 60),
    }[financing]

    inquiry_date  = datetime.fromisoformat(inquiry["inquiryDate"])
    offer_date    = inquiry_date + timedelta(days=random.randint(3, 14))
    accepted_date = offer_date  + timedelta(days=random.randint(1, 5))
    close_date    = accepted_date + timedelta(days=close_days)

    return {
        "transactionId":    str(uuid.uuid4()),
        "listingId":        listing_id,
        "inquiryId":        inquiry["inquiryId"],
        "transactionType":  "Sale",
        "buyerName":        inquiry["inquirerName"],
        "buyerEmail":       inquiry["inquirerEmail"],
        "buyerPhone":       inquiry["inquirerPhone"],
        "listPrice":        list_price,
        "offerPrice":       offer_price,
        "finalSalePrice":   negotiated_price,
        "offerToListRatio": round(offer_price / list_price, 4),
        "financingType":    financing,
        "offerDate":        offer_date.isoformat(),
        "acceptedDate":     accepted_date.isoformat(),
        "closeDate":        close_date.isoformat(),
        "daysToClose":      (close_date - inquiry_date).days,
        "earnestMoneyPct":  round(random.uniform(0.01, 0.03), 3),
        "contingencies":    random.sample(["Inspection", "Financing", "Appraisal"], k=random.randint(0, 3)),
        "propertyType":     property_type,
        "city":             listing.get("city"),
        "state":            listing.get("state"),
        "zipCode":          listing.get("zipCode"),
        "marketAvgPrice":   avg_price,
        "marketAvgDom":     avg_dom,
    }


def generate_rental_transaction(enriched_listing: dict, inquiry: dict):
    """
    Given an enriched listing and a converted inquiry, generate a rental/lease transaction.
    Returns None if the listing is not a rental listing.
    """
    listing  = enriched_listing["listing"]
    market   = enriched_listing["market"]

    rent_price    = listing.get("price", listing.get("rentPrice", 0))
    property_type = listing.get("propertyType", "Single Family")
    listing_id    = listing.get("id")

    if not rent_price:
        return None

    market_stats = get_market_stats_for_property(market, property_type, "Standard")
    avg_rent     = market_stats["avgRent"] or rent_price

    # Lease terms
    profile      = PROPERTY_TYPE_PROFILE.get(property_type, DEFAULT_PROPERTY_PROFILE)
    lease_range  = profile[2]
    lease_months = random.randint(*lease_range)

    # Actual rent: might be slightly negotiated
    rent_ratio        = (rent_price / avg_rent) if avg_rent else 1.0
    negotiation_factor = random.uniform(0.97, 1.0) if rent_ratio >= 1.0 else random.uniform(0.99, 1.02)
    agreed_rent       = round(rent_price * negotiation_factor, -1)

    inquiry_date   = datetime.fromisoformat(inquiry["inquiryDate"])
    application_dt = inquiry_date  + timedelta(days=random.randint(1, 7))
    approved_dt    = application_dt + timedelta(days=random.randint(1, 5))
    lease_start    = approved_dt   + timedelta(days=random.randint(3, 14))
    lease_end      = lease_start   + timedelta(days=lease_months * 30)

    return {
        "transactionId":        str(uuid.uuid4()),
        "listingId":            listing_id,
        "inquiryId":            inquiry["inquiryId"],
        "transactionType":      "Rental",
        "tenantName":           inquiry["inquirerName"],
        "tenantEmail":          inquiry["inquirerEmail"],
        "tenantPhone":          inquiry["inquirerPhone"],
        "listedRent":           rent_price,
        "agreedRent":           agreed_rent,
        "rentToListRatio":      round(agreed_rent / rent_price, 4),
        "leaseTermMonths":      lease_months,
        "leaseStartDate":       lease_start.date().isoformat(),
        "leaseEndDate":         lease_end.date().isoformat(),
        "securityDepositMonths": random.choice([1, 1, 1, 2]),  # most common: 1 month
        "securityDepositAmt":   agreed_rent * random.choice([1, 1, 1, 2]),
        "applicationDate":      application_dt.isoformat(),
        "approvedDate":         approved_dt.isoformat(),
        "screeningOutcome":     random.choices(["Approved", "Approved with Conditions", "Denied"], weights=[0.75, 0.15, 0.10])[0],
        "petsAllowed":          random.choice([True, False]),
        "propertyType":         property_type,
        "city":                 listing.get("city"),
        "state":                listing.get("state"),
        "zipCode":              listing.get("zipCode"),
        "marketAvgRent":        avg_rent,
    }


# ---------------------------------------------------------------------------
# MAIN ORCHESTRATOR
# ---------------------------------------------------------------------------

def generate_synthetic_data(enriched_listings: list[dict], is_rental: bool = False) -> dict:
    """
    For a list of enriched listings (listing + market data joined),
    generate all inquiries and transactions.

    Args:
        enriched_listings: Output of your fetch_and_join() function.
        is_rental: True if these are rental listings, False for sale listings.

    Returns:
        {
            "inquiries":    [...],
            "transactions": [...],
        }
    """
    all_inquiries    = []
    all_transactions = []

    property_profile_defaults = DEFAULT_PROPERTY_PROFILE
    transaction_generator     = generate_rental_transaction if is_rental else generate_sale_transaction

    for enriched in enriched_listings:
        listing      = enriched["listing"]
        property_type = listing.get("propertyType", "Single Family")
        profile       = PROPERTY_TYPE_PROFILE.get(property_type, property_profile_defaults)
        conversion_rate = profile[1]

        # 1. Generate inquiries for this listing
        inquiries = generate_inquiries(enriched)

        # 2. Determine which inquiries convert to transactions
        for inquiry in inquiries:
            if random.random() < conversion_rate:
                transaction = transaction_generator(enriched, inquiry)
                if transaction:
                    inquiry["converted"] = True
                    all_transactions.append(transaction)

        all_inquiries.extend(inquiries)

    return {
        "inquiries":    all_inquiries,
        "transactions": all_transactions,
    }


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def load_enriched_listings_from_csv(filepath: str) -> list:
    """
    Reconstruct enriched listing objects from the CSV saved by rentcast_fetch.py.
    Each CSV row becomes:
        {
            "listing": { all listing_* columns, stripped of the prefix },
            "market":  { avgSalePrice, avgDaysOnMarket, avgRent }
        }
    """
    enriched = []

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # --- Rebuild listing dict (strip "listing_" prefix) ---
            listing = {}
            for key, value in row.items():
                if key.startswith("listing_"):
                    field = key[len("listing_"):]
                    # Attempt to parse JSON strings back to dicts/lists
                    if value and value.startswith(("{", "[")):
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError:
                            pass
                    listing[field] = value

            # Coerce numeric fields that generators depend on
            for num_field in ("price", "rentPrice", "daysOnMarket", "bedrooms", "bathrooms", "squareFootage"):
                if num_field in listing and listing[num_field] not in ("", None):
                    try:
                        listing[num_field] = float(listing[num_field])
                    except (ValueError, TypeError):
                        pass

            # --- Rebuild market dict from the flattened market_* columns ---
            def _float(val):
                try:
                    return float(val) if val not in ("", None) else 0
                except (ValueError, TypeError):
                    return 0

            market = {
                "saleData": {
                    "averagePrice":        _float(row.get("market_avgSalePrice")),
                    "averageDaysOnMarket": _float(row.get("market_avgDaysOnMarket")),
                    "dataByPropertyType":  [],   # not stored in CSV; generators fall back gracefully
                },
                "rentalData": {
                    "averagePrice": _float(row.get("market_avgRent")),
                },
            }

            enriched.append({"listing": listing, "market": market})

    return enriched


def save_csv(records: list[dict], filepath: str):
    """Save a list of flat dicts to a CSV file."""
    if not records:
        print(f"  No records to save for {filepath}")
        return

    import os
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Build a stable, ordered list of all keys across all rows
    all_keys = []
    seen = set()
    for record in records:
        for key in record:
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            # Serialise any list fields (e.g. contingencies)
            row = {
                k: json.dumps(v) if isinstance(v, (list, dict)) else v
                for k, v in record.items()
            }
            writer.writerow({k: row.get(k, "") for k in all_keys})

    print(f"  Saved {len(records):,} rows → {filepath}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os

    csv_path = f"{OUTPUT_DIR}/rentcast_properties.csv"
    print(f"Loading enriched listings from {csv_path}...")
    enriched_listings = load_enriched_listings_from_csv(csv_path)
    print(f"  Loaded {len(enriched_listings):,} listings\n")

    # Split by listing_type (tagged in rentcast_fetch.py)
    rental_listings = [e for e in enriched_listings if e["listing"].get("listing_type") == "rental"]
    sale_listings   = [e for e in enriched_listings if e["listing"].get("listing_type") == "sale"]
    print(f"  {len(rental_listings):,} rental listings | {len(sale_listings):,} sale listings\n")

    # --- Generate ---
    print("Generating synthetic data...")
    sale_results   = generate_synthetic_data(sale_listings,   is_rental=False)
    rental_results = generate_synthetic_data(rental_listings, is_rental=True)

    # Combine inquiries (both types), keep listing_type readable via transactionType / channel
    all_inquiries = sale_results["inquiries"] + rental_results["inquiries"]

    # --- Save ---
    print("\nSaving CSVs...")
    save_csv(all_inquiries,                  f"{OUTPUT_DIR}/inquiries.csv")
    save_csv(sale_results["transactions"],   f"{OUTPUT_DIR}/sale_transactions.csv")
    save_csv(rental_results["transactions"], f"{OUTPUT_DIR}/rental_transactions.csv")

    print(f"""
Summary
-------
  Inquiries         : {len(all_inquiries):,}
    ↳ from sale     : {len(sale_results['inquiries']):,}
    ↳ from rental   : {len(rental_results['inquiries']):,}
  Sale transactions : {len(sale_results['transactions']):,}
  Rental transactions: {len(rental_results['transactions']):,}
""")