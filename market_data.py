"""
RentCast Market Data Pull
=========================
Pulls market data for all zip codes and outputs 3 normalized CSVs:
  - market_stats.csv            (zip + date, all scalar fields)
  - market_stats_by_proptype.csv (zip + date + propertyType)
  - market_stats_by_bedrooms.csv (zip + date + bedrooms)
"""

import os
import time
import logging
import requests
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

API_KEY       = os.environ.get("RENT_CAST_API_KEY")
API_URL       = "https://api.rentcast.io/v1/markets"
HISTORY_RANGE = 120          # months of history to request (10 years)
RATE_LIMIT_RPS = 0.5         # calls per second (2s between calls) - adjust per your plan
OUTPUT_DIR    = Path("./data")

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Zip codes by city (sourced from listings data) ────────────────────────────

ZIP_CODES_BY_CITY = {
    ("Austin", "TX"): [
        78617, 78620, 78641, 78645, 78652, 78653, 78660, 78664, 78681,
        78701, 78702, 78703, 78704, 78705, 78717, 78719, 78721, 78722,
        78723, 78724, 78725, 78726, 78727, 78728, 78729, 78730, 78731,
        78732, 78733, 78734, 78735, 78736, 78737, 78738, 78739, 78741,
        78742, 78744, 78745, 78746, 78747, 78748, 78749, 78750, 78751,
        78752, 78753, 78754, 78756, 78757, 78758, 78759,
    ],
    ("Houston", "TX"): [
        77002, 77003, 77004, 77005, 77006, 77007, 77008, 77009, 77010,
        77011, 77012, 77013, 77014, 77015, 77016, 77017, 77018, 77019,
        77020, 77021, 77022, 77023, 77024, 77025, 77026, 77027, 77028,
        77029, 77030, 77031, 77032, 77033, 77034, 77035, 77036, 77037,
        77038, 77039, 77040, 77041, 77042, 77043, 77044, 77045, 77046,
        77047, 77048, 77049, 77050, 77051, 77053, 77054, 77055, 77056,
        77057, 77058, 77059, 77060, 77061, 77062, 77063, 77064, 77065,
        77066, 77067, 77068, 77069, 77070, 77071, 77072, 77073, 77074,
        77075, 77076, 77077, 77078, 77079, 77080, 77081, 77082, 77083,
        77084, 77085, 77086, 77087, 77088, 77089, 77090, 77091, 77092,
        77093, 77094, 77095, 77096, 77098, 77099, 77316, 77336, 77338,
        77339, 77345, 77346, 77365, 77377, 77379, 77396, 77447, 77449,
        77477, 77489, 77598,
    ],
    ("Phoenix", "AZ"): [
        85003, 85004, 85006, 85007, 85008, 85009, 85012, 85013, 85014,
        85015, 85016, 85017, 85018, 85019, 85020, 85021, 85022, 85023,
        85024, 85027, 85028, 85029, 85031, 85032, 85033, 85034, 85035,
        85037, 85040, 85041, 85042, 85043, 85044, 85045, 85048, 85050,
        85051, 85053, 85054, 85083, 85085, 85086, 85087, 85254, 85281,
        85308, 85331, 85339, 85353, 85373, 86323,
    ],
    ("Denver", "CO"): [
        80010, 80012, 80014, 80016, 80020, 80030, 80110, 80121, 80123,
        80202, 80203, 80204, 80205, 80206, 80207, 80209, 80210, 80211,
        80212, 80214, 80215, 80216, 80218, 80219, 80220, 80221, 80222,
        80223, 80224, 80226, 80227, 80228, 80229, 80230, 80231, 80232,
        80233, 80234, 80235, 80236, 80237, 80238, 80239, 80241, 80246,
        80247, 80249, 80260, 80401, 80470, 80516,
    ],
    ("Atlanta", "GA"): [
        30009, 30033, 30038, 30043, 30097, 30213, 30291, 30303, 30305,
        30306, 30307, 30308, 30309, 30310, 30311, 30312, 30313, 30314,
        30315, 30316, 30317, 30318, 30319, 30324, 30326, 30327, 30328,
        30329, 30331, 30332, 30336, 30337, 30338, 30339, 30340, 30341,
        30342, 30344, 30345, 30346, 30349, 30350, 30354, 30360, 30363,
    ],
    ("Chicago", "IL"): [
        60016, 60067, 60093, 60176, 60201, 60202, 60411, 60601, 60602,
        60603, 60604, 60605, 60606, 60607, 60608, 60609, 60610, 60611,
        60612, 60613, 60614, 60615, 60616, 60617, 60618, 60619, 60620,
        60621, 60622, 60623, 60624, 60625, 60626, 60628, 60629, 60630,
        60631, 60632, 60633, 60634, 60636, 60637, 60638, 60639, 60640,
        60641, 60642, 60643, 60644, 60645, 60646, 60647, 60649, 60651,
        60652, 60653, 60654, 60655, 60656, 60657, 60659, 60660, 60661,
        60706, 60707, 60712, 60714, 60827,
    ],
    ("Charlotte", "NC"): [
        28202, 28203, 28204, 28205, 28206, 28207, 28208, 28209, 28210,
        28211, 28212, 28213, 28214, 28215, 28216, 28217, 28226, 28227,
        28262, 28269, 28270, 28273, 28277, 28278,
    ],
}

# Flat deduplicated list (preserves city order)
seen = set()
ZIP_CODES = [
    z for zips in ZIP_CODES_BY_CITY.values()
    for z in zips
    if not (z in seen or seen.add(z))
]


# ── Field extractors ───────────────────────────────────────────────────────────

SALE_SCALAR_FIELDS = [
    "averagePrice", "medianPrice", "minPrice", "maxPrice",
    "averagePricePerSquareFoot", "medianPricePerSquareFoot",
    "minPricePerSquareFoot", "maxPricePerSquareFoot",
    "averageSquareFootage", "medianSquareFootage",
    "minSquareFootage", "maxSquareFootage",
    "averageDaysOnMarket", "medianDaysOnMarket",
    "minDaysOnMarket", "maxDaysOnMarket",
    "newListings", "totalListings",
]

RENTAL_SCALAR_FIELDS = [
    "averageRent", "medianRent", "minRent", "maxRent",
    "averageRentPerSquareFoot", "medianRentPerSquareFoot",
    "minRentPerSquareFoot", "maxRentPerSquareFoot",
    "averageSquareFootage", "medianSquareFootage",
    "minSquareFootage", "maxSquareFootage",
    "averageDaysOnMarket", "medianDaysOnMarket",
    "minDaysOnMarket", "maxDaysOnMarket",
    "newListings", "totalListings",
]


def extract_stat_row(zip_code, date, data_type, record):
    """Flatten a single stat block (top-level or history entry) into a dict."""
    row = {"zip_code": zip_code, "date": date, "data_type": data_type}
    fields = SALE_SCALAR_FIELDS if data_type == "sale" else RENTAL_SCALAR_FIELDS
    for f in fields:
        row[f] = record.get(f)
    return row


def extract_subtype_rows(zip_code, date, data_type, records, group_field, group_key):
    """Flatten dataByPropertyType or dataByBedrooms array entries."""
    rows = []
    fields = SALE_SCALAR_FIELDS if data_type == "sale" else RENTAL_SCALAR_FIELDS
    for item in (records or []):
        row = {
            "zip_code": zip_code,
            "date": date,
            "data_type": data_type,
            group_key: item.get(group_field),
        }
        for f in fields:
            row[f] = item.get(f)
        rows.append(row)
    return rows


def process_response(zip_code, response_json):
    """Parse one API response into rows for all 3 output tables."""
    stats_rows       = []
    proptype_rows    = []
    bedrooms_rows    = []

    for data_type, section_key in [("sale", "saleData"), ("rental", "rentalData")]:
        section = response_json.get(section_key)
        if not section:
            continue

        last_updated = section.get("lastUpdatedDate")

        # ── Current snapshot (top-level) ──────────────────────────────────────
        stats_rows.append(extract_stat_row(zip_code, last_updated, data_type, section))

        proptype_rows += extract_subtype_rows(
            zip_code, last_updated, data_type,
            section.get("dataByPropertyType", []),
            "propertyType", "property_type"
        )
        bedrooms_rows += extract_subtype_rows(
            zip_code, last_updated, data_type,
            section.get("dataByBedrooms", []),
            "bedrooms", "bedrooms"
        )

        # ── Historical snapshots ──────────────────────────────────────────────
        for month_key, hist in (section.get("history") or {}).items():
            hist_date = hist.get("date", month_key)

            stats_rows.append(extract_stat_row(zip_code, hist_date, data_type, hist))

            proptype_rows += extract_subtype_rows(
                zip_code, hist_date, data_type,
                hist.get("dataByPropertyType", []),
                "propertyType", "property_type"
            )
            bedrooms_rows += extract_subtype_rows(
                zip_code, hist_date, data_type,
                hist.get("dataByBedrooms", []),
                "bedrooms", "bedrooms"
            )

    return stats_rows, proptype_rows, bedrooms_rows


# ── Main pull ─────────────────────────────────────────────────────────────────

def main():
    if not API_KEY:
        raise ValueError("Set RENTCAST_API_KEY environment variable before running.")

    OUTPUT_DIR.mkdir(exist_ok=True)

    all_stats    = []
    all_proptype = []
    all_bedrooms = []

    headers = {"X-Api-Key": API_KEY, "Accept": "application/json"}
    total   = len(ZIP_CODES)

    for i, zip_code in enumerate(ZIP_CODES, 1):
        log.info(f"[{i}/{total}] Pulling zip {zip_code} ...")

        params = {
            "zipCode":      str(zip_code),
            "dataType":     "All",
            "historyRange": HISTORY_RANGE,
        }

        try:
            resp = requests.get(API_URL, headers=headers, params=params, timeout=30)

            if resp.status_code == 429:
                log.warning("Rate limited — waiting 60s ...")
                time.sleep(60)
                resp = requests.get(API_URL, headers=headers, params=params, timeout=30)

            if resp.status_code == 404:
                log.warning(f"  No data for {zip_code}, skipping.")
                time.sleep(1 / RATE_LIMIT_RPS)
                continue

            resp.raise_for_status()
            data = resp.json()

        except requests.RequestException as e:
            log.error(f"  Request failed for {zip_code}: {e}")
            time.sleep(5)
            continue

        s, p, b = process_response(zip_code, data)
        all_stats    += s
        all_proptype += p
        all_bedrooms += b

        log.info(f"  → {len(s)} stat rows, {len(p)} proptype rows, {len(b)} bedroom rows")

        # Checkpoint every 50 zips
        if i % 50 == 0:
            _save_checkpoints(all_stats, all_proptype, all_bedrooms)
            log.info(f"  ✓ Checkpoint saved at {i} zips")

        time.sleep(1 / RATE_LIMIT_RPS)

    # Final save
    _save_checkpoints(all_stats, all_proptype, all_bedrooms, final=True)
    log.info(f"\nDone! Files written to ./{OUTPUT_DIR}/")
    log.info(f"  market_stats.csv:             {len(all_stats):,} rows")
    log.info(f"  market_stats_by_proptype.csv: {len(all_proptype):,} rows")
    log.info(f"  market_stats_by_bedrooms.csv: {len(all_bedrooms):,} rows")


def _save_checkpoints(stats, proptype, bedrooms, final=False):
    suffix = "" if final else "_checkpoint"
    pd.DataFrame(stats).to_csv(
        OUTPUT_DIR / f"market_stats{suffix}.csv", index=False)
    pd.DataFrame(proptype).to_csv(
        OUTPUT_DIR / f"market_stats_by_proptype{suffix}.csv", index=False)
    pd.DataFrame(bedrooms).to_csv(
        OUTPUT_DIR / f"market_stats_by_bedrooms{suffix}.csv", index=False)


if __name__ == "__main__":
    main()