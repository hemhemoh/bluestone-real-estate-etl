import json
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
FINAL_DIR = DATA_DIR / "transformed_data"

DICT_COLUMNS = ["hoa", "listingOffice", "builder", "listingAgent"]
CONTINGENCIES = ["Inspection", "Financing", "Appraisal"]


def _safe_json_loads(value):
    """Attempt to parse a value as JSON; return the original value on failure."""
    if not isinstance(value, str):
        return value

    value = value.strip()
    if not value:
        return value

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the four raw input CSVs and return them as DataFrames."""
    logger.info("Loading input CSVs from %s", DATA_DIR)
    inquiries = pd.read_csv(DATA_DIR / "raw_data/inquiries.csv")
    rental_transactions = pd.read_csv(DATA_DIR / "raw_data/rental_transactions.csv")
    sale_transactions = pd.read_csv(DATA_DIR / "raw_data/sale_transactions.csv")
    listings = pd.read_csv(DATA_DIR / "raw_data/rentcast_properties.csv")
    return inquiries, rental_transactions, sale_transactions, listings


def clean_listings_columns(listings: pd.DataFrame) -> pd.DataFrame:
    """Strip the 'listing_' prefix from column names and drop irrelevant columns."""
    listings = listings.copy()
    listings.columns = [
        col.replace("listing_", "", 1) if col.startswith("listing_") else col
        for col in listings.columns
    ]
    listings = listings.drop(columns=["type", "market_error"], errors="ignore")
    return listings


def expand_dict_columns(df: pd.DataFrame, dict_columns: list[str]) -> pd.DataFrame:
    """Parse JSON-encoded dict columns and expand them into prefixed flat columns."""
    df = df.copy()
    for col in dict_columns:
        if col not in df.columns:
            continue

        parsed = df[col].apply(_safe_json_loads)
        expanded = pd.DataFrame(
            [x if isinstance(x, dict) else {} for x in parsed],
            index=df.index,
        )

        if expanded.shape[1] == 0:
            continue

        expanded.columns = [f"{col}_{key}" for key in expanded.columns]
        df = df.drop(columns=[col]).join(expanded)

    return df


def explode_history_to_table(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten the nested JSON 'history' column into a long-form table."""
    if "history" not in df.columns:
        return pd.DataFrame(columns=["id", "eventDate"])

    logger.info("Exploding history column (%d rows)", len(df))

    parsed = df["history"].apply(_safe_json_loads)
    mask = parsed.apply(lambda x: isinstance(x, dict))
    subset = df.loc[mask, ["id"]].copy()
    subset["_history"] = parsed[mask]

    def _expand_history(row):
        return [
            {"id": row["id"], "eventDate": event_date, **(event if isinstance(event, dict) else {"value": event})}
            for event_date, event in row["_history"].items()
        ]

    exploded = subset.apply(_expand_history, axis=1)
    if exploded.empty:
        return pd.DataFrame(columns=["id", "eventDate"])

    return pd.DataFrame([item for sublist in exploded for item in sublist])


def add_contingency_flags(sale_transactions: pd.DataFrame) -> pd.DataFrame:
    """Convert the 'contingencies' JSON list into individual boolean flag columns."""
    sale_transactions = sale_transactions.copy()
    if "contingencies" not in sale_transactions.columns:
        return sale_transactions

    parsed = sale_transactions["contingencies"].apply(_safe_json_loads)
    for contingency in CONTINGENCIES:
        col_name = f"contingency_{contingency.lower()}"
        sale_transactions[col_name] = parsed.apply(
            lambda x, c=contingency: c in x if isinstance(x, list) else False
        )

    sale_transactions = sale_transactions.drop(columns=["contingencies"])
    return sale_transactions


def normalize_nan_to_null(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN and blank/whitespace-only strings with pd.NA."""
    df = df.copy()
    df = df.where(pd.notna(df), pd.NA)

    # Convert empty/whitespace-only strings to null as well.
    text_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in text_cols:
        as_string = df[col].astype("string")
        df[col] = as_string.mask(as_string.str.strip().eq(""), pd.NA)

    return df


def enforce_types_for_parquet(
    df: pd.DataFrame,
    *,
    string_columns: Optional[List[str]] = None,
    datetime_columns: Optional[List[str]] = None,
    integer_columns: Optional[List[str]] = None,
    float_columns: Optional[List[str]] = None,
    boolean_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Normalize nulls and coerce configured columns to parquet-friendly dtypes."""
    df = normalize_nan_to_null(df)

    string_columns = string_columns or []
    datetime_columns = datetime_columns or []
    integer_columns = integer_columns or []
    float_columns = float_columns or []
    boolean_columns = boolean_columns or []

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in integer_columns:
        if col in df.columns:
            numeric_series = pd.Series(pd.to_numeric(df[col], errors="coerce"), index=df.index)
            df[col] = numeric_series.astype("Int64")

    for col in float_columns:
        if col in df.columns:
            numeric_series = pd.Series(pd.to_numeric(df[col], errors="coerce"), index=df.index)
            df[col] = numeric_series.astype("Float64")

    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    # Any remaining object columns are safest as nullable string for parquet.
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("string")

    return df


def save_outputs(
    inquiries: pd.DataFrame,
    sale_transactions: pd.DataFrame,
    rental_transactions: pd.DataFrame,
    history_df: pd.DataFrame,
    listings: pd.DataFrame,
) -> None:
    """Write all transformed DataFrames to Parquet and CSV in the final output directory."""
    parquet_dir = FINAL_DIR / "parquet"
    csv_dir = FINAL_DIR / "csv"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Saving outputs to %s", FINAL_DIR)

    inquiries.to_parquet(parquet_dir / "inquiries.parquet", index=False, compression="snappy")
    sale_transactions.to_parquet(parquet_dir / "sale_transactions.parquet", index=False, compression="snappy")
    rental_transactions.to_parquet(
        parquet_dir / "rental_transactions.parquet", index=False, compression="snappy"
    )
    history_df.to_parquet(parquet_dir / "listing_history.parquet", index=False, compression="snappy")
    listings.to_parquet(parquet_dir / "listings.parquet", index=False, compression="snappy")

    inquiries.to_csv(csv_dir / "inquiries.csv", index=False, na_rep="null")
    sale_transactions.to_csv(csv_dir / "sale_transactions.csv", index=False, na_rep="null")
    rental_transactions.to_csv(csv_dir / "rental_transactions.csv", index=False, na_rep="null")
    history_df.to_csv(csv_dir / "listing_history.csv", index=False, na_rep="null")
    listings.to_csv(csv_dir / "listings.csv", index=False, na_rep="null")


def main() -> None:
    """Run the full transformation pipeline: load, transform, and save."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    try:
        inquiries, rental_transactions, sale_transactions, listings = load_inputs()
    except FileNotFoundError as exc:
        logger.error("Missing input file: %s", exc)
        raise SystemExit(1) from exc
    except pd.errors.ParserError as exc:
        logger.error("Malformed CSV: %s", exc)
        raise SystemExit(1) from exc

    logger.info("Cleaning listings columns")
    listings = clean_listings_columns(listings)
    history_df = explode_history_to_table(listings)

    listings = listings.drop(columns=["history"], errors="ignore")
    listings = expand_dict_columns(listings, DICT_COLUMNS)

    logger.info("Adding contingency flags to sale transactions")
    sale_transactions = add_contingency_flags(sale_transactions)

    if "zipCode" in listings.columns:
        listings["zipCode"] = listings["zipCode"].astype(str).str.zfill(5)

    logger.info("Enforcing parquet-friendly dtypes")
    inquiries = enforce_types_for_parquet(
        inquiries,
        datetime_columns=["inquiryDate"],
    )
    rental_transactions = enforce_types_for_parquet(
        rental_transactions,
        datetime_columns=["leaseStartDate", "leaseEndDate", "applicationDate", "approvedDate"],
    )
    sale_transactions = enforce_types_for_parquet(
        sale_transactions,
        datetime_columns=["offerDate", "acceptedDate", "closeDate"],
        boolean_columns=[f"contingency_{c.lower()}" for c in CONTINGENCIES],
    )
    history_df = enforce_types_for_parquet(history_df, datetime_columns=["eventDate"])
    listings = enforce_types_for_parquet(
        listings,
        string_columns=["zipCode"],
        datetime_columns=["listedDate", "removedDate", "createdDate", "lastSeenDate"],
    )

    save_outputs(
        inquiries=inquiries,
        sale_transactions=sale_transactions,
        rental_transactions=rental_transactions,
        history_df=history_df,
        listings=listings,
    )
    logger.info("Transformation pipeline completed successfully")


if __name__ == "__main__":
    main()

