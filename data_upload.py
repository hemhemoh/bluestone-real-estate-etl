import boto3
import os

bucket_name = "bluestone-real-estate-amdari"
s3 = boto3.client("s3", region_name="us-east-1")

# Each entry maps a local folder to its file -> S3 prefix mapping
upload_sources = {
    # Bronze – raw data as-is from source
    "./data/raw_data": {
        "rentcast_properties.csv": "bronze/listings/",
        "inquiries.csv":           "bronze/inquiries/",
        "sale_transactions.csv":   "bronze/sale_transactions/",
        "rental_transactions.csv": "bronze/rental_transactions/",
    },
    "./data/market": {
        "market_stats.csv":             "silver/market_data/market_stats/",
        "market_stats_by_proptype.csv": "silver/market_data/by_property_type/",
        "market_stats_by_bedrooms.csv": "silver/market_data/by_bedrooms/",
    },
    # Silver – cleaned & transformed
    "./data/transformed_data/csv": {
        "listings.csv":            "silver/listings/",
        "listing_history.csv":     "silver/listing_history/",
        "sale_transactions.csv":   "silver/sale_transactions/",
        "rental_transactions.csv": "silver/rental_transactions/",
        "inquiries.csv":           "silver/inquiries/",
    },
}

for local_folder, folder_mapping in upload_sources.items():
    print(f"\n--- Processing {local_folder} ---")
    for file in os.listdir(local_folder):
        local_path = os.path.join(local_folder, file)

        if file in folder_mapping:
            s3_key = f"{folder_mapping[file]}{file}"

            s3.upload_file(
                Filename=local_path,
                Bucket=bucket_name,
                Key=s3_key
            )

            print(f"Uploaded {file} to {s3_key}")
        else:
            print(f"Skipped {file} (not in mapping)")



