import extract, transform, load, pandas as pd

def run_pipeline():
    """
    Runs the ETL pipeline: Extracts data, transforms it, and loads it to S3.
    """
    try:
        # Step 1: Extract data
        df = extract.extract_data()
        print("Data extraction completed.")

        # Step 2: Transform data
        transformed_df = transform.transform_data(df)
        print("Data transformation completed.")

        # Step 3: Load data
        load.load_data(transformed_df)
        print("Data loading completed.")

    except Exception as e:
        print(f"An error occurred in the ETL pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()
    print("ETL pipeline executed successfully.")