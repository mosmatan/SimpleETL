import boto3, pandas as pd, os, dotenv

dotenv.load_dotenv()

def extract_data():
    """
    Extracts data from an S3 bucket and returns it as a pandas DataFrame.
    """
    try:
        s3 = boto3.client('s3')
        bucket_name = os.getenv('BUCKET_NAME')
        prefix = os.getenv('PREFIX')

    except:
        raise Exception("Environment variables BUCKET_NAME or PREFIX are not set.")

    try:

        # Find the most recent file in the S3 bucket with the specified prefix
        object_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        file_key = max(object_list['Contents'], key=lambda x: x['LastModified'])['Key']

        # Download the file from S3
        s3.download_file(bucket_name, file_key, 'sample_orders_raw.csv')

    except Exception as e:
        print(e)
        raise

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv('sample_orders_raw.csv')

    return df

def _run():
    """
    Main function to run the extraction process.
    """
    try:
        df = extract_data()
        print("Data extracted successfully.")
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

_run()

