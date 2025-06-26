import pandas as pd, os, dotenv, boto3
import sqlalchemy as sa

dotenv.load_dotenv()

db_url = os.getenv('DATABASE_URL')
print(db_url)
engine = sa.create_engine(db_url)

def load_data(df):
    """
    Loads the transformed DataFrame to an S3 bucket.

    Parameters:
    df (pd.DataFrame): The DataFrame to be loaded.
    """
    load_to_s3(df)
    load_to_db(df)

    print("Data loading completed.")



def load_to_s3(df):
    """
    Loads the DataFrame to S3.

    Parameters:
    df (pd.DataFrame): The DataFrame to be loaded.
    """
    try:
        s3 = boto3.client('s3')
        bucket_name = os.getenv('BUCKET_NAME')

    except Exception as error:
        raise Exception("Environment variable BUCKET_NAME is not set or invalid.") from error

    output_file = 'transformed_orders.csv'

    # Save the DataFrame to a CSV file
    df.to_csv(output_file, index=False)

    try:
        s3.upload_file(output_file, bucket_name, output_file)
    except Exception as e:
        print(e)
        raise

    print(f"Data loaded successfully to {bucket_name}/{output_file}")

def load_to_db(df):
    """
    Placeholder function for loading data to a database.
    Currently, this function does nothing but can be implemented later.

    Parameters:
    df (pd.DataFrame): The DataFrame to be loaded.
    """

    with engine.connect() as connection:
        # Placeholder for database loading logic
        # For example, you could use df.to_sql() to load the DataFrame to a SQL table
        try:
            df.to_sql('orders', con=connection, if_exists='replace', index=False)
        except Exception as e:
            print(f"An error occurred while loading data to the database: {e}")
            raise


    print("Database loading functionality is not implemented yet.")