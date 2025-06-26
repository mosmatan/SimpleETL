import pandas as pd

def transform_data(df):
    """
    Transforms the input DataFrame by cleaning and formatting the data.

    Parameters:
    df (pd.DataFrame): The input DataFrame to be transformed.

    Returns:
    pd.DataFrame: The transformed DataFrame.
    """

    df = clean_types(df)
    df = remove_duplicates(df)
    clean_df = remove_negative(df)

    return clean_df

def clean_types(df):
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    df['order_total'] = pd.to_numeric(df['order_total'], errors='coerce')
    df.dropna(subset=['created_at', 'updated_at', 'order_total'], inplace=True)
    print(df.head())

    return df

def remove_duplicates(df):
    """
    Removes duplicate rows from the DataFrame based on the 'order_id' column.
    Keep the most recent entry based on the 'updated_at' timestamp.

    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with duplicates removed.
    """
    df.sort_values(by='updated_at', ascending=False, inplace=True)
    df.drop_duplicates(subset='order_id', keep='first', inplace=True)
    return df

def remove_negative(df):
    """
    Removes rows with negative 'order_total' values from the DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with negative 'order_total' values removed.
    """
    df = df[df['order_total'] >= 0]
    return df



