import pandas as pd
import string


def load_data():
    df_data = pd.read_csv('../data/data.csv')
    df_traffic = pd.read_csv('../data/traffic.csv')
    df_domains_location = pd.read_csv('../data/domains_location.csv')

    return df_data, df_traffic, df_domains_location


def handle_missingvalue(df_data, df_domains_location):
    """
    This function cleans the dataset by handling missing values as specified.
    
    Parameters:
    - file_path: The path to the input CSV file.
    - output_path: The path to save the cleaned CSV file.
    """
    # Load the dataset
    
    # Dropping rows with missing 'source_name'
    df_data = df_data.dropna(subset=['source_name'])

    # Dropping rows with missing 'domain'
    # df_data = df_data.dropna(subset=['domain'])

    # Dropping rows with missing 'published_at'
    df_data = df_data.dropna(subset=['published_at'])

    # Dropping rows where both 'content' and 'full_content' are missing
    df_data = df_data.dropna(subset=['content', 'full_content'], how='all')

    # Filling missing values in 'category' with 'unknown'
    df_data['category'] = df_data['category'].fillna('unknown')

    df_data = df_data.drop(columns=['source_id', 'author', 'url_to_image'])

    df_domains_location['Country'] = df_domains_location['Country'].fillna('unknown')
    
    # Identifying and printing missing values after cleaning
    missing_values_after = df_data.isnull().sum()
    print("\nMissing values after cleaning:\n", missing_values_after)

    return df_data, df_domains_location


def ensure_data_type_consistency(df_data, df_domains_location, df_traffic):
    """
    This function ensures that each column in the DataFrames is of the correct data type.
    
    Parameters:
    - df_data: DataFrame containing article data.
    - df_domains_location: DataFrame containing domain location data.
    
    Returns:
    - Tuple of DataFrames with ensured data type consistency (df_data, df_domains_location).
    """
    # Ensure 'published_at' in df_data is in datetime format
    if 'published_at' in df_data.columns:
        df_data['published_at'] = pd.to_datetime(df_data['published_at'], errors='coerce')

    # Ensure 'GlobalRank' and other numerical columns in df_domains_location are numeric
    numerical_columns = ['GlobalRank', 'TldRank', 'RefSubNets', 'RefIPs', 'PrevGlobalRank', 'PrevTldRank', 'PrevRefSubNets', 'PrevRefIPs']
    for column in numerical_columns:
        if column in df_traffic.columns:
            df_traffic[column] = pd.to_numeric(df_traffic[column], errors='coerce')

    # Ensure categorical columns in both DataFrames are strings
    categorical_columns_data = ['source_name', 'domain', 'category']
    for column in categorical_columns_data:
        if column in df_data.columns:
            df_data[column] = df_data[column].astype(str)
    
    categorical_columns_domains = ['SourceCommonName', 'location', 'Country']
    for column in categorical_columns_domains:
        if column in df_domains_location.columns:
            df_domains_location[column] = df_domains_location[column].astype(str)

    # Display the data types to verify
    print("\nData types in df_data:\n", df_data.dtypes)
    print("\nData types in df_domains_location:\n", df_domains_location.dtypes)
    print("\nData types in df_traffic:\n", df_traffic.dtypes)

    
    return df_data, df_domains_location, df_traffic


def normalize_and_standardize(df_data, df_domains_location, df_traffic):
    """
    This function normalizes and standardizes text and categorical data in the DataFrames.
    
    Parameters:
    - df_data: DataFrame containing article data.
    - df_domains_location: DataFrame containing domain location data.
    - df_traffic: DataFrame containing traffic data.
    
    Returns:
    - Tuple of DataFrames with normalized and standardized data (df_data, df_domains_location, df_traffic).
    """
    
    # Helper function to normalize text
    def normalize_text(text):
        if pd.isna(text):
            return text
        # Convert to lowercase, remove punctuation, and strip whitespace
        text = text.lower().strip()
        text = text.translate(str.maketrans('', '', string.punctuation))
        return text
    
    # Identify columns to normalize and standardize for each DataFrame
    text_columns_data = ['source_name', 'domain']
    text_columns_domains = ['SourceCommonName', 'location', 'Country']
    text_columns_traffic = ['Domain', 'TLD', 'IDN_Domain', 'IDN_TLD']

    # Normalize text columns in df_data
    for column in text_columns_data:
        if column in df_data.columns:
            df_data[column] = df_data[column].apply(normalize_text)
            # Display unique values for inspection
            print(f"\nUnique values in '{column}' after normalization:\n", df_data[column].unique())
    
    # Normalize text columns in df_domains_location
    for column in text_columns_domains:
        if column in df_domains_location.columns:
            df_domains_location[column] = df_domains_location[column].apply(normalize_text)
            # Display unique values for inspection
            print(f"\nUnique values in '{column}' after normalization:\n", df_domains_location[column].unique())
    
    # Normalize text columns in df_traffic
    for column in text_columns_traffic:
        if column in df_traffic.columns:
            df_traffic[column] = df_traffic[column].apply(normalize_text)
            # Display unique values for inspection
            print(f"\nUnique values in '{column}' after normalization:\n", df_traffic[column].unique())
    
    # Example standardization rules (to be adjusted based on the actual unique values found)
    # Create mappings for standardization based on unique values observed
    country_standardization = {
        'usa': 'united states',
        'uk': 'united kingdom',
        'india': 'india'
        # Add more mappings as needed
    }
    
    # Standardize categorical data
    if 'Country' in df_domains_location.columns:
        df_domains_location['Country'] = df_domains_location['Country'].replace(country_standardization)
    
    # Display the cleaned data
    print("\nNormalized df_data:\n", df_data.head())
    print("\nNormalized df_domains_location:\n", df_domains_location.head())
    print("\nNormalized df_traffic:\n", df_traffic.head())

    return df_data, df_domains_location, df_traffic



def clean_data():

    df_data, df_traffic, df_domains_location = load_data()

    # Apply missing value handling
    df_data, df_domains_location = handle_missingvalue(df_data, df_domains_location)
    
    # Ensure data type consistency
    df_data, df_domains_location, df_traffic = ensure_data_type_consistency(df_data, df_domains_location, df_traffic)
    
    # Normalize and standardize text and categorical data
    df_data, df_domains_location, df_traffic = normalize_and_standardize(df_data, df_domains_location, df_traffic)
    
    # Return cleaned DataFrames
    return df_data, df_domains_location, df_traffic

df_data_cleaned, df_domains_location_cleaned, df_traffic_cleaned = clean_data()