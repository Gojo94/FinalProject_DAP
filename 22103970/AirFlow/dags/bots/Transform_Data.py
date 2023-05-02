import pandas as pd
from GetDataFromMongo import get_data

def drop_columns_with_high_missing_values(df, threshold):
    """
    Drops columns from the dataframe if they have a percentage of missing values
    greater than the threshold value.
    """
    missing_percentages = df.isna().sum() / df.shape[0] * 100
    columns_to_drop = missing_percentages[missing_percentages > threshold].index.tolist()
    relevant_columns = ['surface_condition']
    for column in columns_to_drop:
        if column not in relevant_columns:
            df.drop(column, axis=1, inplace=True)
    return df

def fill_missing_values(df, columns, value):
    """
    Fills missing values in the specified columns with the given value.
    """
    for column in columns:
        df[column].fillna(value, inplace=True)
    return df

def drop_redundant_columns(df, columns):
    """
    Drops redundant columns from the dataframe.
    """
    df.drop(columns, axis=1, inplace=True)
    return df

def extract_time_features(df, column):
    """
    Extracts year, month, day, and hour features from the specified datetime column.
    """
    df[column] = pd.to_datetime(df[column])
    df['crash_Year'] = df[column].dt.year
    df['crash_Month'] = df[column].dt.month
    df['crash_Day'] = df[column].dt.day
    df['crash_Hour'] = df[column].dt.hour
    df.drop(column, axis=1, inplace=True)
    return df

def clean_data():
    """
    Cleans the given dataframe by dropping irrelevant columns, filling missing values,
    and extracting time features.
    """
    # Load the data
    df = get_data()

    df = drop_columns_with_high_missing_values(df, 6)
    df = fill_missing_values(df, ['surface_condition', 'at_fault', 'driver_substance_abuse'], 'UNKNOWN')
    df = drop_redundant_columns(df, ['report_number', 'geolocation'])
    df = extract_time_features(df, 'crash_date_time')
    
    df.to_csv('clean_data.csv', index=False)
    


# Load the data
#df = get_data

# Clean the data
#df = clean_data(df)

# Save the cleaned data to a new CSV file
#df.to_csv('cleaned_data.csv', index=False)
