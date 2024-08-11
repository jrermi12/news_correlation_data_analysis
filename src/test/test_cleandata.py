import pytest
import sys
sys.path.append("../")

from clean_data import  clean_data, load_data


def test_cleandata():
    df_data, df_traffic, df_domains_location = load_data()

    df_data_cleaned, df_traffic_cleaned, df_domains_location_cleaned = clean_data(df_data, df_traffic, df_domains_location)
    
    print(df_data_cleaned)
    print(df_traffic_cleaned)
    print(df_domains_location_cleaned)
    