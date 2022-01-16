import csv
import requests
import glob
import utm
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import data_processing.district as districts
import pickle
import time
import os

# List of all days in the considered period
start = datetime(2000, 1, 1)
end = datetime(2021, 12, 31)
days = [start + timedelta(days=i) for i in range((end - start).days + 1)]

# Classification steps of ground water level
classification = ['extremely low', 'very low', 'low', 'normal', 'high', 'very high', 'extremely high']


def get_district_daily_average_g_w():
    # Process raw files
    if not os.path.exists('district_interpolated_dfs.p'):
        district_interpolated_dfs = __process_raw_files()
        pickle.dump(district_interpolated_dfs, open('district_interpolated_dfs.p', 'wb'))
    else:
        district_interpolated_dfs = pickle.load(open('district_interpolated_dfs.p', 'rb'))

    # Calculate relative percentage and classification
    if not os.path.exists('district_average_g_w.p'):
        district_average_g_w = __calculate_relative_metrics(district_interpolated_dfs)
        pickle.dump(district_average_g_w, open('district_average_g_w.p', 'wb'))
    else:
        district_average_g_w = pickle.load(open('district_average_g_w.p', 'rb'))

    # Calculate daily average values per district
    if not os.path.exists('district_daily_average_g_w.p'):
        district_daily_average_g_w = __calculate_daily_metrics(district_average_g_w)
        pickle.dump(district_daily_average_g_w, open('district_daily_average_g_w.p', 'wb'))
    else:
        district_daily_average_g_w = pickle.load(open('district_daily_average_g_w.p', 'rb'))

    return district_daily_average_g_w


# Calculate daily average relative percentage and classification per district
def __calculate_daily_metrics(district_dfs_percentage):
    district_dfs_averages = {k: {} for k in districts.districts}

    for district, dfs in district_dfs_percentage.items():
        concatinated_dfs = pd.concat(dfs, axis=1)

        # Average of percentage
        daily_pt_averages = concatinated_dfs['pt'].mean(axis=1)

        # Average of classification rounded to int
        daily_classification_averages = concatinated_dfs['classification'].mean(axis=1).round(decimals=0).astype(int)

        # Compose result DataFrame and replace int classification with strings
        district_result_df = pd.concat([daily_pt_averages, daily_classification_averages], axis=1)
        district_result_df.columns = ['pt', 'classification']
        district_result_df['classification'] = district_result_df['classification'].apply(lambda x: classification[x])

        district_dfs_averages[district] = district_result_df

    return district_dfs_averages


# Calculate relative percentage and classification based on percentiles
def __calculate_relative_metrics(district_dfs_interpolated):
    district_dfs = {k: [] for k in districts.districts}

    for district, dfs in district_dfs_interpolated.items():
        for df in dfs:
            # New column with relative level in percentage
            df['pt'] = df['gw_value'] / df['gw_value'].mean()

            # Calculate percentiles of this station
            percentiles = [
                df['gw_value'].quantile(q=0.05, interpolation='linear'),
                df['gw_value'].quantile(q=0.15, interpolation='linear'),
                df['gw_value'].quantile(q=0.25, interpolation='linear'),
                df['gw_value'].quantile(q=0.75, interpolation='linear'),
                df['gw_value'].quantile(q=0.85, interpolation='linear'),
                df['gw_value'].quantile(q=0.95, interpolation='linear')
            ]

            # New column with classification based on percentiles as int
            df['classification'] = df['gw_value'].apply(lambda x: __determine_int_classification(x, percentiles))
            df['classification'] = df['classification'].round(decimals=0)

            district_dfs[district].append(df)

    return district_dfs


# Based on https://wasserportal.berlin.de/erlaeuterungen.php
def __determine_int_classification(value, percentiles):
    if value <= percentiles[0]:
        return 0
    if value <= percentiles[1]:
        return 1
    if value <= percentiles[2]:
        return 2
    if value <= percentiles[3]:
        return 3

    if value > percentiles[3]:
        if value > percentiles[4]:
            if value > percentiles[5]:
                return 6
            return 5
        return 4


# Convert raw files to DataFrames and determine district of station
def __process_raw_files():
    district_dfs = {k: [] for k in districts.districts}

    for file in glob.glob('data/gw/*.txt'):
        with open(file, encoding='utf-8') as f:
            lines = f.readlines()

        # Get district of station
        ms_east = float(lines[10].split(';')[1].replace(',', '.'))
        ms_north = float(lines[11].split(';')[1].replace(',', '.'))
        lat, long = utm.to_latlon(ms_east, ms_north, 33, 'U')
        district = districts.get_district_of_lat_long(lat, long)

        # Ignore stations outside of Berlin
        if not district:
            continue

        data = {day: np.nan for day in days}

        # Convert to dict
        for m in lines[15:]:
            date = datetime.strptime(m.split(';')[0], '%d.%m.%Y')
            value = float(m.split(';')[1].replace(',', '.'))
            data[date] = value

        # Convert dict to DataFrame and interpolate missing values
        df = pd.DataFrame.from_dict(data, orient='index', columns=['gw_value'])
        df['gw_value'] = df['gw_value'].interpolate(limit_area='inside')
        df['gw_value'] = df['gw_value'].interpolate(limit_area='outside', limit_direction='both', limit=7)
        df['gw_value'] = df['gw_value'].round(decimals=2)

        district_dfs[district].append(df)

    return district_dfs


# Download raw measurement files
def download_raw_files():
    base_url = 'https://wasserportal.berlin.de/station.php?anzeige=gd&sstation=<nr>&sreihe=w&smode=c&sthema=gw&sdatum=01.01.2000&senddatum=31.12.2021'

    with open('../data/messtellen.txt', encoding='utf-8') as f:
        rows = [row for row in csv.reader(f)][1:]

    for row in rows:
        r = requests.get(base_url.replace('<nr>', row[0]))
        r.encoding = 'ISO-8859-1'

        with open('../data/gw/' + row[0] + '.txt', 'w', encoding='utf-8') as f:
            f.write(r.text)

        time.sleep(1)
