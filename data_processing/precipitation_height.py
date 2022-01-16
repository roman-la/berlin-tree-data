import tarfile
import gzip
import os
from datetime import datetime
from textwrap import wrap
import json
import glob
from multiprocessing.dummy import Pool
import pickle

# Regnie source:
# https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/regnie/

# Generated with district.py
with open('data/regnie_to_district.json', 'r', encoding='utf-8') as f:
    district_points = json.load(f)


def get_daily_district_average_p_h():
    if not os.path.exists('daily_district_average_p_h.p'):
        daily_district_average_p_h = {}
        for archive in glob.glob('data/ra*.tar'):

            # Create list of input tuples for multiprocessing
            input_tuples = [(file_name, daily_p_h_map) for file_name, daily_p_h_map in __read_archive(archive).items()]

            # TODO: Check if multiprocessing is useful here
            with Pool() as pool:
                result_tuples = pool.map(__process_map, input_tuples)

            # Convert result tuples to dict
            for result_tuple in result_tuples:
                daily_district_average_p_h[result_tuple[0]] = result_tuple[1]
    else:
        daily_district_average_p_h = pickle.load(open('daily_district_average_p_h.p', 'rb'))

    return daily_district_average_p_h


# Extract daily maps from nested archive
def __read_archive(path):
    # Extract main archive
    with open(path, 'rb') as archive:
        with tarfile.open(fileobj=archive, mode='r') as f:
            f.extractall('./temp')

    # Extract maps
    p_h_maps = {}
    for filename in os.listdir('./temp'):
        with gzip.open('./temp/' + filename, 'rt') as f:
            p_h_maps[filename] = f.read()

    # Delete temp files
    for filename in os.listdir('./temp'):
        os.remove('./temp/' + filename)
    os.rmdir('./temp')

    return p_h_maps


# Calculate average rainfall per district from daily map
def __process_map(input_tuple):
    file_name, p_h_map = input_tuple
    date = datetime.strptime(file_name[2:-3], '%y%m%d').date()

    p_h_values = {district: [] for district in district_points}
    for row, line in enumerate(p_h_map.splitlines()):
        # Skip lines without data
        if not line.startswith('-999'):
            continue

        # Skip lines not near Berlin
        if row < 285 or row > 335:
            continue

        for column, value in enumerate(wrap(line, 4)):
            # Skip points outside map
            if value == '-999':
                continue

            # Skip columns not near Berlin
            if column < 430 or column > 480:
                continue

            for district, points in district_points.items():
                if [row, column] in points:
                    p_h_values[district].append(int(value))

    # Calculate average
    for district, values in p_h_values.items():
        p_h_values[district] = sum(p_h_values[district]) / len(p_h_values[district])

    return date, p_h_values
