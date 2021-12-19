import tarfile
import gzip
import os
from datetime import datetime
from textwrap import wrap
import json
import glob

# Regnie source:
# https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/regnie/

# Generated with districts.py
with open('data/regnie_to_district.json', 'r', encoding='utf-8') as f:
    district_points = json.load(f)


def calculate_average_daily_district_p_h():
    average_daily_district_p_h = {}
    for archive in glob.glob('data/ra*.tar'):
        for file_name, daily_p_h_map in __read_archive(archive).items():
            date = datetime.strptime(file_name[2:-3], '%y%m%d').date()
            average_daily_district_p_h[date] = __process_map(daily_p_h_map)

    return average_daily_district_p_h


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
def __process_map(p_h_map):
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

    return p_h_values