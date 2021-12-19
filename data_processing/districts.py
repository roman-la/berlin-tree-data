import json
from shapely.geometry import shape, Point

# Source: https://raw.githubusercontent.com/m-hoerz/berlin-shapes/master/berliner-bezirke.geojson
with open('data/berliner-bezirke.geojson', encoding='utf-8') as f:
    geojson = json.load(f)

districts = {}
for feature in geojson['features']:
    districts[feature['properties']['spatial_alias']] = shape(feature['geometry'])


# Return district for given regnie (row, column) point
# See: https://stackoverflow.com/questions/20776205/point-in-polygon-with-geojson-in-python
def get_district_of_point(row, column):
    lat, long = __row_column_to_lat_long(row, column)
    for district, polygon in districts.items():
        if polygon.contains(Point(long, lat)):
            return district


# Convert regnie (row, column) to (lat, long)
# See: https://www.dwd.de/DE/leistungen/regnie/download/regnie_beschreibung_pdf.pdf?__blob=publicationFile&v=2
def __row_column_to_lat_long(row, column):
    long_step = 1 * (1 / 60)
    lat_step = 1 * (1 / 120)

    long = (6 - 10 * long_step) + column * long_step
    lat = (55 + 10 * lat_step) - row * lat_step

    return lat, long
