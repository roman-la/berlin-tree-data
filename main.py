from data_io import neo
from data_processing import genera, districts, trees, precipitation_heights

neo.clear_db()
neo.create_genera(genera.averages)
neo.create_districts(districts.districts.keys())
neo.create_trees(trees.get_trees())
neo.create_precipitation_height(precipitation_heights.calculate_average_daily_district_p_h())
