from data_io import neo
from data_processing import genera, districts, trees, precipitation_heights

neo.clear_db()
print('cleared db')

neo.create_genera(genera.calculate_genera_p_h_averages())
print('imported genera')

neo.create_districts(districts.districts.keys())
print('imported districts')

neo.create_trees(trees.get_trees())
print('imported trees')

neo.create_precipitation_height(precipitation_heights.calculate_average_daily_district_p_h())
print('imported precipitation height')
