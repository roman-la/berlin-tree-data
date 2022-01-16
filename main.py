from data_io import neo
from data_processing import genus, district, tree, precipitation_height, ground_water

neo.clear_db()
print('cleared db')

neo.create_genera(genus.get_genera_average_p_h())
print('imported genera')

neo.create_districts(district.districts.keys())
print('imported districts')

neo.create_ground_water(ground_water.get_district_daily_average_g_w())
print('imported ground water')

neo.create_precipitation_height(precipitation_height.get_daily_district_average_p_h())
print('imported precipitation height')

neo.create_trees(tree.get_trees())
print('imported trees')
