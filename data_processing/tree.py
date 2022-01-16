import json
import utm
from datetime import datetime
from multiprocessing.dummy import Pool
import multiprocessing


# Data sources:
# https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_wfs_baumbestand_an?request=getfeature&service=wfs&version=2.0.0&typenames=s_wfs_baumbestand_an&outputFormat=application/json
# https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_wfs_baumbestand?request=getfeature&service=wfs&version=2.0.0&typenames=s_wfs_baumbestand&outputFormat=application/json
# https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_baumbestand?request=getfeature&service=wfs&version=2.0.0&typenames=s_baumbestand&outputFormat=application/json
# https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_baumbestand_an?request=getfeature&service=wfs&version=2.0.0&typenames=s_baumbestand_an&outputFormat=application/json


def get_trees():
    wfs_trees = __process_raw_tree_data('data/s_wfs_baumbestand_an.json') + \
                __process_raw_tree_data('data/s_wfs_baumbestand.json')
    non_wfs_trees = __process_raw_tree_data('data/s_baumbestand.json') + \
                    __process_raw_tree_data('data/s_baumbestand_an.json')

    # Calculate missing ids in wfs data
    wfs_trees_ids = [x['tree_id'] for x in wfs_trees]
    non_wfs_trees_ids = [x['tree_id'] for x in non_wfs_trees]
    missing_tree_ids = set(non_wfs_trees_ids) - set(wfs_trees_ids)

    combined_trees = wfs_trees

    # Add all missing non_wfs_trees to result
    for non_wfs_tree in non_wfs_trees:
        if non_wfs_tree['tree_id'] in missing_tree_ids:
            combined_trees.append(non_wfs_tree)

    return combined_trees


# Read file and start tree processing with multiprocessing
def __process_raw_tree_data(path):
    with open(path, encoding='utf-8') as f:
        trees_raw = json.load(f)['features']

    with Pool(multiprocessing.cpu_count()) as pool:
        trees = pool.map(process_tree, trees_raw)

    return [x for x in trees if x]


# Convert raw data to unified format, also converting utm coordinates to wgs84 (lat, long)
def process_tree(tree_raw):
    tree = {'tree_id': tree_raw['id']}

    tree_raw_props = tree_raw['properties']

    # District (skip whole tree if no district given)
    if 'bezirk' in tree_raw_props:
        if len(tree_raw_props['bezirk']) < 5:
            return
        else:
            tree['district'] = tree_raw_props['bezirk']
    else:
        return

    # Genus (skip whole tree if no genus given)
    if 'gattung_botanisch' in tree_raw_props:
        if len(tree_raw_props['gattung_botanisch']) > 1:
            tree['genus'] = tree_raw_props['gattung_botanisch'].title()
        else:
            return
    elif 'gattung' in tree_raw_props:
        if len(tree_raw_props['gattung']) > 1:
            tree['genus'] = tree_raw_props['gattung'].title()
        else:
            return
    else:
        return

    # Coordinates
    if 'geometry' in tree_raw:
        utm_east = tree_raw['geometry']['coordinates'][0]
        utm_north = tree_raw['geometry']['coordinates'][1]
        tree['utm_east'] = utm_east
        tree['utm_north'] = utm_north
        tree['utm_zone'] = '33U'
        tree['lat'], tree['long'] = utm.to_latlon(utm_east, utm_north, 33, 'U')

    # Height
    if 'baumhoehe_akt' in tree_raw_props:
        try:
            tree['height'] = float(tree_raw_props['baumhoehe_akt'])
        except:
            pass
    elif 'baumhoehe' in tree_raw_props:
        try:
            tree['height'] = float(tree_raw_props['baumhoehe'])
        except:
            pass

    # Plant year
    if 'pflanzjahr' in tree_raw_props:
        try:
            tree['planting_year'] = datetime.strptime(tree_raw_props['pflanzjahr'], '%Y').date()
        except:
            pass

    # Species german
    if 'art_deutsch' in tree_raw_props:
        if len(tree_raw_props['art_deutsch']) > 1:
            tree['species_german'] = tree_raw_props['art_deutsch']
    elif 'art_dtsch' in tree_raw_props:
        if len(tree_raw_props['art_dtsch']) > 1:
            tree['species_german'] = tree_raw_props['art_dtsch']

    # Crown diameter
    if 'kronendurchmesser_akt' in tree_raw_props:
        try:
            tree['crown_diameter'] = float(tree_raw_props['kronendurchmesser_akt'])
        except:
            pass
    elif 'kronedurch' in tree_raw_props:
        try:
            tree['crown_diameter'] = float(tree_raw_props['kronedurch'])
        except:
            pass

    # Circumference
    if 'stammumfang_akt' in tree_raw_props:
        try:
            tree['circumference'] = float(tree_raw_props['stammumfang_akt'])
        except:
            pass
    elif 'stammumfg' in tree_raw_props:
        try:
            tree['circumference'] = float(tree_raw_props['stammumfg'])
        except:
            pass

    return tree
