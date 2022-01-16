import json


# Calculate average yearly genera precipitation height
# Intervals mainly based on https://www.fva-bw.de/fileadmin/publikationen/sonstiges/180201steckbrief.pdf
def get_genera_average_p_h():
    with open('data/genus_requirements.json', encoding='utf-8') as f:
        genera_requirements = json.load(f)

    genera_p_h_averages = {}
    for genus, properties in genera_requirements.items():
        interval_averages = []
        for interval in properties['precipitation_height_intervals']:
            interval_averages.append((interval[0] + interval[1]) / 2)
        genera_p_h_averages[genus] = sum(interval_averages) / len(interval_averages)

    return genera_p_h_averages
