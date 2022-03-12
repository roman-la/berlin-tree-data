import json


# Get max precipitation height per year for genera from file
# Intervals mainly based on https://www.fva-bw.de/fileadmin/publikationen/sonstiges/180201steckbrief.pdf
def get_genera_max_p_h():
    with open('data/genus_requirements.json', encoding='utf-8') as f:
        return json.load(f)
