from neomodel import config, db
from data_io.models import Tree, District, Genus, PrecipitationHeight

config.DATABASE_URL = 'bolt://neo4j:ezpw@localhost:7687'

# Save neo references of genera and districts for faster relationship creation
genera_neo = {}
districts_neo = {}


def clear_db():
    db.cypher_query('MATCH (n) DETACH DELETE n')


@db.transaction
def create_genera(average_yearly_genus_precipitation_height):
    for genus, average in average_yearly_genus_precipitation_height.items():
        genera_neo[genus] = Genus(name=genus, precipitation_height_yearly=average).save()


@db.transaction
def create_districts(districts):
    for district in districts:
        districts_neo[district] = District(name=district).save()


@db.transaction
def create_trees(trees):
    for i, tree in enumerate(trees):
        district = tree.pop('district')
        genus = tree.pop('genus')
        tree_neo = Tree.create(tree)[0]

        tree_neo.district.connect(districts_neo[district])

        if genus in genera_neo:
            tree_neo.genus.connect(genera_neo[genus])
        else:
            # TODO: default value for unknown genus?
            genera_neo[genus] = Genus(name=genus, precipitation_height_yearly=0).save()


@db.transaction
def create_precipitation_height(average_daily_p_h):
    for date in average_daily_p_h:
        for district, p_h in average_daily_p_h[date].items():
            p_h_neo = PrecipitationHeight(value=p_h, date=date).save()
            p_h_neo.district.connect(districts_neo[district])
