from neomodel import StructuredNode, StringProperty, FloatProperty, DateProperty, RelationshipTo


class Tree(StructuredNode):
    tree_id = StringProperty()
    height = StringProperty()
    planting_year = DateProperty()
    species_german = StringProperty()
    crown_diameter = FloatProperty()
    circumference = FloatProperty()
    utm_east = StringProperty()
    utm_north = StringProperty()
    utm_zone = StringProperty()
    lat = StringProperty()
    long = StringProperty()

    genus = RelationshipTo('Genus', 'IS')
    district = RelationshipTo('District', 'IN')


class Genus(StructuredNode):
    name = StringProperty()
    precipitation_height_yearly = FloatProperty()


class District(StructuredNode):
    name = StringProperty()


class PrecipitationHeight(StructuredNode):
    value = FloatProperty()
    date = DateProperty()

    district = RelationshipTo('District', 'IN')


class GroundWater(StructuredNode):
    relative_water_level = FloatProperty()
    classification = StringProperty()
    date = DateProperty()

    district = RelationshipTo('District', 'IN')
