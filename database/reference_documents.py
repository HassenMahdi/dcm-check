#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.connectors import mongo


class CurrenciesDocument:

    def get_currencies(self):
        """Fetches all currencies code from CURRENCY collections"""

        currencies_collection = mongo.db.CURRENCY

        currencies = currencies_collection.find({}, projection=["CODE"])
        currencies = [currency["CODE"] for currency in currencies]

        return {"currencies": currencies}


class ConstructionsDocument:

    def get_construction_classes(self):
        """Fetches all construction class names from CNSTR_REF_DATA collection"""

        constructions = mongo.db.CNSTR_REF_DATA

        construction_classes = constructions.find({}, projection=["CLASS"]).distinct("CLASS")

        return {"classes": construction_classes}

    def get_all_construction_codes(self):
        """Fetches all construction codes from CNSTR_REF_DATA collection"""

        constructions = mongo.db.CNSTR_REF_DATA

        construction_codes = constructions.find({}, projection=["CODE"])
        construction_codes = [construction_code["CODE"] for construction_code in construction_codes]

        return {"codes": construction_codes}

    def get_construction_codes(self, class_name):
        """Fetches construction codes from CNSTR_REF_DATA collection based on class_name"""

        constructions = mongo.db.CNSTR_REF_DATA

        construction_codes = constructions.find({"CLASS": class_name}, projection=["CODE"])
        construction_codes = [construction_code["CODE"] for construction_code in construction_codes]

        return {"codes": construction_codes}


class OccupancyDocument:

    def get_occupancy_classes(self):
        """Fetches all occupancy class names from OCC_REF_DATA collection"""

        occupancy = mongo.db.OCC_REF_DATA
        occupancy_classes = occupancy.find({}, projection=["CLASS"]).distinct("CLASS")

        return {"classes": occupancy_classes}

    def get_all_occupancy_subclasses(self):
        """Fetches all occupancy subclasses names from OCC_REF_DATA collection"""

        occupancy = mongo.db.OCC_REF_DATA
        occupancy_subclasses = occupancy.find({}, projection=["SUB_CLASS"]).distinct("SUB_CLASS")

        return {"subclasses": occupancy_subclasses}

    def get_occupancy_subclasses(self, class_name):
        """Fetches occupancy subclasses names from OCC_REF_DATA collection based on class_name"""

        occupancy = mongo.db.OCC_REF_DATA
        occupancy_subclasses = occupancy.find({"CLASS": class_name}, projection=["SUB_CLASS"]).distinct("SUB_CLASS")

        return {"subclasses": occupancy_subclasses}

    def get_all_occupancy_codes(self):
        """Fetches all occupancy codes from OCC_REF_DATA collection"""

        occupancy = mongo.db.OCC_REF_DATA

        occupancy_codes = occupancy.find({}, projection=["CODE"])
        occupancy_codes = [occupancy_code["CODE"] for occupancy_code in occupancy_codes]

        return {"codes": occupancy_codes}

    def get_occupancy_codes(self, class_name=None, subclass_name=None):
        """Fetches occupancy code from OCC_REF_DATA collection based on class_name and subclass_name"""

        occupancy = mongo.db.OCC_REF_DATA
        conditions = {}

        if class_name is None:
            conditions["CLASS"] = class_name
        if subclass_name is None:
            conditions["SUB_CLASS"] = subclass_name

        occupancy_codes = occupancy.find(conditions, projection=["CODE"])
        occupancy_codes = [occupancy_code["CODE"] for occupancy_code in occupancy_codes]

        return {"codes": occupancy_codes}


class GeoScopeDocument:

    def get_countries(self):
        """Fetches countries code and name from GEOSCOPE collection"""

        geo_scope = mongo.db.GEOSCOPE

        countries = geo_scope.find({"GEO_LEVEL": "1"}, projection=["GEO_CD", "GEO_DESCRIPTION"])
        countries = [{"code": country["GEO_CD"], "name": country["GEO_DESCRIPTION"]} for country in countries]

        return {"countries": countries}

    def get_states(self, country):
        """Fetches states code and name from GEOSCOPE collection based on country"""

        geo_scope = mongo.db.GEOSCOPE

        states = geo_scope.find({"GEO_LEVEL1_CD": country, "GEO_LEVEL": "2"}, projection=["GEO_CD", "GEO_DESCRIPTION"])
        states = [{"code": state["GEO_CD"], "name": state["GEO_DESCRIPTION"]} for state in states]

        return {"states": states}

    def get_counties(self, state):
        """"Fetches counties code and name from GEOSCOPE collection based on state"""

        geo_scope = mongo.db.GEOSCOPE

        counties = geo_scope.find({"GEO_LEVEL2_CD": state, "GEO_LEVEL": "3"}, projection=["GEO_CD", "GEO_DESCRIPTION"])
        counties = [{"code": county["GEO_CD"], "name": county["GEO_DESCRIPTION"]} for county in counties]

        return {"counties": counties}
