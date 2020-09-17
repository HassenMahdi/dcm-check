#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.connectors import mongo


class CheckerDocument:

    def get_mappings(self,mappingId):
        """Fetches a document from the mappings collection based on the given params"""

        mappings = mongo.db.mappings

        mapping = mappings.find_one({"mappingId": mappingId}, {"_id": 0, "rules": 1})
        if mapping:
            return {rule["target"]: rule["source"] for rule in mapping["rules"]}
            
    def get_ref_value(self, ref_collection, field_name, condition):
        """Fetches all field names in the passed collection"""

        collection = eval(f"mongo.db.{ref_collection.upper()}")

        field_names = collection.find(condition, {field_name: 1, "_id": 0})

        return {ref_name[field_name] for ref_name in field_names}

    def get_all_target_fields(self, domain_id):
        """Fetches all target fields for running check job"""

        domain_fields = self.get_domain_fields(domain_id)
        target_fields = {}

        for field in domain_fields:
            target_fields[field["name"]] = {key: field.get(key) for key in ["label", "type", "rules"]}
        
        return target_fields

    def get_headers(self, domain_id):
        """Fetches all the the documents from Scor Fields collection to get lob fields details"""

        domain_fields = self.get_domain_fields(domain_id)
        headers = []
        for field in domain_fields:
            headers.append({key: field[key] for key in ["name", "label", "type", "description"]})

        return headers

    def get_domain_fields(self, domain_id):
        """Fetches all the domain fields"""

        fields_collection = mongo.db[f"{domain_id}.fields"]
        domain_fields = fields_collection.find()

        return domain_fields
