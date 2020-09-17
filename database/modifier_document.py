#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.connectors import mongo


class ModifierDocument:

    def get_modifications(self, worksheet_id):
        """Fetches a modification document from check_modifcation collection"""

        check_modification = mongo.db.modifications

        modification = check_modification.find_one({"worksheetId": worksheet_id}, projection=["columns"])

        return modification

    def save_modifications(self, worksheet_id, modifications):
        """Saves the check modifications in modifications collection"""

        check_modification = mongo.db.modifications
        exist_modification = self.get_modifications(worksheet_id)
        if exist_modification:
            exist_modification = self.update_modification(modifications, exist_modification["columns"])
            check_modification.update_one(
                    {'worksheetId': worksheet_id},
                    {'$set': {
                        "columns": exist_modification
                    }
                    }, upsert=False
                )
        else:
            check_modification.insert_one({"worksheetId": worksheet_id, "columns": modifications})

    def update_modification(self, modifications, exist_modification):
        """Updates an existent column modification"""

        for column, modification in modifications.items():
            if exist_modification.get(column):
                for line, content in modification.items():
                    exist_modification.get(column)[line] = modification[line]
            else:
                exist_modification[column] = modification

        return exist_modification

    def apply_modifications(self, df, worksheet_id, indices=[], is_all=False):
        """Applies modifications data to a dataframe"""

        check_modification = mongo.db.modifications

        exist_modifications = self.get_modifications(worksheet_id)
        if exist_modifications:
            if is_all:
                indices = range(0, df.shape[0])
            
            for column, modification in exist_modifications["columns"].items():
                for line, content in modification.items():
                    if int(line) in indices:
                        df.loc[int(line)][column] = content

