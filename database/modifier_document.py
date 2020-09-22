#!/usr/bin/python
# -*- coding: utf-8 -*-

from database.connectors import mongo


class ModifierDocument:

    def get_modifications(self, worksheet_id, line):
        """Fetches a modification document from check_modifcation collection"""

        check_modification = mongo.db.modifications

        modification = check_modification.find_one({"worksheetId": worksheet_id, "line": line}, projection=["columns"])

        return modification

    def save_modifications(self, worksheet_id, modifications):
        """Saves the check modifications in modifications collection"""

        check_modification = mongo.db.modifications

        to_insert = []
        modifications = {int(line): modification for line, modification in modifications.items()}
        for line, modification in modifications.items():
            exist_modification = self.get_modifications(worksheet_id, line)
            if exist_modification:
                exist_modification = self.update_modification(modification, exist_modification["columns"])
                check_modification.update_one(
                    {'worksheetId': worksheet_id},
                    {'$set': {
                        "columns": exist_modification
                    }
                    }, upsert=False
                )
            else:
                modif = {column: {"previous": [modification[column]["previous"]], "new": modification[column]["new"]}
                         for column in modification}
                to_insert.append({"worksheetId": worksheet_id, "line": line, "columns": modif})
        if to_insert:
            check_modification.insert_many(to_insert, ordered=False)

    def update_modification(self, line_modification, exist_modification):
        """Updates an existent column modification"""

        for column, modification in line_modification.items():
            if exist_modification.get(column):
                exist_modification.get(column)["previous"].append(modification["previous"])
                exist_modification.get(column)["new"] = modification["new"]
            
            else:
                modif = {"previous" : [modification["previous"]], "new": modification["new"]}
                exist_modification[column] = modif

        return exist_modification

    def apply_modifications(self, df, worksheet_id, indices=[], is_all=False):
        """Applies modifications data to a dataframe"""

        check_modification = mongo.db.modifications

        if is_all:
            exist_modification = check_modification.find({"worksheetId": worksheet_id}, projection=["line", "columns"])
            for modification_document in exist_modification:
                for column, modification in modification_document["columns"].items():
                    if df.get(column) is not None:
                        df.loc[modification_document["line"]][column] = modification["new"]
        else:
            for line in indices:
                exist_content = self.get_modifications(worksheet_id, line)
                if exist_content:
                    for column, modification in exist_content["columns"].items():
                        if df.get(column) is not None:
                            df.loc[line][column] = modification["new"]
