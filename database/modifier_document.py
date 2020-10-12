#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
from datetime import datetime

from database.connectors import mongo


class ModifierDocument:

    def get_modifications(self, worksheet_id, lines=None, is_all=False):
        """Fetches a modification document from check_modifcation collection"""

        check_modification = mongo.db.modifications

        if is_all:
            return check_modification.find({"worksheetId": worksheet_id})
        else:
            return check_modification.find({"worksheetId": worksheet_id, "line": {"$in": lines}},
                                               {"_id": 0, "columns": 1, "line": 1})

    def save_modifications(self, worksheet_id, modifications, user_id):
        """Saves the check modifications in modifications collection"""

        check_modification = mongo.db.modifications

        to_insert = []
        modifications = {int(line): modification for line, modification in modifications.items()}
        to_insert_indices = set(modifications.keys())
        exist_content = self.get_modifications(worksheet_id, list(modifications.keys()))
        if exist_content:
            for exist_modification in exist_content:
                line = exist_modification["line"]
                to_insert_indices.remove(line)
                exist_modification = self.update_modification(modifications[line], exist_modification["columns"],
                                                              user_id)
                check_modification.update_one(
                    {'worksheetId': worksheet_id, "line": line},
                    {'$set': {
                        "columns": exist_modification
                    }
                    }, upsert=False
                )

        for line in to_insert_indices:
            modification = modifications[line]
            modif = {column: {"previous": [modification[column]["previous"]], "new": modification[column]["new"],
                                "updatedAt": datetime.now(), "userId": user_id} for column in modification}
            to_insert.append({"worksheetId": worksheet_id, "line": line, "columns": modif})
        if to_insert:
            check_modification.insert_many(to_insert, ordered=False)

    def update_modification(self, line_modification, exist_modification, user_id):
        """Updates an existent column modification"""

        for column, modification in line_modification.items():
            if exist_modification.get(column):
                exist_modification.get(column)["previous"].append(modification["previous"])
                exist_modification.get(column)["new"] = modification["new"]
                exist_modification.get(column)["userId"] = user_id
                exist_modification.get(column)["updatedAt"] = datetime.now()
            else:
                modif = {"previous": [modification["previous"]], "new": modification["new"],
                         "updatedAt": datetime.now(), "userId": user_id}
                exist_modification[column] = modif

        return exist_modification

    def apply_modifications(self, df, worksheet_id, indices=[], is_all=False):
        """Applies modifications data to a dataframe"""

        if is_all:
            exist_modification = self.get_modifications(worksheet_id, is_all=is_all)
            for modification_document in exist_modification:
                for column, modification in modification_document["columns"].items():
                    if df.get(column) is not None:
                        df.loc[modification_document["line"]][column] = modification["new"]
        else:
            exist_content = self.get_modifications(worksheet_id, indices)
            if exist_content:
                for exist_modif in exist_content:
                    for column, modification in exist_modif["columns"].items():
                        if df.get(column) is not None:
                            df.loc[exist_modif["line"]][column] = modification["new"]
