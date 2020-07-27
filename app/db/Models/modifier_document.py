from pymongo import UpdateOne

from app.db.connection import mongo


class ModifierDocument:

    def load_import_modifications(self, df, worksheet_id):
        """Fetches the modifications from the modifications collection to update the dataframe"""

        modification = mongo.db.modifications

        modif = modification.find_one({"worksheetId": worksheet_id})
        if modif:
            indexes = list(map(lambda line: line - 1, modif["num"]))
            df.loc[indexes] = modif["line"]

        return df

    def load_top_panel(self, df, worksheet_id):
        """Fetches mapping's top panel elements from top_panel collection to update the dataframe"""

        top_panel = mongo.db.top_panel

        content = top_panel.find_one({"worksheetId": worksheet_id}, {"_id": 0, "content": 1})
        if content:
            for column, value in content["content"].items():
                df[column] = value

        return df

    def load_check_modifications(self, df, worksheet_id, modifications):
        """Fetches checks' modifications from check_modification collection"""

        check_modification = mongo.db.check_modification
        if modifications["is_all"]:
            exist_content = check_modification.find({"worksheetId": worksheet_id}, projection=["line", "content"])
            for modification_document in exist_content:
                for column, value in modification_document["content"].items():
                    if df.get(column) is not None:
                        df.loc[modification_document["line"]][column] = value
        else:
            for index in modifications["indices"]:
                exist_content = self.get_check_modifications(worksheet_id, index)
                if exist_content:
                    for column, value in exist_content["content"].items():
                        if df.get(column) is not None:
                            df.loc[index][column] = value

        return df

    def get_check_modifications(self, worksheet_id, line):
        """Fetches a modification document from check_modifcation collection"""

        check_modification = mongo.db.check_modification

        modification = check_modification.find_one({"worksheetId": worksheet_id, "line": line}, projection=["content"])

        return modification

    def save_check_modifications(self, worksheet_id, modifications, mapped_df):
        """Saves the check modifications in check_modification collection"""

        check_modification = mongo.db.check_modification

        to_insert = []
        for index in modifications["indices"]:
            exist_content = self.get_check_modifications(worksheet_id, index)

            if exist_content:
                tiv = float(mapped_df.loc[index]["tiv_amount"])

                for column, value in modifications["content"].items():
                    exist_content["content"][column] = value
                    if (column == "pd_value") or (column == "bi_value"):
                        tiv = tiv - float(mapped_df.loc[index][column]) + float(value)
                if tiv != mapped_df.loc[index]["tiv_amount"]:
                    exist_content["content"]["tiv_amount"] = tiv
                check_modification.update_one(
                    {'_id': exist_content["_id"]},
                    {'$set': {
                        "content": exist_content["content"]
                    }
                    }, upsert=False
                )
            else:
                to_insert.append({"worksheetId": worksheet_id, "line": index, "content": modifications["content"]})

        if to_insert:
            check_modification.insert_many(to_insert, ordered=False)

    def delete_check_modification(self, worksheet_id, indices=None):
        """Deletes documents in check_modification collection"""

        check_modification = mongo.db.check_modification

        if indices:
            check_modification.delete_many({"worksheetId": worksheet_id, "line": {"$in": indices}})
        else:
            check_modification.delete_many({"worksheetId": worksheet_id})

    def update_indices(self, worksheet_id, indices):
        """Updates line key in modification collection documents after delete"""

        check_modification = mongo.db.check_modification

        min_index = min(indices)
        exist_documents = check_modification.find({"line": {"$gt": min_index}})
        exist_documents = [exist_document for exist_document in exist_documents]
        if exist_documents:
            bulk_updates = []
            for exist_document in exist_documents:
                decrement = sum(i < exist_document["line"] for i in indices)
                bulk_updates.append(
                    UpdateOne({'_id': exist_document["_id"]}, {"$set": {"line": exist_document["line"] - decrement}},
                              upsert=True))

            check_modification.bulk_write(bulk_updates)
