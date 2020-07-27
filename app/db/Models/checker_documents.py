#!/usr/bin/python
# -*- coding: utf-8 -*-

from app.db.connection import mongo
import app.main.service.cleansing_service as cleansing_service
from app.db.Models.field import TargetField



class CheckerDocument:

    def get_mappings(self,fileId, domainId):
        """Fetches a document from the mappings collection based on the given params"""

        mappings = mongo.db.mappings

        mapping = mappings.find_one({"fileId": fileId, "domainId": domainId}, {"_id": 0, "rules": 1})
        if mapping:
            return {rule["target"]: rule["source"] for rule in mapping["rules"]}

    def get_headers(self, domain_id):
        """Fetches all the the documents from Scor Fields collection to get lob fields details"""
        target_fields= self.get_target_fields(domain_id)
        return [target for target in target_fields]

    def get_all_target_fields(self,domain_id):
        fields_names = self.get_target_fields(domain_id)
        target_names = {}
        for field_name in fields_names:
            code =field_name.get("name","")
            target_names[code]={'name': field_name.get("name",""),'label': field_name.get("label",""),'description': field_name.get("description",""),
                                 'category': field_name.get("category",""),'type': field_name.get("type",""),'rules': field_name.get("rules",""),
                                 'mandatory': field_name.get("mandatory",""),'editable': field_name.get("editable","")}

        return target_names

    def get_target_fields(self, domain_id, query = {} ):
        Table = "fields"
        scor_fields = mongo.db[f"{domain_id}.{Table}"]
        return list(scor_fields.find(query))

    def get_ref_value(self, ref_collection, field_name, condition):
        """Fetches all field names in the passed collection"""

        collection = eval(f"mongo.db.{ref_collection.upper()}")

        field_names = collection.find(condition, {field_name: 1, "_id": 0})

        return {ref_name[field_name] for ref_name in field_names}

    def get_default_values(self, lob_id):
        """Gets the default value from scor_fields collection"""

        scor_fields = mongo.db.scor_fields

        fields_default_values = scor_fields.find({"lob": lob_id, "defaultValue": {"$exists": True}},
                                                 {"_id": 0, "code": 1, "defaultValue": 1})
        if fields_default_values:
            return {field["code"]: field["defaultValue"] for field in fields_default_values}

    def get_worksheet_metadata(self, worksheet_id):
        """Fetches a document from sheet_metadata collection based on worksheet Id"""

        sheet_metadata = mongo.db.worksheet_metadata

        worksheet = sheet_metadata.find_one({"worksheetId": worksheet_id}, projection=["sheetPath", "totalExposures"])

        return worksheet

    def delete_metadata(self, params, modifications, mapped_df, result_df):
        """Edits metadata documents after adding or deleting an exposure"""

        sheet_metadata = mongo.db.sheet_metadata
        job_results = mongo.db.data_check_jobs

        if modifications["is_all"]:
            sheet_metadata.delete_one({"worksheetId": params["worksheet_id"]})
            job_results.delete_one({"jobId": params["job_id"]})
        else:
            exist_sheet = sheet_metadata.find_one({"worksheetId": params["worksheet_id"]})
            if exist_sheet["totalExposures"] < 2:
                sheet_metadata.delete_one({"worksheetId": params["worksheet_id"]})
                job_results.delete_one({"jobId": params["job_id"]})
            else:
                total_exposures = exist_sheet["totalExposures"] - len(modifications["indices"])
                sheet_metadata.update_one(
                    {"_id": exist_sheet["_id"]},
                    {'$set': {"totalExposures": total_exposures}
                     }, upsert=False)

                exist_job = job_results.find_one({"jobId": params["job_id"]})
                exist_job["jobResult"] = []
                modified_columns = {column: 1 for column in result_df.columns.values}
                job_result_document = JobResultDocument()
                cleansing_service.update_data_check_metadata(exist_job, result_df, modified_columns,
                                                             modifications_result_df={}, indices=None)
                exist_job["totalTIV"] = cleansing_service.calculate_tiv(mapped_df)
                exist_job["totalExposures"] = total_exposures
                job_result_document.save_check_job(exist_job)

    def get_calculated_field_ref(self, df, expression):
        """Fetches reference data for calculated values operands"""

        collection = eval(f"mongo.db.{expression['collection'].upper()}")

        reference_data = collection.find(
            {expression["conditions"][0]["field"]: {"$in": df[expression["conditions"][0]["value"]["code"]].tolist()}},
            {"_id": 0, expression["field"]: 1, expression["conditions"][0]["field"]: 1})
        reference_data = {field[expression["conditions"][0]["field"]]:
                              field[expression["field"]] for field in reference_data}
        return reference_data

    def get_calculated_fields_formula(self, lob_id, condition, mappings):
        """Fetches all calculated field formula from calculated_fields collection"""

        calculated_fields = mongo.db.calculated_fields

        formulas = calculated_fields.find({"lob": lob_id})
        fields_formulas = {}
        for formula in formulas:
            formula_condition = formula.get("conditions")
            field_code = formula.get("code")
            if ((formula_condition == condition) or (formula_condition is None)) and (field_code not in mappings):
                fields_formulas[formula["code"]] = formula["expression"]

        return fields_formulas


class JobResultDocument:

    def get_data_check_job(self, job_id):
        """Fetches a data cleansing document from job_results collection"""

        job_results = mongo.db.data_check_jobs

        return job_results.find_one({"jobId": job_id})

    def save_check_job(self, job):
        """Saves a data cleansing document in job_results collection"""

        job_results = mongo.db.data_check_jobs

        exist_job = self.get_data_check_job(job["jobId"])
        if exist_job:
            _id = exist_job["_id"]
            job_results.update_one(
                {'_id': _id},
                {'$set': {

                    "jobResult": job["jobResult"],
                    "totalErrors": job["totalErrors"],
                    "uniqueErrorLines": job["uniqueErrorLines"],
                }
                }, upsert=False
            )

        else:
            job_results.insert_one(job)

        return {"job_id": job["jobId"]}
