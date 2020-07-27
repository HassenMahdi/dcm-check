#!/usr/bin/python
# -*- coding: utf-8 -*-

import traceback

from flask import request, jsonify
from flask_restx import Resource, Namespace, fields

from  app.main.util import responses as resp
from app.main.util.responses import response_with
from app.main.service.controllers import start_check_job, read_exposures, read_results, read_column
from app.main.util.storage import get_import_path
    
from app.db.Models.checker_documents import JobResultDocument, CheckerDocument

api = Namespace('data check', description='checks')



@api.route('')
class CheckingData(Resource):
    get_request_param = {"filename": "The excel file name", "worksheet": "The name of worksheet",
                         "worksheet_id": "The created worksheet Id", "domain_id": "The domain Id", "domain_name": "The domain name"}
    post_request_body = api.model("CheckingData", {
        "is_all": fields.Boolean(required=True),
        "indices": fields.List(fields.Integer, required=False),
        "content": fields.Raw(required=True),
    })

    @api.doc("Check the data health")
    @api.doc(params=get_request_param)
    @api.expect(post_request_body)
    def post(self):
        if request.method == 'POST':
            try:
                params = {param: request.args.get(param) for param in ["filename", "worksheet", "worksheet_id",
                                                                       "domain_name", "domain_id"]}
                modifications = request.get_json()
                result = start_check_job(params, modifications=modifications)

                return jsonify(result)

            except Exception:

                traceback.print_exc()

                return response_with(resp.SERVER_ERROR_500)


@api.route("/metadata")
class ChecksMetadata(Resource):
    get_req_params = {"job_id": "Job Id returned by the data check endpoint"}

    @api.doc("Returns the data check results metadata")
    @api.doc(params=get_req_params)
    def get(self):
        param = request.args.get("job_id")

        job_result_document = JobResultDocument()
        job_metadata = job_result_document.get_data_check_job(param)
        del job_metadata["_id"]

        return jsonify(job_metadata)

@api.route("/db")
class ChecksMetadatas(Resource):
    get_req_params = {"domain_id": "Job Id returned by the data check endpoint"}

    @api.doc("Returns the data check results metadata")
    @api.doc(params=get_req_params)
    def get(self):
        param = request.args.get("domain_id")

        checker_document = CheckerDocument()

        # TODO: get target fileds by domain and categories
        target_fields = checker_document.get_all_target_fields(param)

        return jsonify(target_fields)

@api.route("/path")
class ChecksMetadatass(Resource):
    get_req_params = {"filename": "The excel file name", "worksheet": "The name of worksheet",
                         "worksheet_id": "The created worksheet Id", "domain_id": "The domain Id"}

    @api.doc("Returns the data check results metadata")
    @api.doc(params=get_req_params)
    def get(self):
        param = {param: request.args.get(param) for param in ["filename", "worksheet", "domain_id", "nrows"]}

        path = get_import_path(param["filename"], param["worksheet"], as_folder=False, create=True)


        return jsonify(path)

@api.route('/results')
class CheckResults(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name",
                      "page": "The page number", "nrows": "Number of rows to preview"}

    @api.doc("Get paginated check results")
    @api.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet", "page", "nrows"]}
        params["nrows"] = 50 if params["nrows"] == "None" else int(params["nrows"])

        check_results = read_results(params)

        return jsonify(check_results)


@api.route('/headers')
class DataGridHeaders(Resource):
    # TODO: change reslut model
    get_req_params = {"domain_id": "domain identifier"}

    @api.doc("Get the dataGrid headers")
    @api.doc(params=get_req_params)
    def get(self):
        domain_id = request.args.get("domain_id")
        checker_document = CheckerDocument()

        headers = checker_document.get_headers(domain_id)

        return jsonify(headers)

 # TODO: change reslut model to eliminate !!!
@api.route('/exposures')
class Exposures(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name", "page": "The page number",
                      "worksheet_id": "The worksheet created Id", "nrows": "Number of rows to preview","domain_id":"domain_id"}

    @api.doc("Get paginated exposures")
    @api.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename","domain_id", "worksheet", "worksheet_id", "page",
                                                               "nrows"]}
        exposures = read_exposures(request, params)

        return jsonify(exposures)

@api.route('/read-column/')
class ColumnReader(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name", "column": "column code",
                      "unique": "Whenever to get unique values (if empty then not unique)"}

    @api.doc("Gets data for specific column")
    @api.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet", "column", "unique"]}

        return read_column(params)














