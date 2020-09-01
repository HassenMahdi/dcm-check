#!/usr/bin/python
# -*- coding: utf-8 -*-

import traceback

from flask import request, jsonify
from flask_restx import Resource, Namespace, fields

from  app.main.util import responses as resp
from app.main.util.responses import response_with
from app.main.service.controllers import start_check_job, read_exposures, read_column

    
from app.db.Models.checker_documents import JobResultDocument, CheckerDocument

api = Namespace('data check', description='checks')



@api.route('')
class CheckingData(Resource):
    get_request_param = {"filename": "The excel file name", "worksheet": "The name of worksheet",
                         "worksheet_id": "The created worksheet Id", "domain_id": "The domain Id", "domain_name": "The domain name",
                         "isTransformed":"isTransformed","mappingId":"mappingId"}
    post_request_body = api.model("modificationData", {
        "columns": fields.List(fields.Raw, required=False),
    })

    @api.doc("Check the data health")
    @api.doc(params=get_request_param)
    @api.expect(post_request_body)
    def post(self):
        if request.method == 'POST':
            try:
                params = {param: request.args.get(param) for param in ["filename", "worksheet", "worksheet_id",
                                                                       "domain_name", "domain_id", "isTransformed","mappingId"]}
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



@api.route('/read-column/')
class ColumnReader(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name", "column": "column code",
                      "unique": "Whenever to get unique values (if empty then not unique)"}

    @api.doc("Gets data for specific column")
    @api.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet", "column", "unique"]}

        return read_column(params)
        
@api.route('/data')
class Exposures(Resource):
    get_request_param ={"filename": "Excel file name", "worksheet": "Worksheet name", "page": "The page number",
                      "worksheet_id": "The worksheet created Id", "nrows": "Number of rows to preview","domain_id":"domain_id",
                        "isTransformed":"isTransformed","mappingId":"mappingId"}

    post_request_body = api.model("CheckingData", {
        "filter": fields.String(description='filter string expression',required=False),
        "sort": fields.List(fields.Raw, required=False),
        #"content": fields.Raw(required=True),
    })
    @api.doc("Get paginated data")
    @api.doc(params=get_request_param)
    @api.expect(post_request_body)
    def post(self):
        if request.method == 'POST':
            try:
                params = {param: request.args.get(param) for param in
                          ["filename", "domain_id", "worksheet", "worksheet_id", "page",
                           "nrows", "isTransformed","mappingId"]}
                filter_sort = request.get_json()

                exposures = read_exposures(request, params,filter_sort)

                return jsonify(exposures)

            except Exception:

                traceback.print_exc()

            return response_with(resp.SERVER_ERROR_500)













