#!/usr/bin/python
# -*- coding: utf-8 -*-

import traceback

from flask import request, jsonify
from flask_restx import Namespace, Resource, fields

import api.utils.responses as resp 
from api.controllers import start_check_job, read_exposures
from database.checker_document import CheckerDocument
from database.job_result_document import JobResultDocument


check_namespace = Namespace("data check")


@check_namespace.route('')
class CheckingData(Resource):
    
    content = check_namespace.model('Content', {
        'line': fields.Integer(required=True),
        "value": fields.String(required=True)
})
    modification = check_namespace.model("Modification", {
        "column": fields.String(required=True),
        "modifications": fields.Nested(content, required=True)
    })
    post_request_body = check_namespace.model("CheckingData", {
        "job_id": fields.String(required=False),
        "file_id": fields.String(required=True),
        "worksheet_id": fields.String(required=True),
        "mapping_id": fields.String(required=True),
        "domain_id": fields.String(required=True),
        "is_transformed": fields.Boolean(default=False, required=True),
        "modifications": fields.Nested(modification, required=False)
    })

    @check_namespace.doc("Check the data health")
    @check_namespace.expect(post_request_body)
    def post(self):
        if request.method == 'POST':
            try:
                params = {param: request.get_json().get(param) for param in ["job_id", "file_id", "worksheet_id",
                                                                             "mapping_id", "domain_id", "is_transformed"]}                
                modifications = request.get_json().get("modifications") if params["job_id"] else None

                result = start_check_job(params["job_id"], params["file_id"], params["worksheet_id"], 
                                         params["mapping_id"], params["domain_id"], params["is_transformed"],
                                         modifications=modifications)

                return jsonify(result)
            except Exception:
                traceback.print_exc()
                return resp.response_with(resp.SERVER_ERROR_500)


@check_namespace.route('/headers/<domain_id>')
class DataGridHeaders(Resource):

    @check_namespace.doc("Get the dataGrid headers")
    def get(self, domain_id):
        try:
            checker_document = CheckerDocument()
            headers = checker_document.get_headers(domain_id)

            return jsonify(headers)
        except Exception:
            traceback.print_exc()
            return resp.response_with(resp.SERVER_ERROR_500)


@check_namespace.route('/exposures/<file_id>/<worksheet_id>')
class DataPreview(Resource):

    url_request_params = {"page": "The page number", "nrows": "Number of rows to preview"}
    
    sort = check_namespace.model('Sort', {
        "column": fields.String(required=True),
        "order": fields.String(required=True)
    })
    column_filter = check_namespace.model('ColumnFilter', {
        "column": fields.String(required=True),
        "operator": fields.String(required=True),
        "value": fields.String(required=True)
    })
    body_request_params = check_namespace.model("DataPreview", {
        "is_transformed": fields.Boolean(required=True),
        "filter": fields.List(fields.Nested(column_filter, required=True), required=False),
        "sort": fields.Nested(sort, required=False)
    })
    @check_namespace.doc("Get paginated exposures")
    @check_namespace.doc(params=url_request_params)
    @check_namespace.expect(body_request_params)
    def post(self, file_id, worksheet_id):
        try:
            url_params = {param: request.args.get(param) for param in ["page", "nrows"]}
            params = request.get_json()
            exposures = read_exposures(request.base_url, file_id, worksheet_id, url_params, params["is_transformed"], 
                                       params["sort"], params["filter"])

            return jsonify(exposures)
        except Exception:
            traceback.print_exc()
            return resp.response_with(resp.SERVER_ERROR_500)


@check_namespace.route("/metadata/<job_id>")
class ChecksMetadata(Resource):

    @check_namespace.doc("Returns the data check results metadata")
    def get(self, job_id):
        try:
            job_result_document = JobResultDocument()
            job_metadata = job_result_document.get_data_check_job(job_id)
            del job_metadata["_id"]

            return jsonify(job_metadata)
        except Exception:
            traceback.print_exc()
            return resp.response_with(resp.SERVER_ERROR_500)
