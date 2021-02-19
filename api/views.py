#!/usr/bin/python
# -*- coding: utf-8 -*-

import traceback

from flask import request, jsonify
from flask_restx import Namespace, Resource, fields

import api.utils.responses as resp 
from api.controllers import start_check_job, read_exposures, get_check_modifications
from database.checker_document import CheckerDocument
from database.job_result_document import JobResultDocument


check_namespace = Namespace("data check")


@check_namespace.route('')
class CheckingData(Resource):
    
    content = check_namespace.model('Content', {
        'previous': fields.String(required=True),
        "new": fields.String(required=True)
})
    column_modification = check_namespace.model("ColumnModification", {
        "column": fields.Nested(content, required=True)
    })
    modifications = check_namespace.model("Modifications", {
        "line": fields.Nested(column_modification, required=True)
    })
    post_request_body = check_namespace.model("CheckingData", {
        "job_id": fields.String(required=False),
        "file_id": fields.String(required=True),
        "worksheet_id": fields.String(required=True),
        "mapping_id": fields.String(required=True),
        "domain_id": fields.String(required=True),
        "is_transformed": fields.Boolean(default=False, required=True),
        "user_id": fields.String(required=True),
        "modifications": fields.Nested(modifications, required=False)
    })

    @check_namespace.doc("Check the data health")
    @check_namespace.expect(post_request_body)
    def post(self):
        if request.method == 'POST':
            try:
                params = {param: request.get_json().get(param) for param in ["job_id", "file_id", "worksheet_id",
                                                                             "mapping_id", "domain_id", 
                                                                             "is_transformed", "user_id"]}                
                modifications = request.get_json().get("modifications") if params["job_id"] else None

                result = start_check_job(params["job_id"], params["file_id"], params["worksheet_id"], 
                                         params["mapping_id"], params["domain_id"], params["is_transformed"],
                                         params["user_id"], modifications=modifications)

                return jsonify(result)
            except Exception as exp:
                print(exp)
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
        except Exception as exp:
            print(exp)
            traceback.print_exc()
            return resp.response_with(resp.SERVER_ERROR_500)


@check_namespace.route('/data')
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
    errors_filter = check_namespace.model("ErrorsFilter",{
        "level": fields.String(required=True),
        "column": fields.String(required=True)
        })
    body_request_params = check_namespace.model("DataPreview", {
        "file_id": fields.String(required=True),
        "worksheet_id": fields.String(required=True),
        "is_transformed": fields.Boolean(required=True), 
        "filter": fields.List(fields.Nested(column_filter, required=True), required=False),
        "errors_filter": fields.Nested(errors_filter, required=False),
        "sort": fields.Nested(sort, required=False)
    })

    @check_namespace.doc("Get paginated exposures")
    @check_namespace.doc(params=url_request_params)
    @check_namespace.expect(body_request_params)
    def post(self):
        try:
            url_params = {param: request.args.get(param) for param in ["page", "nrows"]}
            params = request.get_json()
            exposures = read_exposures(request.base_url, params["file_id"], params["worksheet_id"], url_params, 
                                       params["is_transformed"], params["sort"], params["filter"], params["errors_filter"])

            return jsonify(exposures)
        except Exception as exp:
            print(exp)
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
        except Exception as exp:
            print(exp)
            traceback.print_exc()
            return resp.response_with(resp.SERVER_ERROR_500)


@check_namespace.route('/audit-trial')
class CheckModifications(Resource):

    body_request_params = check_namespace.model("AuditTrial", {
        "worksheet_id": fields.String(required=True),
        "domain_id": fields.String(required=True)
    })
    @check_namespace.doc("Get all check modification data")
    @check_namespace.expect(body_request_params)
    def post(self):
        try:
            worksheet_id, domain_id = (request.get_json().get(param) for param in ["worksheet_id", "domain_id"])
            modifications = get_check_modifications(worksheet_id, domain_id)
            return jsonify(modifications)

        except Exception as exp:
            print(exp) 
            traceback.print_exc()
            return resp.response_with(resp.SERVER_ERROR_500)