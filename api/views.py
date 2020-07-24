#!/usr/bin/python
# -*- coding: utf-8 -*-

import traceback

from flask import request, jsonify
from flask_restx import Resource, Namespace, fields

from api.utils import responses as resp
from api.utils.responses import response_with
from api.controllers import start_check_job, read_exposures, read_results, delete_exposure, read_map_infos, read_column, \
    create_chart
from api.utils.storage import get_import_path
from database.checker_documents import JobResultDocument, CheckerDocument
from database.connectors import mongo
from database.reference_documents import CurrenciesDocument, ConstructionsDocument, OccupancyDocument, GeoScopeDocument


check_namespace = Namespace("data check")
references_namespace = Namespace("Reference Data")


@check_namespace.route('')
class CheckingData(Resource):
    get_request_param = {"filename": "The excel file name", "worksheet": "The name of worksheet",
                         "worksheet_id": "The created worksheet Id", "domain_id": "The domain Id", "domain_name": "The domain name"}
    post_request_body = check_namespace.model("CheckingData", {
        "is_all": fields.Boolean(required=True),
        "indices": fields.List(fields.Integer, required=False),
        "content": fields.Raw(required=True),
    })

    @check_namespace.doc("Check the data health")
    @check_namespace.doc(params=get_request_param)
    @check_namespace.expect(post_request_body)
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


@check_namespace.route("/metadata")
class ChecksMetadata(Resource):
    get_req_params = {"job_id": "Job Id returned by the data check endpoint"}

    @check_namespace.doc("Returns the data check results metadata")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        param = request.args.get("job_id")

        job_result_document = JobResultDocument()
        job_metadata = job_result_document.get_data_check_job(param)
        del job_metadata["_id"]

        return jsonify(job_metadata)

@check_namespace.route("/db")
class ChecksMetadatas(Resource):
    get_req_params = {"domain_id": "Job Id returned by the data check endpoint"}

    @check_namespace.doc("Returns the data check results metadata")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        param = request.args.get("domain_id")

        checker_document = CheckerDocument()

        # TODO: get target fileds by domain and categories
        target_fields = checker_document.get_all_target_fields(param)

        return jsonify(target_fields)
    
@check_namespace.route("/path")
class ChecksMetadatass(Resource):
    get_req_params = {"filename": "The excel file name", "worksheet": "The name of worksheet",
                         "worksheet_id": "The created worksheet Id", "domain_id": "The domain Id"}

    @check_namespace.doc("Returns the data check results metadata")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        param = {param: request.args.get(param) for param in ["filename", "worksheet", "domain_id", "nrows"]}

        path = get_import_path(param["filename"], param["worksheet"], as_folder=False, create=True)


        return jsonify(path)

@check_namespace.route('/results')
class CheckResults(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name",
                      "page": "The page number", "nrows": "Number of rows to preview"}

    @check_namespace.doc("Get paginated check results")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet", "page", "nrows"]}
        params["nrows"] = 50 if params["nrows"] == "None" else int(params["nrows"])

        check_results = read_results(params)

        return jsonify(check_results)


@check_namespace.route('/headers')
class DataGridHeaders(Resource):
    # TODO: change reslut model
    get_req_params = {"domain_id": "domain identifier"}

    @check_namespace.doc("Get the dataGrid headers")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        domain_id = request.args.get("domain_id")
        checker_document = CheckerDocument()

        headers = checker_document.get_headers(domain_id)

        return jsonify(headers)

 # TODO: change reslut model to eliminate !!!
@check_namespace.route('/exposures')
class Exposures(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name", "page": "The page number",
                      "worksheet_id": "The worksheet created Id", "nrows": "Number of rows to preview","domain_id":"domain_id"}

    @check_namespace.doc("Get paginated exposures")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename","domain_id", "worksheet", "worksheet_id", "page",
                                                               "nrows"]}
        exposures = read_exposures(request, params)

        return jsonify(exposures)

 # TODO: change reslut model to eliminate !!!
@check_namespace.route('/delete')
class ExpouresHandler(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name",
                      "worksheet_id": "The worksheet created Id", "job_id": "Job Id returned by the data check"}
    post_request_body = check_namespace.model("ExpouresHandler", {
        "is_all": fields.Boolean(required=True),
        "indices": fields.List(fields.Integer, required=False),
    })

    @check_namespace.doc("Remove an exposure")
    @check_namespace.doc(params=get_req_params)
    @check_namespace.expect(post_request_body)
    def post(self):
        try:
            params = {param: request.args.get(param) for param in ["filename", "worksheet", "worksheet_id",
                                                                   "job_id"]}
            modifications = request.get_json()
            delete_exposure(params, modifications)
        except Exception:
            traceback.print_exc()
            return response_with(resp.SERVER_ERROR_500)

 # TODO: change reslut model to eliminate !!!
@check_namespace.route('/map')
class Map(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name"}

    @check_namespace.doc("Gets the longitude and latitude value for all exposures")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet"]}

        return read_map_infos(params)

 # TODO: change reslut model
@check_namespace.route('/read-column/')
class ColumnReader(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name", "column": "column code",
                      "unique": "Whenever to get unique values (if empty then not unique)"}

    @check_namespace.doc("Gets data for specific column")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet", "column", "unique"]}

        return read_column(params)


@check_namespace.route('/dashboard')
class Dashboard(Resource):
    get_req_params = {"filename": "Excel file name", "worksheet": "Worksheet name",
                      "worksheet_id": "The worksheet created Id", "image": "Function value",
                      "argument": "Function variable: x axis", "values": "Whenever to use exact values or percentage"}

    @check_namespace.doc("Gets chart's data")
    @check_namespace.doc(params=get_req_params)
    def get(self):
        params = {param: request.args.get(param) for param in ["filename", "worksheet", "worksheet_id",
                                                               "image", "argument", "values"]}

        return create_chart(params)


@references_namespace.route('/currencies')
class Currencies(Resource):

    @references_namespace.doc("Get currencies")
    def get(self):
        currencies_document = CurrenciesDocument()

        return currencies_document.get_currencies()


@references_namespace.route('/constructions/classes')
class ConstructionClasses(Resource):

    @references_namespace.doc("Get construction classes")
    def get(self):
        constructions_document = ConstructionsDocument()

        return constructions_document.get_construction_classes()


@references_namespace.route('/constructions/codes')
class ConstructionCodes(Resource):
    get_req_params = {"class": "Construction class name"}

    @references_namespace.doc("Get construction codes")
    @references_namespace.doc(params=get_req_params)
    def get(self):
        class_name = request.args.get("class", None)
        constructions_document = ConstructionsDocument()

        if class_name is None:
            return constructions_document.get_all_construction_codes()

        return constructions_document.get_construction_codes(class_name)


@references_namespace.route('/occupancies/classes')
class OccupancyClasses(Resource):

    @references_namespace.doc("Get occupancy classes")
    def get(self):
        occupancy_document = OccupancyDocument()

        return occupancy_document.get_occupancy_classes()


@references_namespace.route('/occupancies/subclasses')
class OccupancySubClasses(Resource):
    get_req_params = {"class": "Occupancy class name"}

    @references_namespace.doc("Get occupancy subclasses")
    @references_namespace.doc(params=get_req_params)
    def get(self):
        class_name = request.args.get("class", None)
        occupancy_document = OccupancyDocument()

        if class_name is None:
            return occupancy_document.get_all_occupancy_subclasses()

        return occupancy_document.get_occupancy_subclasses(class_name)


@references_namespace.route('/occupancies/codes')
class OccupancyCodes(Resource):
    get_req_params = {"class": "Occupancy class name", "subclass": "Occupancy subclass name"}

    @references_namespace.doc("Get occupancy codes")
    @references_namespace.doc(params=get_req_params)
    def get(self):
        class_name = request.args.get("class", None)
        sub_class_name = request.args.get("subclass", None)
        occupancy_document = OccupancyDocument()

        if sub_class_name is None and class_name is None:
            return occupancy_document.get_all_occupancy_codes()

        return occupancy_document.get_occupancy_codes(class_name=class_name, subclass_name=sub_class_name)


@references_namespace.route('/countries')
class Countries(Resource):

    @references_namespace.doc("Get countries")
    def get(self):
        geo_scope_document = GeoScopeDocument()

        return geo_scope_document.get_countries()


@references_namespace.route('/states/<country>')
class States(Resource):

    @references_namespace.doc("Get states of a given country")
    def get(self, country):
        geo_scope_document = GeoScopeDocument()

        return geo_scope_document.get_states(country)


@references_namespace.route('/counties/<state>')
class Counties(Resource):

    @references_namespace.doc("Get counties of a given state")
    def get(self, state):
        geo_scope_document = GeoScopeDocument()

        return geo_scope_document.get_counties(state)
