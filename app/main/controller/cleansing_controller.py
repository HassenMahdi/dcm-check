from flask import request
from flask_restplus import Resource

from app.main.service.auth_helper import Auth
from ..service.doms_service import get_all_domains, save_domain
from ..service.fields_service import get_all_fields, save_field, delete_field, fields_from_file
from ..util.dto import DomainDto, FieldsDto

api = FieldsDto.api
dto = FieldsDto.field


@api.route('/<domain_id>/fields')
@api.param('domain_id', 'Domain ID')
class Fields(Resource):
    """
        Domain Resource
    """
    @api.doc('Get All Domains')
    @api.marshal_list_with(dto)
    def get(self, domain_id):
        return get_all_fields(domain_id)

    @api.doc('Create/Update Domain Fields')
    @api.response(201, 'Field successfully created/updated.')
    @api.expect(dto, validate=True)
    @api.marshal_with(dto)
    def post(self, domain_id):
        # get the post data
        post_data = request.json
        return save_field(data=post_data, domain_id = domain_id)

    @api.doc('delete field')
    @api.response(201, 'field successfully deleted.')
    @api.expect(dto, validate=True)
    @api.marshal_with(dto)
    def delete(self, domain_id):
        # get the post data
        post_data = request.json
        return delete_field(data=post_data, domain_id = domain_id)


@api.route('/<domain_id>/fields/file')
@api.param('domain_id', 'Domain ID')
class FieldsFile(Resource):

    @api.doc('Create/Update Domain Fields')
    def post(self, domain_id):
        # get the file data
        return fields_from_file(request.files['file'], domain_id)