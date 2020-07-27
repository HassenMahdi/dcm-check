from flask import request
from flask_restplus import Resource

from ..service.super_dom_service import get_all_super_domains, save_super_domain, delete_super_domain
from ..util.dto import DomainDto, SuperDomainDto

api = SuperDomainDto.api
dto = SuperDomainDto.super_domain


@api.route('/')
class SuperDomains(Resource):
    """
        Domain Resource
    """
    @api.doc('Get All Super Domains')
    @api.marshal_list_with(dto)
    def get(self):
        return get_all_super_domains()

    @api.doc('Create/Update Super Domains')
    @api.response(201, 'Super Domain successfully created/updated.')
    @api.expect(dto, validate=True)
    @api.marshal_with(dto)
    def post(self):
        # get the post data
        post_data = request.json
        return save_super_domain(data=post_data)

    @api.doc('delete super Domains')
    @api.response(201, 'Super Domain successfully deleted.')
    @api.expect(dto, validate=True)
    @api.marshal_with(dto)
    def delete(self):
        # get the post data
        post_data = request.json
        return delete_super_domain(data=post_data)