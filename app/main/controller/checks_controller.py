from flask_restplus import Resource

from ..service.check_service import get_domain_checks
from ..util.dto import ChecksDto

api = ChecksDto.api
user_auth = ChecksDto.check


@api.route('/<domain_id>/checks')
@api.param('domain_id', 'Domain ID')
class Fields(Resource):
    """
        Domain Resource
    """
    @api.doc('Get All Domains')
    @api.marshal_list_with(user_auth)
    def get(self, domain_id):
        return get_domain_checks(domain_id)