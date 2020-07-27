from flask_restplus import Namespace, fields


class NullableString(fields.String):
    __schema_type__ = ['string', 'null']
    __schema_example__ = 'nullable string'


class UserDto:
    api = Namespace('user', description='user related operations')
    user = api.model('user', {
        'email': fields.String(required=True, description='user email address'),
        'username': fields.String(required=True, description='user username'),
        'password': fields.String(required=True, description='user password'),
        'public_id': fields.String(description='user Identifier')
    })


class AuthDto:
    api = Namespace('auth', description='authentication related operations')
    user_auth = api.model('auth_details', {
        'email': fields.String(required=True, description='The email address'),
        'password': fields.String(required=True, description='The user password '),
    })


class DomainDto:
    api = Namespace('domain', description='us-er related operations')
    domain = api.model('domain', {
        'name': fields.String(required=True, description='user email address'),
        'identifier': NullableString(description='user username'),
        'description': NullableString(description='user username'),
        'id': NullableString(description='user password'),
        'created_on': fields.DateTime(description='user Identifier'),
        'super_domain_id': fields.String(required=True, description='Super Domain Id')
    })


class SuperDomainDto:
    api = Namespace('super-domain', description='super domain related operations')
    super_domain = api.model('super-domain', {
        'name': fields.String(required=True, description='user email address'),
        'identifier': NullableString(description='user username'),
        'description': NullableString(description='user username'),
        'id': NullableString(description='user password'),
        'created_on': fields.DateTime(description='user Identifier')
    })


class FieldsDto:
    api = Namespace('domain', description='domain specific fields')
    field = api.model('field', {
        'id': NullableString(description='user password'),
        'name': fields.String(description='user email address'),
        'label': fields.String(description='user username'),
        'description': NullableString(description='user username'),
        # 'category': fields.String(description='user username'),
        'type': fields.String(description='user username'),
        'mandatory': fields.Boolean(description='user username'),
        'editable': fields.Boolean(description='user username'),
        'created_on': fields.DateTime(description='user Identifier'),
        'rules': fields.List(fields.Raw, description='list of rules')
    })



class ChecksDto:
    api = Namespace('domain', description='domain specific checks')

    check_param = api.model('check param', {
        'name': fields.String,
        'type': fields.String,
        'label': fields.String,
        'options': fields.List(fields.Raw)
    })

    check = api.model('checks', {
        'id': NullableString(description='user password'),
        'name': fields.String(description='user email address'),
        'description': NullableString(description='user username'),
        'category': fields.String(description='user username'),
        'parameters': fields.List(fields.Nested(check_param))
    })
class JobDto:
    api = Namespace('job', description='domain specific fields')
    job_id = api.model('job', {
        'job_id': NullableString(description='job id'),
        'jobResult': fields.List(fields.Raw),
        'totalErrors': NullableString(description='user username'),
        'uniqueErrorLines': fields.String(description='user username'),
        'filename': fields.String(description='user email address'),
        'worksheet_id': NullableString(description='user username'),
        'domain_id': fields.String(description='user username'),
        })