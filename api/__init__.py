from flask_restx import Api
from flask import Blueprint

from api.views import check_namespace, references_namespace


check_bp = Blueprint('check_api', __name__)

check_api = Api(check_bp,
                title='Checking data',
                version='1.0',
                description='API for checking imported data on DCM'
                )

check_api.add_namespace(check_namespace, path='/check')
check_api.add_namespace(references_namespace, path='/references')
