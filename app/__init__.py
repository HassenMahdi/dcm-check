
from flask import Blueprint
from flask_restx import Api

from .main.controller.user_controller import api as user_ns
from .main.controller.auth_controller import api as auth_ns
from .main.controller.dom_controller  import api as doms_ns
from .main.controller.fields_controller  import api as flds_ns
from .main.controller.checks_controller  import api as chks_ns
from .main.controller.super_dom_controller  import api as super_ns
from .main.controller.views  import api as check_ns


check_bp = Blueprint('check_api', __name__)

check_api = Api(check_bp,
                title='Checking data',
                version='1.0',
                description='API for checking imported data on DCM'
                )

check_api.add_namespace(check_ns, path='/check')


