import os

from flask import Flask
from flask_cors import CORS
<<<<<<< HEAD
=======
from flask_script import Manager
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818

from config import config_by_name
from api import check_bp
from database.connectors import mongo


def create_app(config_name):
    """Creates the flask app and initialize its component"""

    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])
    CORS(app)
    mongo.init_app(app)
    app.register_blueprint(check_bp)

    return app

<<<<<<< HEAD
=======
app = create_app(os.getenv('DEPLOY_ENV') or 'dev')

app.app_context().push()

manager = Manager(app)


@manager.command
def run():
    app.run(threaded=True)
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818

app = create_app(os.getenv('DEPLOY_ENV') or 'dev')

if __name__ == '__main__':
<<<<<<< HEAD
    app.run(port=5000, debug=True)

=======
    manager.run()
>>>>>>> f40dd9415bab8ee43a8ad771c2fcbe515cc2e818
