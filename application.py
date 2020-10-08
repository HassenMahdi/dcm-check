import os

from flask import Flask
from flask_cors import CORS
from flask_script import Manager

from config import config_by_name
from api import check_bp
from database.connectors import mongo


app = create_app(os.getenv('APP_ENV') or 'dev')
app.register_blueprint(check_bp)
def create_app(config_name):
    """Creates the flask app and initialize its component"""

    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])
    CORS(app)
    mongo.init_app(app)
    app.register_blueprint(check_bp)

    return app

app = create_app(os.getenv('APP_ENV') or 'dev')

app.app_context().push()

manager = Manager(app)


@manager.command
def run():
    app.run(threaded=True)


if __name__ == '__main__':
    manager.run()
