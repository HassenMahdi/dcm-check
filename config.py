import os
import urllib

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    """Base class for config"""

    SECRET_KEY = os.getenv('SECRET_KEY', 'my_precious_secret_key')
    DEBUG = False


class DevelopmentConfig(Config):
    """Dev config settings"""
    params = urllib.parse.quote_plus(
        'DRIVER={SQL Server};SERVER=HP-PC;DATABASE=dcm_db;Trusted_Connection=yes;')
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = f"mssql+pyodbc:///?odbc_connect={params}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    UPLOAD_FOLDER = "C:/uploads/"
    #UPLOAD_FOLDER = "/scor-data/"
    MONGO_URI = "mongodb://localhost:27017/dcm_db"
    #MONGO_URI = 'mongodb://dcm-consmos:pUQRAZMYnTiYikWTxjcq7zQch27litMHCSJnHOu9XCssYxBqVRWmMpd8sSnd0G7w66dQ7GMS4UK8iAvOsoBGtw==@dcm-consmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@dcm-consmos@'
    CORS_HEADERS = 'Content-Type'


class TestingConfig(Config):
    """Test config settings"""

    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_test.db')
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    """Prod config settings"""

    DEBUG = False


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY