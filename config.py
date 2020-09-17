#!/usr/bin/python
# -*- coding: utf-8 -*-

import os


basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    """Base class for config"""

    SECRET_KEY = os.getenv("SECRET_KEY", "my_precious_secret_key")
    DEBUG = False


class DevelopmentConfig(Config):
    """Dev config settings"""

    DEBUG = True
    UPLOAD_FOLDER = "/home/dexter/uploads/"
    MONGO_URI = "mongodb://dcm-consmos" \
                ":pUQRAZMYnTiYikWTxjcq7zQch27litMHCSJnHOu9XCssYxBqVRWmMpd8sSnd0G7w66dQ7GMS4UK8iAvOsoBGtw==@dcm" \
                "-consmos.mongo.cosmos.azure.com:10255/dcm?ssl=true&replicaSet=globaldb&retrywrites=false" \
                "&maxIdleTimeMS=120000&appName=@dcm-consmos@ "
    CORS_HEADERS = "Content-Type"


class TestingConfig(Config):
    """Test config settings"""

    DEBUG = True
    TESTING = True


class ProductionConfig(Config):
    """Prod config settings"""

    DEBUG = False


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY