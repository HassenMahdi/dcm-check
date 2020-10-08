#!/usr/bin/python
# -*- coding: utf-8 -*-

import os   
 

basedir = os.path.abspath(os.path.dirname(__file__))    


class Config:   
    
    SECRET_KEY = os.getenv('SECRET_KEY', 'my_precious_secret_key')  
    DEBUG = False   
    UPLOAD_FOLDER = "/scor-data/"   
    MONGO_URI = "mongodb://dcm-consmos" \
                ":pUQRAZMYnTiYikWTxjcq7zQch27litMHCSJnHOu9XCssYxBqVRWmMpd8sSnd0G7w66dQ7GMS4UK8iAvOsoBGtw==@dcm" \
                "-consmos.mongo.cosmos.azure.com:10255/dcm?ssl=true&replicaSet=globaldb&retrywrites=false" \
                "&maxIdleTimeMS=120000&appName=@dcm-consmos@"


class DevelopmentConfig(Config):    

    DEBUG = True    
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_main.db') 
    SQLALCHEMY_TRACK_MODIFICATIONS = False  


class TestingConfig(Config):    
    
    DEBUG = True    
    TESTING = True  
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_test.db') 
    PRESERVE_CONTEXT_ON_EXCEPTION = False   
    SQLALCHEMY_TRACK_MODIFICATIONS = False  


class ProductionConfig(Config): 
    
    DEBUG = False   
    MONGO_URI = os.getenv('MONGO_URI')


config_by_name = dict(  
    dev=DevelopmentConfig,  
    test=TestingConfig, 
    prod=ProductionConfig   
)   

key = Config.SECRET_KEY
