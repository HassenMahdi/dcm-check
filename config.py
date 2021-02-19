#!/usr/bin/python
# -*- coding: utf-8 -*-

import os   
 

basedir = os.path.abspath(os.path.dirname(__file__))    


class Config:   
    
    SECRET_KEY = os.getenv('SECRET_KEY', 'my_precious_secret_key')  
    DEBUG = True
    UPLOAD_FOLDER = "/scor-data/"
    MONGO_URI = os.getenv("MONGO_URI")
    # MONGO_URI = "mongodb://root:Bxia!2020DaaTa1920CAvlmd@a4ec5441fc63a4fefbc97353d13465d2-1236515762.eu-west-3.elb.amazonaws.com:27017/dcm?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false&retryWrites=false"
    ASA_URI = "BlobEndpoint=https://devdcmstorage.blob.core.windows.net/;" \
              "QueueEndpoint=https://devdcmstorage.queue.core.windows.net/;" \
              "FileEndpoint=https://devdcmstorage.file.core.windows.net/;" \
              "TableEndpoint=https://devdcmstorage.table.core.windows.net/;" \
              "SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-07-16T07:57:54Z&st=2020-07-15T23:57:54Z&spr=https&sig=4cDoQPv%2Ba%2FQyBEFcr2pVojyMj4vgsm%2Fld6l9TPveQH0%3D"


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



config_by_name = dict(  
    dev=DevelopmentConfig,  
    test=TestingConfig, 
    prod=ProductionConfig   
)   

key = Config.SECRET_KEY
