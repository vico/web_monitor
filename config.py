import logging
import os
from logging.handlers import RotatingFileHandler

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

basedir = os.path.abspath(os.path.dirname(__file__))

env_file_name = os.path.join(basedir, '.flaskenv')

if os.path.exists(env_file_name):
    print(f'Importing environment from {env_file_name}...')
    for line in open(env_file_name):
        var = line.strip().split('=')
        if len(var) == 2:
            os.environ[var[0]] = var[1]


class Config:

    def __init__(self):  # to satisfy PEP-8
        pass

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.environ.get('SECRET_KEY', 'HtrFLAVDdlkjlsdjgoirem4ETLFWnKxcg3XNVCroad}cgNFKxz')
    ALLOWED_EXTENSIONS = set(os.environ.get('ALLOWED_EXTENSIONS', '').split(','))
    WEB_MONITOR_RECORDS_PER_PAGE = 20
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'data.sqlite')
    # SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:root@localhost/hkg02p?charset=utf8'
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True

    # SSL_DISABLE = False
    # SQLALCHEMY_COMMIT_ON_TEARDOWN = False  # when there is Integrity error the server stop
    # SQLALCHEMY_RECORD_QUERIES = True
    # SQLALCHEMY_TRACK_MODIFICATIONS = True
    # MAIL_SERVER = os.environ.get('MAIL_SERVER')
    # MAIL_PORT = 587
    # MAIL_USE_TLS = True
    # MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    # MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')

    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(url='sqlite:///' + os.path.join(basedir, 'data.sqlite'))
    }

    SCHEDULER_EXECUTORS = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(3)
    }

    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': False,
        'max_instances': 3
    }

    SCHEDULER_API_ENABLED = True

    @staticmethod
    def init_app(cls, app):
        pass


class DevelopmentConfig(Config):
    FLASK_DEBUG = 1

    # SERVER_NAME = 'localhost:7000'  # for avoiding error in url_for, which generated url without port number
    # DB_HOST = 'localhost'
    # DB_PORT = 12345

    @classmethod
    def init_app(cls, app):
        app.debug = True
        open('development.log', 'a').close()  # create file if not exists
        file_handler = RotatingFileHandler('development.log')
        file_handler.setLevel(logging.DEBUG)
        app.logger.addHandler(file_handler)


class TestingConfig(Config):
    TESTING = True
    WTF_CSRF_ENABLED = False

    @classmethod
    def init_app(cls, app):
        pass


class ProductionConfig(Config):

    SSL_DISABLE = bool(os.environ.get('SSL_DISABLE'))

    @classmethod
    def init_app(cls, app):
        Config.init_app(cls, app)

        # email errors to the administrators
        credentials = None
        secure = None
        # if getattr(cls, 'MAIL_USERNAME', None) is not None:
        #     credentials = (cls.MAIL_USERNAME, cls.MAIL_PASSWORD)
        #     if getattr(cls, 'MAIL_USE_TLS', None):
        #         secure = ()
        # mail_handler = SMTPHandler(
        #     mailhost=(cls.MAIL_SERVER, cls.MAIL_PORT),
        #     fromaddr=cls.FINDAT_MAIL_SENDER,
        #     toaddrs=[cls.FINDAT_ADMIN],
        #     subject=cls.FINDAT_MAIL_SUBJECT_PREFIX + ' Application Error',
        #     credentials=credentials,
        #     secure=secure)
        # mail_handler.setLevel(logging.ERROR)
        # app.logger.addHandler(mail_handler)

        # for debugging
        open('production.log', 'a').close()  # create file if not exists
        file_handler = RotatingFileHandler('production.log')
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)


class UnixConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to syslog
        import logging
        from logging.handlers import SysLogHandler
        syslog_handler = SysLogHandler()
        syslog_handler.setLevel(logging.WARNING)
        app.logger.addHandler(syslog_handler)


config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'unix': UnixConfig,
    'default': DevelopmentConfig
}
