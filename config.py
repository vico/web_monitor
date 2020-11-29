# -*- encoding: utf8 -*-
import logging
import os
from logging.handlers import RotatingFileHandler, SMTPHandler
from pathlib import Path

from dotenv import load_dotenv

env_path = Path(os.path.abspath(os.path.dirname(__file__))) / '.flaskenv'
load_dotenv(dotenv_path=env_path)


class TLSSMTPHandler(SMTPHandler):
    def emit(self, record):
        """
        Emit a record
        Format the record and send it to specified addresses.
        http://mynthon.net/howto/-/python/python%20-%20logging.SMTPHandler-how-to-use-gmail-smtp-server.txt
        http://stackoverflow.com/questions/36937461/how-can-i-send-an-email-using-python-loggings-smtphandler-and-ssl
        """
        try:
            import smtplib
            import string
            try:
                from email.utils import formatdate
            except ImportError:
                formatdate = self.date_time
            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                string.join(self.toaddrs, ","),
                self.getSubject(record),
                formatdate(), msg)

            if self.username:
                smtp.ehlo()  # for tls add this line
                smtp.starttls()  # for tls
                smtp.ehlo()  # for tls
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class Config:

    def __init__(self):  # to satisfy PEP-8
        pass

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.environ.get('SECRET_KEY', 'HtrFLAVDdlkjlsdjgoirem4ETLFWnKxcg3XNVCroad}cgNFKxz')
    ALLOWED_EXTENSIONS = set(os.environ.get('ALLOWED_EXTENSIONS', '').split(','))
    WEB_MONITOR_RECORDS_PER_PAGE = 20
    CHROME_DRIVER = os.environ.get('CHROME_DRIVER')
    CHROME_OPTIONS = os.environ.get('CHROME_OPTIONS', '--headless')
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'data.sqlite')
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(os.environ.get('DB_USER'),
                                                                                os.environ.get('DB_PASSWORD'),
                                                                                os.environ.get('DB_SERVER'),
                                                                                os.environ.get('DB_SCHEMA'))
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True

    # SSL_DISABLE = False
    # SQLALCHEMY_COMMIT_ON_TEARDOWN = False  # when there is Integrity error the server stop
    # SQLALCHEMY_RECORD_QUERIES = True
    # SQLALCHEMY_TRACK_MODIFICATIONS = True
    MAIL_SERVER = os.environ.get('MAIL_SERVER')
    MAIL_PORT = 587
    MAIL_USE_TLS = True
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_SENDER = os.environ.get('MAIL_SENDER')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    MAIL_RECIPIENT = os.environ.get('MAIL_RECIPIENT')
    MAIL_SUBJECT_PREFIX = os.environ.get('MAIL_SUBJECT_PREFIX')
    MAIL_DEBUG = False  # default set to app.debug
    WEB_MONITOR_ADMIN = 'tranvinhcuong@gmail.com'
    SENTRY_URL = os.environ.get('SENTRY_URL')

    # CELERY_BROKER_URL = 'redis://localhost:6379/0'
    # CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
    # CELERY_ACCEPT_CONTENT = ['application/x-python-serialize']
    # CELERY_TASK_SERIALIZER = 'pickle'
    # accept_content = ['pickle', 'application/x-python-serialize']
    # result_accept_content = ['application/x-python-serialize', 'pickle']

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
        if getattr(cls, 'MAIL_USERNAME', None) is not None:
            credentials = (cls.MAIL_USERNAME, cls.MAIL_PASSWORD)
            if getattr(cls, 'MAIL_USE_TLS', None):
                secure = ()
        mail_handler = SMTPHandler(
            mailhost=(cls.MAIL_SERVER, cls.MAIL_PORT),
            fromaddr=cls.MAIL_SENDER,
            toaddrs=[cls.WEB_MONITOR_ADMIN],
            subject=cls.MAIL_SUBJECT_PREFIX + ' Application Error',
            credentials=credentials,
            secure=secure)
        mail_handler.setLevel(logging.ERROR)
        app.logger.addHandler(mail_handler)

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
