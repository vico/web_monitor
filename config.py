import os

basedir = os.path.abspath(os.path.dirname(__file__))

import logging
from logging.handlers import RotatingFileHandler, SMTPHandler

class Config:
    def __init__(self):
        pass

    SECRET_KEY = os.environ.get('SECRET_KEY') or 'adf098098'
    NUMBER_OF_ROW_PER_PAGE = 41
    NUMBER_OF_TOP_POSITIONS = 8
    MAIL_SERVER = 'smtp.office365.com'
    MAIL_PORT = 587
    MAIL_USE_TLS = True
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    MAIL_SUBJECT_PREFIX = 'RH'
    MAIL_SENDER = os.environ.get('MAIL_SENDER')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True
    MAIL_DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'data.sqlite')
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True


class TestingConfig(Config):
    TESTING = True


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
                smtp.ehlo() # for tls add this line
                smtp.starttls() # for tls
                smtp.ehlo() # for tls
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
        


class ProductionConfig(Config):

    ANALYTICS_ADMIN = os.environ.get('ANALYTICS_ADMIN')
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'data.sqlite')
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True

    @classmethod
    def init_app(cls, app):
        Config.init_app(app)
        # email errors to the administrators
        credentials = None
        secure = None
        if getattr(cls, 'MAIL_USERNAME', None) is not None:
            credentials = (cls.MAIL_USERNAME, cls.MAIL_PASSWORD)

            if getattr(cls, 'MAIL_USE_TLS', None):
                secure = ()

        mail_handler = TLSSMTPHandler(
                mailhost=(cls.MAIL_SERVER, cls.MAIL_PORT),
                fromaddr=cls.MAIL_SENDER,
                toaddrs=[cls.ANALYTICS_ADMIN],
                subject=cls.MAIL_SUBJECT_PREFIX + ' Application Error',
                credentials=credentials,
                secure=secure)
        mail_handler.setLevel(logging.ERROR)

        print(os.getcwd())
        open('logs/app.log', 'a').close() # create file if not exists
        file_handler = RotatingFileHandler('logs/app.log', maxBytes=1*1024*1024, backupCount=100)
        formatter = logging.Formatter("[%(asctime)s] |  %(levelname)s | {%(pathname)s:%(lineno)d} | %(message)s")
        # Set the level according to whether we're debugging or not
        if app.debug:
            file_handler.setLevel(logging.DEBUG)
        else:
            file_handler.setLevel(logging.WARN)

        # Set the email format
        mail_handler.setFormatter(logging.Formatter('''
        Message type:       %(levelname)s
        Location:           %(pathname)s:%(lineno)d
        Module:             %(module)s
        Function:           %(funcName)s
        Time:               %(asctime)s
        
        Message:
        
        %(message)s
        '''))

        loggers = [app.logger, logging.getLogger('sqlalchemy'), logging.getLogger('werkzeug')]

        for logger in loggers:
            logger.addHandler(file_handler)
            logger.addHandler(mail_handler)


config = {
    'developement': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
