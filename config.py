import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'adf098098'
    TIMEOUT = 15 * 60
    NUMBER_OF_ROW_PER_PAGE = 41
    NUMBER_OF_TOP_POSITIONS = 8

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True


config = {
    'developement': DevelopmentConfig,
    'testing': TestingConfig,

    'default': DevelopmentConfig
}
