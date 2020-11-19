# -*- encoding: utf8 -*-
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_login import LoginManager
from flask_mail import Mail
from flask_moment import Moment
from flask_sqlalchemy import SQLAlchemy

from config import config
from .diff_match_patch import diff_match_patch, patch_obj

mail = Mail()
moment = Moment()
bootstrap = Bootstrap()
db = SQLAlchemy()
login_manager = LoginManager()


# service = Service('/Users/cuong/localdev/python/flask/web_monitor/chromedriver')
# service.start()


def create_app(config_name='development'):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    mail.init_app(app)
    # scheduler.init_app(app)
    db.init_app(app)
    moment.init_app(app)
    bootstrap.init_app(app)
    # https://stackoverflow.com/questions/40117324/querying-model-in-flask-apscheduler-job-raises-app-context-runtimeerror (first answer 1 vote)
    db.app = app

    # scheduler.start()
    if not app.debug and not app.testing and not app.config['SSL_DISABLE']:
        # from flask_sslify import SSLify
        # sslify = SSLify(app)
        pass

    return app


def decorate_app(app):
    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)  # default url_prefix == '/'

    return app
