from flask import Flask
# from flask_mail import Mail
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from config import config
import os
from typing import List

# mail = Mail()
db = SQLAlchemy()
login_manager = LoginManager()


def create_app(config_name='development'):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    # mail.init_app(app)
    db.init_app(app)

    if not app.debug and not app.testing and not app.config['SSL_DISABLE']:
        # from flask_sslify import SSLify
        # sslify = SSLify(app)
        pass

    return app


def decorate_app(app):

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)  # default url_prefix == '/'

    return app
