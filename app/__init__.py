from flask import Flask
from config import config
from cache import cache
from flask.ext.bootstrap import Bootstrap
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager
from flask.ext.mail import Mail
from flask.ext.moment import Moment

db = SQLAlchemy()
login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'auth.login'
mail = Mail()
bootstrap = Bootstrap()
moment = Moment()


def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    cache.init_app(app, config={'CACHE_TYPE': 'simple'})
    with app.app_context():
        cache.clear()

    db.init_app(app)

    login_manager.init_app(app)
    mail.init_app(app)
    bootstrap.init_app(app)
    moment.init_app(app)

    from .auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint, url_prefix='/auth')

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint, url_prefix='/attr')

    from .tradehistory import tradehistory as tradehistory_blueprint
    app.register_blueprint(tradehistory_blueprint, url_prefix='/tradehistory')

    from .wiki import wiki as wiki_blueprint
    app.register_blueprint(wiki_blueprint, url_prefix='/wiki')

    from .front import front as front_blueprint
    app.register_blueprint(front_blueprint, url_prefix='/')

    return app
