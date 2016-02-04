from flask import Flask
from config import config
from cache import cache


def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    cache.init_app(app, config={'CACHE_TYPE': 'simple'})

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app
