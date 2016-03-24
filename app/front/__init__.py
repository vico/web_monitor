from flask import Blueprint

front = Blueprint('front', __name__)

from . import views