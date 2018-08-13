from flask import Blueprint

earnings = Blueprint('earnings', __name__)
from . import views, errors