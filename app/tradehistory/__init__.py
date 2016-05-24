from flask import Blueprint

tradehistory = Blueprint('tradehistory', __name__)
from . import views, errors
