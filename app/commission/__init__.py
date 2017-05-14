from flask import Blueprint

commissions = Blueprint('commissions', __name__)
from . import views