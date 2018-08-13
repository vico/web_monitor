from flask import render_template, redirect, url_for, request, flash
from flask.ext.login import login_user, logout_user, current_user, login_required
from . import front


@front.route('/', methods=['GET'])
# @login_required
def index():
    return render_template('front/index.html')
