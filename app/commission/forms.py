# -*- coding: utf8 -*-
from flask_wtf import Form
from wtforms import StringField, SelectField, SubmitField
from wtforms.fields import DateField
from wtforms.validators import DataRequired


class RankSearchForm(Form):
    year = DateField('Year', validators=[DataRequired], format='%Y')
    submit = SubmitField('Search')

