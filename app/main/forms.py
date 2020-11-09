from flask_wtf import FlaskForm
from wtforms import SubmitField, StringField
from wtforms.fields.html5 import URLField
from wtforms.validators import DataRequired


class PageForm(FlaskForm):
    url = URLField(validators=[DataRequired()], render_kw={'size': 40, 'placeholder': 'https://example.com'})
    cron_schedule = StringField('Cron schedule: ', render_kw={'placeholder': '*/1 * * * *'})
    xpath = StringField('XPath: ')
    submit = SubmitField('Submit')
