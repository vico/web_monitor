from flask_wtf import FlaskForm
from wtforms import SubmitField, StringField
from wtforms.fields.html5 import URLField
from wtforms.validators import DataRequired, ValidationError


class CronValidator(object):
    def __init__(self, message=None):
        if not message:
            message = 'Cron expression is not correct!'
        self.message = message

    def __call__(self, form, field):
        values = field.data.split()
        if len(values) != 5:
            message = 'Wrong number of fields; got {}, expected 5'.format(len(values))
            raise ValueError(message)

        day_of_week = values[4]
        if '-' in day_of_week:
            start, end = [int(x) for x in day_of_week.split('-')]
            if not (0 <= start <= 6 and 0 <= end <= 6 and start <= end):
                message = 'Day of week must be within 0 and 6.'
                raise ValueError(message)


cron = CronValidator


class PageForm(FlaskForm):
    url = URLField(validators=[DataRequired()], render_kw={'size': 40, 'placeholder': 'https://example.com'})
    cron_schedule = StringField('Cron schedule: ', [cron()], render_kw={'placeholder': '*/1 * * * *'})
    xpath = StringField('XPath: ', render_kw={'placeholder': '/html/body/article/div/div[3]/div/div[4]/div/div[1]/table'})
    keyword = StringField('Keyword: ', render_kw={'placeholder': '月次'})
    submit = SubmitField('Submit')

