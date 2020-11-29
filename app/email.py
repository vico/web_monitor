from threading import Thread
from flask import current_app, render_template
from flask_mail import Message

from . import mail


def send_async_email(app, msg):
    with app.app_context():
        mail.send(msg)


def send_email(to, subject, template='emails/notification', image_path=None, **kwargs):
    app = current_app._get_current_object()
    msg = Message(app.config['MAIL_SUBJECT_PREFIX'] + ' ' + subject,
                  sender=app.config['MAIL_SENDER'], recipients=[to])
    msg.body = render_template(template + '.txt', **kwargs)
    msg.html = render_template(template + '.html', **kwargs)
    if image_path:
        with app.open_resource(image_path) as fp:
            import imghdr
            msg.attach(image_path, "image/{}".format(imghdr.what(image_path)), fp.read(),
                       'inline', headers=[['Content-ID', '<DiffImage>'], ])
    thr = Thread(target=send_async_email, args=[app, msg])
    thr.start()
    return thr


def send_multiple_emails(recipients, subject, template='emails/notification', image_path=None, **kwargs):
    for to in recipients:
        send_email(to, subject, template, image_path, **kwargs)
