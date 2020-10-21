#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from flask_apscheduler import APScheduler
from flask_bootstrap import Bootstrap
from flask_moment import Moment
from flask_script import Shell, Manager

from app import create_app, decorate_app, db
from app.models import Page

app = create_app(os.getenv('FLASK_ENV') or 'default')
app = decorate_app(app)
Bootstrap(app)
Moment(app)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

manager = Manager(app)

COV = None
if os.environ.get('FLASK_COVERAGE'):
    import coverage
    COV = coverage.coverage(branch=True, include='app/*')
    COV.start()


def make_shell_context():
    return dict(app=app, db=db, Page=Page, scheduler=scheduler)


@app.cli.command()
def test(coverage=False):
    """Run the unit tests."""
    if coverage and not os.environ.get('FLASK_COVERAGE'):
        import sys
        os.environ['FLASK_COVERAGE'] = '1'
        os.execvp(sys.executable, [sys.executable] + sys.argv)
    import unittest
    tests = unittest.TestLoader().discover('app/tests')
    unittest.TextTestRunner(verbosity=2).run(tests)
    if COV:
        COV.stop()
        COV.save()
        print('Coverage summary:')
        COV.report()
        basedir = os.path.abspath(os.path.dirname(__file__))
        covdir = os.path.join(basedir, 'tmp/coverage')
        COV.html_report(directory=covdir)
        print('HTML version: file://{}/index.html'.format(covdir))
        COV.erase()


manager.add_command("shell", Shell(make_context=make_shell_context))

if __name__ == '__main__':
    manager.run()

