# -*- encoding: utf8 -*-
"""
This is an example showing how to make the scheduler into a remotely accessible service.
It uses RPyC to set up a service through which the scheduler can be made to add, modify and remove
jobs.

To run, first install RPyC using pip. Then change the working directory to the ``rpc`` directory
and run it with ``python -m server``.
"""

import hashlib
import logging
import os
from datetime import datetime

import polling2
import rpyc
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pymysql import IntegrityError
from pytz import timezone
from rpyc.utils.server import ThreadedServer
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.options import Options

from app import diff_match_patch, create_app, decorate_app
from app.email import send_multiple_emails
from app.models import Page
from config import Config
from db import db_session

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

app = create_app(os.getenv('FLASK_ENV') or 'default')
app = decorate_app(app)


def fetch(page_id):
    try:
        page = db_session.query(Page).filter(Page.id == page_id).one()  # get fresh page object from db
        chrome_options = Options()
        for option in Config.CHROME_OPTIONS.split(','):
            chrome_options.add_argument(option)
        # driver = webdriver.Remote(service.service_url)
        driver = webdriver.Chrome(Config.CHROME_DRIVER, options=chrome_options)
        driver.implicitly_wait(10)  # seconds
        driver.get(page.url)
        # time.sleep(5)  # Let the user actually see something!
        try:
            target = polling2.poll(lambda: driver.find_element_by_xpath(page.xpath), step=0.5, timeout=10)
            target_text = target.get_property('outerHTML')

            md5sum = hashlib.md5(target_text.encode('utf-8')).hexdigest()
            if page and page.md5sum is not None and (page.md5sum != md5sum):
                # update new md5sum
                dmp = diff_match_patch()
                diffs = dmp.diff_main(page.text, target_text)
                page.md5sum = md5sum
                page.text = target_text
                dmp.diff_cleanupSemantic(diffs)
                diff_html = dmp.diff_prettyHtml(diffs)
                page.diff = diff_html
                page.updated_time = datetime.utcnow()
                db_session.add(page)
                try:
                    db_session.commit()
                except IntegrityError:
                    db_session.rollback()
                    raise
                # notify diff
                if page.keyword in target_text:  # only notify if there is keyword in response
                    with app.app_context():
                        send_multiple_emails(Config.MAIL_RECIPIENT.split(','),
                                             '{} updated'.format(page.url),
                                             diff=diff_html, page=page)
            driver.quit()
            page.last_check_time = datetime.utcnow()
            page.text = target_text
            page.md5sum = md5sum
            db_session.add(page)
            try:
                db_session.commit()
            except IntegrityError:
                db_session.rollback()
                raise
        except (NoSuchElementException, WebDriverException) as e:
            if driver:
                driver.quit()
            send_multiple_emails(Config.MAIL_RECIPIENT.split(','),
                                 '{} ERROR'.format(page.url),
                                 template='emails/error',
                                 exception=str(e), page=page)

    except Exception as e:
        db_session.rollback()
        send_multiple_emails(Config.MAIL_RECIPIENT.split(','),
                             '{} ERROR'.format(page_id),
                             template='emails/error_db',
                             exception=str(e))
        raise


class SchedulerService(rpyc.Service):
    def exposed_add_job(self, func, *args, cron=None, **kwargs):
        # return scheduler.add_job(func, *args, **kwargs)
        return scheduler.add_job(func, *args, trigger=CronTrigger.from_crontab(cron), **kwargs)

    # def exposed_modify_job(self, job_id, jobstore=None, **changes):
    #     return scheduler.modify_job(job_id, jobstore, **changes)

    def exposed_reschedule_job(self, job_id, cron=None, **trigger_args):
        return scheduler.reschedule_job(job_id, trigger=CronTrigger.from_crontab(cron), **trigger_args)

    def exposed_pause_job(self, job_id, jobstore=None):
        return scheduler.pause_job(job_id, jobstore)

    def exposed_resume_job(self, job_id, jobstore=None):
        return scheduler.resume_job(job_id, jobstore)

    def exposed_remove_job(self, job_id, jobstore=None):
        scheduler.remove_job(job_id, jobstore)

    def exposed_get_job(self, job_id):
        return scheduler.get_job(job_id)

    def exposed_get_jobs(self, jobstore=None):
        return scheduler.get_jobs(jobstore)


if __name__ == '__main__':

    tokyo_timezone = timezone('Asia/Tokyo')

    jobstores = {
        'default': SQLAlchemyJobStore(url=Config.SQLALCHEMY_DATABASE_URI, engine_options={'pool_pre_ping': True})
    }

    executors = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(3)
    }

    job_defaults = {
        'coalesce': True,
        'max_instances': 3,
        'misfire_grace_time': 20*60  # Maximum time in seconds for the job execution to be allowed to delay
    }
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults,
                                    timezone=tokyo_timezone)
    scheduler.start()
    protocol_config = {'allow_public_attrs': True}
    server = ThreadedServer(SchedulerService, port=12345, protocol_config=protocol_config)
    try:
        server.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()
