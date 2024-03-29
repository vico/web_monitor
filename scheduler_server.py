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
import tempfile
import time
import traceback
import urllib.request
from datetime import datetime

import polling2
import rpyc
import sentry_sdk
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pymysql import IntegrityError
from pytz import timezone
from rpyc.utils.server import ThreadedServer
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException
from selenium.webdriver.chrome.options import Options
from sqlalchemy.orm.exc import NoResultFound

from app import diff_match_patch, create_app, decorate_app
from app.email import send_multiple_emails
from app.models import Page
from config import Config
from db import db_session

sentry_sdk.init(
    Config.SENTRY_URL,
    traces_sample_rate=1.0
)

logging.basicConfig(format='%(asctime)-15s.%(msecs).03d %(levelname)-8s %(threadName)-19s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger('apscheduler')
logger.level = logging.DEBUG

app = create_app(os.getenv('FLASK_ENV') or 'default')
app = decorate_app(app)


def retry_fetch(page_id: int):
    retries = 10
    while retries > 0:
        try:
            return fetch(page_id)
        except (NoSuchElementException, TimeoutException, WebDriverException) as e:
            if retries > 0:
                retries -= 1
                logger.info("Retries left {}, Continuing on {}".format(retries, traceback.format_exc()))
                time.sleep(5)
            else:
                logger.error("Already retried {} times, Continuing on {}".format(10, traceback.format_exc()))
                raise e


def fetch(page_id: int):
    try:
        page = db_session.query(Page).filter(Page.id == page_id).one()  # get fresh page object from db
        chrome_options = Options()
        for option in Config.CHROME_OPTIONS.split(','):
            chrome_options.add_argument(option)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36")
        # driver = webdriver.Remote(service.service_url)
        driver = webdriver.Chrome(Config.CHROME_DRIVER, options=chrome_options)
        driver.implicitly_wait(10)  # seconds

        driver.get(page.url)

        # time.sleep(5)  # Let the user actually see something!
        try:
            xpath = page.xpath if page.xpath else '//body'
            target = polling2.poll(lambda: driver.find_element_by_xpath(xpath), step=0.5, timeout=10)
            if page.xpath and page.xpath.endswith('img'):
                with tempfile.TemporaryDirectory() as tmp:
                    path = os.path.join(tmp, 'webmonitor')
                    # get the image source
                    img_src = target.get_attribute('src')

                    # download the image and write to `path`
                    urllib.request.urlretrieve(img_src, path)

                    md5sum = hashlib.md5(open(path, 'rb').read()).hexdigest()

                    if page and md5sum != page.md5sum:
                        page.md5sum = md5sum
                        page.updated_time = datetime.utcnow()
                        db_session.add(page)
                        try:
                            db_session.commit()
                        except IntegrityError:
                            db_session.rollback()
                            raise
                        with app.app_context():
                            send_multiple_emails(Config.MAIL_RECIPIENT.split(','),
                                                 '{} updated'.format(page.url),
                                                 diff='Image', page=page, image_path=path)
            else:  # not image

                target_text = target.get_property('outerHTML')
                md5sum = hashlib.md5(target_text.encode('utf-8')).hexdigest()
                if page and page.text and md5sum != page.md5sum:
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
                page.text = target_text
            driver.quit()
            page.last_check_time = datetime.utcnow()
            page.md5sum = md5sum  # md5sum for new page/image file
            db_session.add(page)
            try:
                db_session.commit()
            except IntegrityError:
                db_session.rollback()
                raise
        except (NoSuchElementException, WebDriverException) as e:
            if driver:
                driver.quit()
            raise

    except NoResultFound as _:
        job = scheduler.get_job(str(page_id))
        if job:
            job.remove()
        else:
            logger.error("No job found for {} even though it is running.".format(page_id))
    except Exception as e:
        db_session.rollback()
        raise


class SchedulerService(rpyc.Service):
    def exposed_add_job(self, func, *args, cron=None, **kwargs):
        # return scheduler.add_job(func, *args, **kwargs)
        return scheduler.add_job(func, *args, trigger=CronTrigger.from_crontab(cron),
                                 misfire_grace_time=180 * 60, **kwargs)

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
        'processpool': ProcessPoolExecutor(2)
    }

    job_defaults = {
        'coalesce': True,
        'max_instances': 3,
        'misfire_grace_time': 180 * 60  # Maximum time in seconds for the job execution to be allowed to delay
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
