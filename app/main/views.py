# -*- encoding: utf8 -*-
import hashlib
from datetime import datetime
from pprint import pprint

import polling2
from apscheduler.triggers.cron import CronTrigger
from flask import g
from flask import render_template, request, current_app, flash, redirect, url_for
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.remote.webelement import WebElement
from sqlalchemy.exc import IntegrityError

from app import scheduler
from . import main
from .forms import PageForm
from .. import db, diff_match_patch
from ..email import send_email, send_multiple_emails
from ..models import Page


@main.before_request
def before_request():
    if 'jobs' not in g:
        g.jobs = {}
        jobs = current_app.apscheduler.get_jobs()
        for job in jobs:
            g.jobs[job.id] = job


def fetch(id):
    app = scheduler.app
    page = Page.query.get_or_404(id)  # get fresh page object from db
    app.logger.info('fetch called on {}'.format(page.url))
    chrome_options = Options()
    # chrome_options.add_argument("--disable-extensions")
    # chrome_options.add_argument("--disable-gpu")
    # chrome_options.add_argument("--no-sandbox") # linux only
    for option in app.config['CHROME_OPTIONS'].split(','):
        chrome_options.add_argument(option)
    # driver = webdriver.Remote(service.service_url)
    driver = webdriver.Chrome(app.config['CHROME_DRIVER'], options=chrome_options)
    driver.implicitly_wait(10)  # seconds
    driver.get(page.url)
    # time.sleep(5)  # Let the user actually see something!
    # xpath = '/html/body/article/div/div[3]/div/div[4]/div/div[1]/table'
    target = polling2.poll(lambda: driver.find_element_by_xpath(page.xpath), step=0.5, timeout=10)
    target_text = target.get_property('outerHTML')

    md5sum = hashlib.md5(target_text.encode('utf-8')).hexdigest()
    if page and (page.md5sum != md5sum):
        # update new md5sum
        dmp = diff_match_patch()
        diffs = dmp.diff_main(target_text, page.text)
        page.md5sum = md5sum
        page.text = target_text
        dmp.diff_cleanupSemantic(diffs)
        diff_html = dmp.diff_prettyHtml(diffs)
        page.diff = diff_html
        page.updated_time = datetime.utcnow()
        db.session.add(page)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise
        # notify diff
        with app.app_context():
            send_multiple_emails(app.config['MAIL_RECIPIENT'].split(','), 'Updated', 'emails/notification', diff=diff_html, page=page)
    driver.quit()
    page.last_check_time = datetime.utcnow()
    db.session.add(page)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        raise


def add_job(page, scheduler):
    job = scheduler.add_job(func=fetch, trigger=CronTrigger.from_crontab(page.cron),
                            args=[page.id], id=str(page.id))
    g.jobs[job.id] = job
    return job


@main.route('/stop_job', methods=['GET'])
def stop_job():
    jid = request.args.get('id')
    job = g.jobs[jid]
    job.remove()
    flash('Job {} is removed.'.format(jid))
    return redirect(url_for('.index'))


@main.route('/start_job', methods=['GET'])
def start_job():
    page_id = request.args.get('id')
    page = Page.query.get_or_404(page_id)
    job = add_job(page, scheduler)
    flash('Job {} is started.'.format(job.id))
    return redirect(url_for('.index'))


@main.route('/', methods=['GET', 'POST'])
def index():
    form = PageForm()
    pprint(g.jobs)
    if form.validate_on_submit():
        page = Page(url=form.url.data, cron=form.cron_schedule.data,
                    xpath=form.xpath.data)

        db.session.add(page)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise
        # trigger = CronTrigger(second=5)

        pprint(page)
        print('id = {}'.format(page.id))
        add_job(page, scheduler)
        flash('The page has been created.')
        return redirect(url_for('.index'))

    urls = Page.query.all()
    return render_template('index.html', urls=urls, form=form, jobs=g.jobs)


@main.route('/edit/<int:id>', methods=['GET', 'POST'])
# @login_required
def edit(id):
    page = Page.query.get_or_404(id)
    # if current_user != post.author and \
    #         not current_user.can(Permission.ADMINISTER):
    #     abort(403)
    form = PageForm()
    if form.validate_on_submit():
        page.url = form.url.data
        page.cron = form.cron_schedule.data
        # xpath = '/html/body/article/div/div[3]/div/div[4]/div/div[1]/table'
        page.xpath = form.xpath.data
        db.session.add(page)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise
        if str(page.id) in g.jobs:
            job = g.jobs[str(page.id)]
            job.reschedule(trigger=CronTrigger.from_crontab(page.cron))
        else:
            add_job(page, scheduler)
        flash('The page has been updated.')
        return redirect(url_for('.page', id=page.id))
    form.url.data = page.url
    form.cron_schedule.data = page.cron
    form.xpath.data = page.xpath
    return render_template('edit_page.html', form=form)


@main.route('/page/<int:id>', methods=['GET', 'POST'])
def page(id):
    page = Page.query.get_or_404(id)
    return render_template('page.html', page=page)
