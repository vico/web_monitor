# -*- encoding: utf8 -*-

import rpyc
from flask import g, current_app
from flask import render_template, request, flash, redirect, url_for
# from selenium.webdriver.remote.webelement import WebElement
from sqlalchemy.exc import IntegrityError

from . import main
from .forms import PageForm
from .. import db
from ..models import Page


def save_to_db(page):
    db.session.add(page)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return False
    return True


def add_job(page):
    conn = rpyc.connect('localhost', 12345)
    job = conn.root.add_job('scheduler_server:fetch', cron=page.cron,
                            args=[page.id], id=str(page.id))
    return job.id


def reschedule_job(job_id, cron):
    conn = rpyc.connect('localhost', 12345)
    job = conn.root.reschedule_job(job_id, cron=cron)
    return job.id


def remove_job(job_id):
    conn = rpyc.connect('localhost', 12345)
    conn.root.remove_job(job_id)


def get_jobs():
    conn = rpyc.connect('localhost', 12345)
    return conn.root.get_jobs()


@main.route('/stop_job', methods=['GET'])
def stop_job():
    jid = request.args.get('id')
    remove_job(jid)
    flash('Job {} is removed.'.format(jid))
    return redirect(url_for('.index'))


@main.route('/start_job', methods=['GET'])
def start_job():
    page_id = request.args.get('id')
    page = Page.query.get_or_404(page_id)
    job_id = add_job(page)
    flash('Job {} is started.'.format(job_id))
    return redirect(url_for('.index'))


@main.route('/rm', methods=['GET'])
def delete_page():
    page_id = request.args.get('id')
    page = Page.query.get_or_404(page_id)
    if page_id in g.jobs:  # if the job is started, stop it
        remove_job(page_id)
    db.session.delete(page)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        raise
    flash('Target URL is removed.')
    return redirect(url_for('.index'))


@main.route('/', methods=['GET', 'POST'])
def index():
    form = PageForm()
    if form.validate_on_submit():
        page = Page(url=form.url.data, cron=form.cron_schedule.data,
                    xpath=form.xpath.data, keyword=form.keyword.data)

        save_to_db(page)  # save page to db first to get id

        job_id = add_job(page)

        if job_id:  # if we have no error in registering job, add info to db
            flash('The page has been created.')
        else:
            flash('Job is not successfully registered!! Maybe some error in cron expression')

        return redirect(url_for('.index'))

    pages = Page.query.all()
    jobs = get_jobs()
    job_ids = [job.id for job in jobs]
    ps = []
    for page in pages:
        domain = page.url.split('/')[2] if '/' in page.url else ''
        p = {
            'domain': domain,
            'url': page.url,
            'cron': page.cron,
            'id': page.id,
            'updated_time': page.updated_time,
            'last_check_time': page.last_check_time
        }
        ps.append(p)
    return render_template('index.html', urls=ps, form=form, jobs=job_ids)


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
        page.xpath = form.xpath.data
        page.keyword = form.keyword.data

        try:
            jobs = get_jobs()
            # remember job.id is of string
            if str(page.id) not in [job.id for job in jobs]:
                add_job(page)
            else:
                reschedule_job(page.id, page.cron)

        except Exception as e:
            flash('[edit:{}] Job is not successfully scheduled!!'.format(id))
            current_app.logger.error(e)
            form.url.data = page.url
            form.cron_schedule.data = page.cron
            form.xpath.data = page.xpath
            form.keyword.data = page.keyword
            return redirect(url_for('.edit', id=page.id))

        if save_to_db(page):
            flash('The page has been updated.')
        else:
            flash('The page is not successfully saved in db.')
        return redirect(url_for('.page', id=page.id))

    form.url.data = page.url
    form.cron_schedule.data = page.cron
    form.xpath.data = page.xpath
    form.keyword.data = page.keyword
    return render_template('edit_page.html', form=form)


@main.route('/page/<int:id>', methods=['GET', 'POST'])
def page(id):
    page = Page.query.get_or_404(id)
    return render_template('page.html', page=page)
