# -*- encoding: utf8 -*-
from datetime import datetime
from pprint import pprint

from apscheduler.triggers.cron import CronTrigger
from flask import g
from flask import render_template, request, current_app, flash, redirect, url_for
from sqlalchemy.exc import IntegrityError

from . import main
from .forms import PageForm
from .. import db
from ..models import Page


@main.before_request
def before_request():
    if 'jobs' not in g:
        g.jobs = {}
        jobs = current_app.apscheduler.get_jobs()
        for job in jobs:
            g.jobs[job.id] = job


# @main.teardown_request
# def tear_down():
#     pass


def scheduled_task(task_id):
    print('Task {} running at {}.'.format(task_id, datetime.now()))


@main.route('/stop_job', methods=['GET'])
def stop_job():
    jid = request.args.get('id')
    job = g.jobs[jid]
    job.remove()
    flash('Job {} is removed.'.format(jid))
    return redirect(url_for('.index'))


@main.route('/', methods=['GET', 'POST'])
def index():
    form = PageForm()
    pprint(g.jobs)
    if form.validate_on_submit():
        page = Page(url=form.url.data, cron=form.cron_schedule.data)

        db.session.add(page)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise
        # trigger = CronTrigger(second=5)

        pprint(page)
        print('id = {}'.format(page.id))
        job = current_app.apscheduler.add_job(func=scheduled_task, trigger=CronTrigger.from_crontab('*/1 * * * *'),
                                              args=[1], id=str(page.id))
        g.jobs[job.id] = job
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
        db.session.add(page)
        flash('The page has been updated.')
        return redirect(url_for('.page', id=page.id))
    form.url.data = page.url
    return render_template('edit_page.html', form=form)


@main.route('/page/<int:id>', methods=['GET', 'POST'])
def page(id):
    page = Page.query.get_or_404(id)
    # form = CommentForm()
    # if form.validate_on_submit():
    #     comment = Comment(body=form.body.data,
    #                       post=post,
    #                       author=current_user._get_current_object())
    #     db.session.add(comment)
    #     flash('Your comment has been published.')
    #     return redirect(url_for('.post', id=post.id, page=-1))

    return render_template('page.html', page=page)
