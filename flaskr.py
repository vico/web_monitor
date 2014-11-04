# all the imports
import urlparse
import StringIO
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash, Response
from contextlib import closing
import pyotp
import qrcode

# configuration
DATABASE = '/tmp/flaskr.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)

def connect_db():
    return sqlite3.connect(app.config['DATABASE'])

def init_db():
    with closing(connect_db()) as db:
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

@app.route('/')
def show_entries():
    cur = g.db.execute('select title, text from entries order by id desc')
    entries = [dict(title=row[0], text=row[1]) for row in cur.fetchall()]
    return render_template('show_entries.html', entries=entries)

@app.route('/add', methods=['POST'])
def add_entry():
    if not session.get('logged_in'):
        abort(401)
    g.db.execute('insert into entries (title, text) values (?, ?)', 
                [request.form['title'], request.form['text']])
    g.db.commit()
    flash('New entry was succesfully posted')
    return redirect(url_for('show_entries'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            flash('You were loggined in')
            #return redirect(url_for('show_entries'))
            return redirect(url_for('enable_tfa_via_app'))

    return render_template('login.html', error=error)

@app.route('/enable-tfa-via-app')
def enable_tfa_via_app():
    if request.method == 'GET':
        return render_template('enable_tfa_via_app.html')
    token = request.form['token']
    if token:
        pass

@app.route('/auth-qr-code.png')
def auth_qr_code():
    """generate a QR code with the users TOTP secret

    We do this to reduce the risk of leaking
    the secret over the wire in plaintext"""
    #FIXME: This logic should really apply site-wide
    domain = urlparse.urlparse(request.url).netloc

    secret = pyotp.random_base32()

    totp = pyotp.TOTP(secret)

    if not domain:
        domain = 'example.com'
    username = "%s@%s" % (5, domain)

    uri = totp.provisioning_uri(username)
    qrc = qrcode.make(uri)

    stream = StringIO.StringIO()
    qrc.save(stream)
    image = stream.getvalue()
    return Response(image, mimetype='image/png')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('show_entries'))

if __name__ == '__main__':
    app.run(host='0.0.0.0')


