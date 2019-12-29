import functools
from flask import (
  Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash
from app.db import get_db

auth = Blueprint('auth', __name__, url_prefix='/auth', static_folder='static', template_folder='templates')

@auth.route('/register', methods=['GET', 'POST'])
def register():
  if request.method == 'POST':
    email = request.form['email']
    password = request.form['password']
    db = get_db()
    error = None

    if not email:
      error = 'Email is required'
    elif not password:
      error = 'Password is required.'
    elif db.execute('select id from user where email = (?)', (email,)).fetchone() is not None:
      error = 'User with email {} is already registered.'.format(email)

    if error is None:
      db.execute('insert into user (email, password) values (?, ?)', (email, generate_password_hash(password)))
      db.commit()
      return redirect(url_for('auth.login'))

    flash(error)

  return render_template('auth/register.html')

@auth.route('/login', methods=('GET', 'POST'))
def login():
  if request.method == 'POST':
    email = request.form['email']
    password = request.form['password']
    db = get_db()
    error = None
    user = db.execute('select * from user where email = ?', (email,)).fetchone()

    if user is None:
      error = 'Incorrect email.'
    elif not check_password_hash(user['password'], password):
      error = 'Incorrect password.'

    if error is None:
      session.clear()
      session['user_id'] = user['id']
      return redirect(url_for('home'))

    flash(error)

  return render_template('auth/login.html')

@auth.before_app_request
def load_logged_in_user():
  user_id = session.get('user_id')

  if user_id is None:
    g.user = None
  else:
    g.user = get_db().execute('SELECT * FROM user WHERE id = ?', (user_id,)).fetchone()

@auth.route('/logout')
def logout():
  session.clear()

  return redirect(url_for('home'))
