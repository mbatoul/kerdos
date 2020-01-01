import os
from flask import Flask, render_template, session, redirect, url_for

def create_app(test_config=None):
  # App initialization
  app = Flask(__name__, instance_relative_config=True)

  app.config.from_mapping(
    SECRET_KEY='dev',
    DATABASE=os.path.join(app.instance_path, 'app.sqlite'),
  )

  if test_config is None:
    app.config.from_pyfile('config.py', silent=True)
  else:
    app.config.from_mapping(test_config)

  try:
    os.makedirs(app.instance_path)
  except OSError:
    pass

  # Database initialization
  from . import db
  db.init_app(app)

  # Authentication blueprint
  from . import auth
  app.register_blueprint(auth.auth)

  # Trading info blueprint
  from . import trading
  app.register_blueprint(trading.trading)

  # Main route
  @app.route('/')
  def home():
    user_id = session.get('user_id')

    if user_id is None:
      return render_template('home.html')
    else:
      return redirect(url_for('trading.index'))

  return app

app = create_app()
