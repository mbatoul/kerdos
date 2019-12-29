import os
from flask import Flask, render_template

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

  # Main route
  @app.route('/')
  def home():
    return render_template('home.html')

  return app

app = create_app()
