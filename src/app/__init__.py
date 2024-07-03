import os

from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_migrate import Migrate
from flask_restx import Api
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
limiter = Limiter(key_func=get_remote_address)


def create_app():
    app = Flask(__name__)
    app.config.from_object("config.Config")

    # Define the UPLOAD_FOLDER path
    app.config["UPLOAD_FOLDER"] = "uploads/"
    # Define the 150MB limit app wide
    app.config['MAX_CONTENT_LENGTH'] = 150 * 1000 * 1000

    # Ensure the UPLOAD_FOLDER exists
    try:
        os.makedirs(app.config["UPLOAD_FOLDER"])
    except FileExistsError:
        pass

    db.init_app(app)
    Migrate(app, db)
    limiter.init_app(app)

    api = Api(app, doc="/swagger")

    with app.app_context():
        from . import views

        api.add_namespace(views.api, path="/api")

        # Create tables if they do not exist
        db.create_all()

        # Import sample CSV data if the table is empty
        if not db.session.query(views.GameData).first():
            from .utils import import_sample_data, import_sample_events

            import_sample_data()
            import_sample_events()

    return app
