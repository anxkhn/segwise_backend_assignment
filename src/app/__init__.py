"""
This module sets up the Flask application with necessary configurations
and dependencies. It includes initializing the database, migrations,
rate limiting, and API integration with Flask-RESTX. The module also
ensures the upload folder exists and sets a maximum content length
for file uploads.

Dependencies:
    - os: For operating system dependent functionalities.
    - Flask: To create the Flask application instance.
    - Limiter: For rate limiting the API.
    - get_remote_address: To get the remote address for rate limiting.
    - Migrate: For handling database migrations.
    - Api: For creating a RESTful API.
    - SQLAlchemy: For handling database operations.
"""

import os

from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_migrate import Migrate
from flask_restx import Api
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
limiter = Limiter(key_func=get_remote_address)
authorizations = {"apikey": {"type": "apiKey",
                             "in": "header", "name": "X-API-Key"}}

# Flask application factory function with docstrings


def create_app() -> Flask:
    """
    Creates a Flask application instance with database, migrations,
    rate limiting, and API integration.

    Args:
        config_object (object, optional): The configuration object to use.
            Defaults to Config.

    Returns:
        Flask: The configured Flask application instance.
    """
    app = Flask(__name__)
    app.config.from_object("config.Config")
    # Define the UPLOAD_FOLDER path
    app.config["UPLOAD_FOLDER"] = "uploads/"
    # Define the 150MB limit app wide
    app.config["MAX_CONTENT_LENGTH"] = 150 * 1000 * 1000
    # Ensure the UPLOAD_FOLDER exists
    try:
        os.makedirs(app.config["UPLOAD_FOLDER"])
    except FileExistsError:
        pass
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
    db.init_app(app)
    Migrate(app, db)
    limiter.init_app(app)
    api = Api(app, doc="/docs", authorizations=authorizations, security="apikey")
    with app.app_context():
        from . import views

        api.add_namespace(views.api, path="/api")
        # Create tables if they do not exist
        db.create_all()
        # Import sample CSV data if the table is empty
        if not db.session.query(views.GameData).first():
            from .utils import import_sample_data, import_sample_events

            import_sample_events()
            import_sample_data()
    return app
