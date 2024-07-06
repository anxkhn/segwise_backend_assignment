"""
Configuration module for application settings.

This module defines the `Config` class that holds configuration parameters
such as database URI, secret keys, and rate limit settings.
"""

import os


class Config:
    """
    Configuration class for application settings.

    Attributes:
        SECRET_KEY (str): Secret key for securing session data.
        SQLALCHEMY_DATABASE_URI (str): Database URI for SQLAlchemy.
        RATELIMIT_DEFAULT (str): Default rate limit setting.
        SQLALCHEMY_TRACK_MODIFICATIONS (bool): Whether to track modifications in SQLAlchemy.
        API_SECRET_KEY (str): Secret key for API authentication.
    """

    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    RATELIMIT_DEFAULT = os.environ.get("RATELIMIT_DEFAULT")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
