import os


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    RATELIMIT_DEFAULT = os.environ.get("RATELIMIT_DEFAULT")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
