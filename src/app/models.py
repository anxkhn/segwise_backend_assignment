"""
This module defines the database models for the application.
It includes the Event and GameData models which are used to
store event details and game data respectively.
"""

from typing import Dict, Union

from . import db


class Event(db.Model):
    """
    Event model represents an event entry in the database.

    Attributes:
        id (int): The primary key of the event.
        original_url (str): The original URL associated with the event.
        mode (str): The mode of the event.
        altname (str): An alternative name for the event.
        filepath (str): The file path where the event data is stored.
        encoding (str): The encoding format of the event data file.
        delimiter (str): The delimiter used in the event data file.
        created_at (datetime): The timestamp when the event was created.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    original_url = db.Column(db.String(255), nullable=True)
    mode = db.Column(db.String(50), nullable=False)
    altname = db.Column(db.String(255), nullable=True)
    filepath = db.Column(db.String(255), nullable=False)
    encoding = db.Column(db.String(50), nullable=False)
    delimiter = db.Column(db.String(5), nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())


class GameData(db.Model):
    """
    GameData model represents the game data entry in the database.

    Attributes:
        id (int): The primary key of the game data.
        app_id (int): The application ID of the game.
        name (str): The name of the game.
        release_date (str): The release date of the game.
        required_age (int): The required age to play the game.
        price (float): The price of the game.
        dlc_count (int): The number of downloadable content (DLC) available.
        about_game (str): A description of the game.
        supported_languages (str): The languages supported by the game.
        windows (bool): Whether the game supports Windows OS.
        mac (bool): Whether the game supports macOS.
        linux (bool): Whether the game supports Linux OS.
        positive (int): The number of positive reviews.
        negative (int): The number of negative reviews.
        score_rank (int): The score rank of the game.
        developers (str): The developers of the game.
        publishers (str): The publishers of the game.
        categories (str): The categories of the game.
        genres (str): The genres of the game.
        tags (str): The tags associated with the game.
        event_id (int): The ID of the associated event.
    """

    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    release_date = db.Column(db.String(255), nullable=True)
    required_age = db.Column(db.Integer, nullable=True)
    price = db.Column(db.Float, nullable=True)
    dlc_count = db.Column(db.Integer, nullable=True)
    about_game = db.Column(db.Text, nullable=True)
    supported_languages = db.Column(db.String(255), nullable=True)
    windows = db.Column(db.Boolean, nullable=True)
    mac = db.Column(db.Boolean, nullable=True)
    linux = db.Column(db.Boolean, nullable=True)
    positive = db.Column(db.Integer, nullable=True)
    negative = db.Column(db.Integer, nullable=True)
    score_rank = db.Column(db.Integer, nullable=True)
    developers = db.Column(db.String(255), nullable=True)
    publishers = db.Column(db.String(255), nullable=True)
    categories = db.Column(db.String(255), nullable=True)
    genres = db.Column(db.String(255), nullable=True)
    tags = db.Column(db.String(255), nullable=True)
    event_id = db.Column(db.Integer, db.ForeignKey("event.id"), nullable=False)

    def to_dict(self) -> Dict[str, Union[int, str, float, bool]]:
        """
        Convert the GameData instance into a dictionary.

        Returns:
            Dict[str, Union[int, str, float, bool]]: The game data as a dictionary.
        """
        return {
            "id": self.id,
            "app_id": self.app_id,
            "name": self.name,
            "release_date": self.release_date,
            "required_age": self.required_age,
            "price": self.price,
            "dlc_count": self.dlc_count,
            "about_game": self.about_game,
            "supported_languages": self.supported_languages,
            "windows": self.windows,
            "mac": self.mac,
            "linux": self.linux,
            "positive": self.positive,
            "negative": self.negative,
            "score_rank": self.score_rank,
            "developers": self.developers,
            "publishers": self.publishers,
            "categories": self.categories,
            "genres": self.genres,
            "tags": self.tags,
            "event_id": self.event_id,
        }
