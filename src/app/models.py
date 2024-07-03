from . import db


class Event(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    original_url = db.Column(db.String(255), nullable=True)
    mode = db.Column(db.String(50), nullable=False)
    altname = db.Column(db.String(255), nullable=True)
    filepath = db.Column(db.String(255), nullable=False)
    encoding = db.Column(db.String(50), nullable=False)
    delimiter = db.Column(db.String(5), nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())


class GameData(db.Model):
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

    def as_dict(self):
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
