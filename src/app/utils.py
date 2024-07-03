import pandas as pd

from . import db
from .models import GameData


def save_csv_to_db(csv_file_path):
    data = pd.read_csv(csv_file_path)
    data = data.where(pd.notnull(data), None)  # Replace NaNs with None
    for _, row in data.iterrows():
        game_data = GameData(
            app_id=row["AppID"],
            name=row["Name"],
            release_date=row["Release date"],
            required_age=row["Required age"],
            price=row["Price"],
            dlc_count=row["DLC count"],
            about_game=row["About the game"],
            supported_languages=row["Supported languages"],
            windows=row["Windows"],
            mac=row["Mac"],
            linux=row["Linux"],
            positive=row["Positive"],
            negative=row["Negative"],
            score_rank=row["Score rank"],
            developers=row["Developers"],
            publishers=row["Publishers"],
            categories=row["Categories"],
            genres=row["Genres"],
            tags=row["Tags"],
        )
        db.session.add(game_data)
    db.session.commit()


def query_data(filters):
    query = GameData.query
    for key, value in filters.items():
        if hasattr(GameData, key):
            column = getattr(GameData, key)
            if isinstance(value, str):
                query = query.filter(column.ilike(f"%{value}%"))
            else:
                query = query.filter(column == value)
    return [data.as_dict() for data in query.all()]


def import_sample_data():
    sample_csv_path = "sample.csv"
    data = pd.read_csv(sample_csv_path)
    data = data.where(pd.notnull(data), None)  # Replace NaNs with None
    for _, row in data.iterrows():
        game_data = GameData(
            app_id=row["AppID"],
            name=row["Name"],
            release_date=row["Release date"],
            required_age=row["Required age"],
            price=row["Price"],
            dlc_count=row["DLC count"],
            about_game=row["About the game"],
            supported_languages=row["Supported languages"],
            windows=row["Windows"],
            mac=row["Mac"],
            linux=row["Linux"],
            positive=row["Positive"],
            negative=row["Negative"],
            score_rank=row["Score rank"],
            developers=row["Developers"],
            publishers=row["Publishers"],
            categories=row["Categories"],
            genres=row["Genres"],
            tags=row["Tags"],
        )
        db.session.add(game_data)
    db.session.commit()
