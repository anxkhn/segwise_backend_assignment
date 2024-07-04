from . import db
from .models import GameData, Event
import pandas as pd
from datetime import datetime


def parse_date(date_str):
    try:
        date = datetime.strptime(date_str, "%b %d, %Y")
    except ValueError:
        date_parts = date_str.split()
        month = date_parts[0][:3] if len(date_parts) > 0 else "Jan"
        day = date_parts[1] if len(date_parts) > 1 else "1"
        year = date_parts[2] if len(date_parts) > 2 else "1970"
        try:
            date = datetime.strptime(f"{month} {day}, {year}", "%b %d, %Y")
        except ValueError:
            date = datetime(
                1970, 1, 1
            )  # fallback to Jan 1, 1970 for invalid date strings
    return date.strftime("%Y-%m-%d")


def save_csv_to_db(csv_file_path, encoding="utf-8", delimiter=",", event_id=None):
    data = pd.read_csv(csv_file_path, encoding=encoding, delimiter=delimiter)
    data = data.where(pd.notnull(data), None)  # Replace NaNs with None
    for _, row in data.iterrows():
        game_data = GameData(
            app_id=row["AppID"],
            name=row["Name"],
            release_date=parse_date(row["Release date"]),
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
            event_id=event_id,
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
    sample_csv_path = "sample_gamedata.csv"
    data = pd.read_csv(sample_csv_path)
    data = data.where(pd.notnull(data), None)  # Replace NaNs with None
    for _, row in data.iterrows():
        game_data = GameData(
            app_id=row["AppID"],
            name=row["Name"],
            release_date=parse_date(row["Release date"]),
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
            event_id=0,
        )
        db.session.add(game_data)
    db.session.commit()


def import_sample_events():
    sample_events_path = "sample_events.csv"
    data = pd.read_csv(sample_events_path)
    data = data.where(pd.notnull(data), None)  # Replace NaNs with None
    for _, row in data.iterrows():
        event = Event(
            original_url=row["original_url"],
            mode=row["mode"],
            altname=row["altname"],
            filepath=row["filepath"],
            encoding=row["encoding"],
            delimiter=row["delimiter"],
            created_at=(
                datetime.utcnow() if pd.isnull(row["created_at"]) else row["created_at"]
            ),
        )
        db.session.add(event)
    db.session.commit()
