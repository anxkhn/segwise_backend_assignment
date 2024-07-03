import os

import pandas as pd

from app import db
from app.models import GameData


def import_sample_data():
    sample_csv_path = os.path.join(os.path.dirname(__file__), "sample.csv")
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
