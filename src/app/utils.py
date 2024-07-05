from datetime import datetime

import pandas as pd
from flask import jsonify
from fuzzywuzzy import process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import func
import numpy as np
from scipy.stats import skew, kurtosis

from . import db
from .models import Event, GameData


def load_game_data():
    games = GameData.query.all()
    data = [
        {
            "Name": game.name,
            "About the game": game.about_game,
            "Categories": game.categories,
            "Genres": game.genres,
            "Tags": game.tags,
        }
        for game in games
    ]
    df = pd.DataFrame(data)
    df["combined_features"] = (
        df["About the game"]
        + " "
        + df["Categories"]
        + " "
        + df["Genres"]
        + " "
        + df["Tags"]
    )
    df["combined_features"] = df["combined_features"].fillna("")
    return df


def get_similar_games(game_name):
    df = load_game_data()

    # TF-IDF Vectorization
    tfidf = TfidfVectorizer(stop_words="english")
    tfidf_matrix = tfidf.fit_transform(df["combined_features"])

    # Calculate cosine similarity
    cosine_sim = cosine_similarity(tfidf_matrix)

    # Find the most similar game name in the dataset
    closest_match = find_most_similar_game(game_name, df["Name"].tolist())

    idx = df.index[df["Name"] == closest_match].tolist()[0]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:11]  # Top 10 similar games
    game_indices = [i[0] for i in sim_scores]
    return df["Name"].iloc[game_indices].tolist(), closest_match


def find_most_similar_game(input_name, names):
    match = process.extractOne(input_name, names)
    return match[0] if match else None


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

def query_aggregate_data(aggregate, column=None):
    allowed_columns = ["price", "dlc_count", "positive", "negative"]
    
    if column and column not in allowed_columns and column != "all":
        raise ValueError(f"Column {column} is not allowed for aggregation")
    
    query = db.session.query(GameData)
    result = {}
    if column and column != "all":
        column_attr = getattr(GameData, column)
        data = [row[0] for row in query.with_entities(column_attr).all()]
        
        if not data:
            raise ValueError(f"No data found for column {column}")
        
        if aggregate == "all":
            result = {
                column: {
                    "min": min(data),
                    "max": max(data),
                    "median": np.median(data),
                    "mean": np.mean(data),
                    "range": max(data) - min(data),
                    "iqr": np.percentile(data, 75) - np.percentile(data, 25),
                    "std_dev": np.std(data),
                    "variance": np.var(data),
                    "sum": sum(data),
                    "count": len(data),
                    "percentiles": {
                        "25th": np.percentile(data, 25),
                        "50th": np.percentile(data, 50),
                        "75th": np.percentile(data, 75),
                    },
                    "skewness": skew(data),
                    "kurtosis": kurtosis(data),
                }
            }
        elif aggregate == "min":
            result = {column: {"min": min(data)}}
        elif aggregate == "max":
            result = {column: {"max": max(data)}}
        elif aggregate == "median":
            result = {column: {"median": np.median(data)}}
        elif aggregate == "mean":
            result = {column: {"mean": np.mean(data)}}
        elif aggregate == "range":
            result = {column: {"range": max(data) - min(data)}}
        elif aggregate == "iqr":
            result = {column: {"iqr": np.percentile(data, 75) - np.percentile(data, 25)}}
        elif aggregate == "std_dev":
            result = {column: {"std_dev": np.std(data)}}
        elif aggregate == "variance":
            result = {column: {"variance": np.var(data)}}
        elif aggregate == "sum":
            result = {column: {"sum": sum(data)}}
        elif aggregate == "count":
            result = {column: {"count": len(data)}}
        elif aggregate == "percentiles":
            result = {
                column: {
                    "percentiles": {
                        "25th": np.percentile(data, 25),
                        "50th": np.percentile(data, 50),
                        "75th": np.percentile(data, 75),
                    }
                }
            }
        elif aggregate == "skewness":
            result = {column: {"skewness": skew(data)}}
        elif aggregate == "kurtosis":
            result = {column: {"kurtosis": kurtosis(data)}}
    elif column == "all" or not column:
        for col in allowed_columns:
            column_attr = getattr(GameData, col)
            data = [row[0] for row in query.with_entities(column_attr).all()]
            if not data:
                continue
            print(data)
            result[col] = {
                "min": min(data),
                "max": max(data),
                "median": np.median(data),
                "mean": np.mean(data),
                "range": max(data) - min(data),
                "iqr": np.percentile(data, 75) - np.percentile(data, 25),
                "std_dev": np.std(data),
                "variance": np.var(data),
                "sum": sum(data),
                "count": len(data),
                "percentiles": {
                    "25th": np.percentile(data, 25),
                    "50th": np.percentile(data, 50),
                    "75th": np.percentile(data, 75),
                },
                "skewness": skew(data),
                "kurtosis": kurtosis(data),
            }
    
    return result


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
                datetime.utcnow() if pd.isnull(
                    row["created_at"]) else row["created_at"]
            ),
        )
        db.session.add(event)
    db.session.commit()
