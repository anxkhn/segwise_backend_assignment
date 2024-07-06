"""
Utility functions for game data analysis and database operations.

This module includes functions to load game data, find similar games,
perform data queries and aggregations, and import sample data into the database.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from fuzzywuzzy import process
from scipy.stats import kurtosis, skew
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import and_

from . import db
from .models import Event, GameData


def load_game_data() -> pd.DataFrame:
    """
    Load game data from the database and prepare it for analysis.

    Returns:
        pd.DataFrame: A DataFrame containing game data with combined features.
    """
    games = GameData.query.all()
    data = [
        {
            "app_id": game.app_id,
            "name": game.name,
            "release_date": game.release_date,
            "price": game.price,
            "about_game": game.about_game,
            "categories": game.categories,
            "genres": game.genres,
            "tags": game.tags,
        }
        for game in games
    ]
    df = pd.DataFrame(data)
    df["combined_features"] = (
        df["about_game"].fillna("")
        + " "
        + df["categories"].fillna("")
        + " "
        + df["genres"].fillna("")
        + " "
        + df["tags"].fillna("")
    )
    return df


def get_similar_games(game_name: str) -> Dict[str, Any]:
    """
    Find similar games based on the input game name.

    Args:
        game_name (str): The name of the game to find similar games for.

    Returns:
        Dict[str, Any]: A dictionary containing the closest match and similar games.
    """
    df = load_game_data()

    tfidf = TfidfVectorizer(stop_words="english")
    tfidf_matrix = tfidf.fit_transform(df["combined_features"])
    cosine_sim = cosine_similarity(tfidf_matrix)

    closest_match = find_most_similar_game(game_name, df["name"].tolist())
    idx = df.index[df["name"] == closest_match].tolist()[0]
    sim_scores = sorted(
        list(enumerate(cosine_sim[idx])), key=lambda x: x[1], reverse=True
    )[1:11]
    game_indices = [i[0] for i in sim_scores]

    similar_games_info = [
        {
            "app_id": int(df.iloc[i]["app_id"]),
            "name": df.iloc[i]["name"],
            "release_date": format_date(df.iloc[i]["release_date"]),
            "price": float(df.iloc[i]["price"]),
            "similarity_score": float(sim_scores[idx][1]),
        }
        for idx, i in enumerate(game_indices)
    ]

    return {
        "closest_match": {
            "app_id": int(df.loc[idx, "app_id"]),
            "name": df.loc[idx, "name"],
            "release_date": format_date(df.loc[idx, "release_date"]),
            "price": float(df.loc[idx, "price"]),
        },
        "similar_games": similar_games_info,
    }


def find_most_similar_game(input_name: str, names: List[str]) -> Optional[str]:
    """
    Find the most similar game name from a list of names.

    Args:
        input_name (str): The input game name to match.
        names (List[str]): A list of game names to search from.

    Returns:
        Optional[str]: The most similar game name, or None if no match is found.
    """
    match = process.extractOne(input_name, names)
    return match[0] if match else None


def format_date(date_value: Any) -> str:
    """
    Format a date value to a string.

    Args:
        date_value (Any): The date value to format.

    Returns:
        str: A formatted date string in 'YYYY-MM-DD' format.
    """
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")
    return date_value


def parse_date(date_str: str) -> str:
    """
    Parse a date string into a standardized format.

    Args:
        date_str (str): The date string to parse.

    Returns:
        str: A formatted date string in 'YYYY-MM-DD' format.
    """
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
            date = datetime(1970, 1, 1)
    return date.strftime("%Y-%m-%d")


def save_csv_to_db(
    csv_file_path: str,
    encoding: str = "utf-8",
    delimiter: str = ",",
    event_id: Optional[int] = None,
) -> None:
    """
    Save data from a CSV file to the database.

    Args:
        csv_file_path (str): The path to the CSV file.
        encoding (str, optional): The encoding of the CSV file. Defaults to "utf-8".
        delimiter (str, optional): The delimiter used in the CSV file. Defaults to ",".
        event_id (Optional[int], optional): The ID of the associated event. Defaults to None.
    """
    data = pd.read_csv(csv_file_path, encoding=encoding, delimiter=delimiter)
    data = data.where(pd.notnull(data), None)
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


def query_data(
    filters: Dict[str, Any], cursor: int, limit: int
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Query game data based on filters and pagination parameters.

    Args:
        filters (Dict[str, Any]): A dictionary of filters to apply to the query.
        cursor (int): The offset for pagination.
        limit (int): The maximum number of results to return.

    Returns:
        A tuple containing the list of game data and the total count.
    """
    query = GameData.query

    filter_conditions = []
    for key, value in filters.items():
        if key == "before":
            filter_conditions.append(
                GameData.release_date < datetime.strptime(
                    value, "%Y-%m-%d").date()
            )
        elif key == "after":
            filter_conditions.append(
                GameData.release_date > datetime.strptime(
                    value, "%Y-%m-%d").date()
            )
        elif key == "release_date":
            filter_conditions.append(
                GameData.release_date == datetime.strptime(
                    value, "%Y-%m-%d").date()
            )
        elif key == "min_price":
            filter_conditions.append(GameData.price >= float(value))
        elif key == "max_price":
            filter_conditions.append(GameData.price <= float(value))
        elif hasattr(GameData, key):
            column = getattr(GameData, key)
            column_type = column.property.columns[0].type.python_type
            if column_type in (int, float):
                filter_conditions.append(column == column_type(value))
            elif column_type is str:
                filter_conditions.append(column.contains(value))
            elif column_type is bool:
                filter_conditions.append(
                    column == (value.lower() in ["true", "1", "yes"])
                )
            else:
                filter_conditions.append(column == value)
        else:
            print(f"Attribute {key} not found in GameData model")

    if filter_conditions:
        query = query.filter(and_(*filter_conditions))

    total = query.count()
    results = query.offset(cursor).limit(limit).all()

    return [game.to_dict() for game in results], total


def query_aggregate_data(
    aggregate: str, column: Optional[str] = None
) -> Dict[str, Any]:
    """
    Perform aggregate operations on game data.

    Args:
        aggregate (str): The type of aggregation to perform.
        column (Optional[str], optional): The column to aggregate. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary containing the aggregated results.
    """
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
            result = {
                column: {"iqr": np.percentile(
                    data, 75) - np.percentile(data, 25)}
            }
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


def import_sample_data() -> None:
    """
    Import sample game data from a CSV file into the database.
    """
    sample_csv_path = "sample_gamedata.csv"
    data = pd.read_csv(sample_csv_path)
    data = data.where(pd.notnull(data), None)
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


def import_sample_events() -> None:
    """
    Import sample events data from a CSV file into the database.
    """
    sample_events_path = "sample_events.csv"
    data = pd.read_csv(sample_events_path)
    data = data.where(pd.notnull(data), None)
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
