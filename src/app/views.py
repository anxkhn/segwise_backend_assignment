"""This Python script defines a Flask REST API for game data operations.

The API supports the following functionalities:

* Uploading CSV files containing game data.
* Importing CSV files from URLs containing game data.
* Querying game data based on various filters.
* Retrieving statistical data for game numerical attributes.
* Finding similar games based on a given game name.

The API utilizes Flask-RESTX for building the API endpoints and data validation.

Additionally, the script defines helper functions for:
* Validating CSV file parameters (encoding and delimiter).
* Checking if a file has an allowed extension.
* Requiring a valid API key for specific functions.
* Saving CSV data to the database.
* Querying game data based on filters.
* Querying statistical data for game numerical attributes.
* Finding similar games based on a given game name.
"""

import os
import re
import time
from functools import wraps
from typing import Any, Dict, Tuple

import requests
from dotenv import load_dotenv
from flask import current_app, redirect, request
from flask_restx import Namespace, Resource, fields, reqparse
from requests.exceptions import RequestException
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename

from . import db, limiter
from .models import Event, GameData
from .utils import get_similar_games, query_aggregate_data, query_data, save_csv_to_db

load_dotenv()
API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
authorizations = {"apikey": {"type": "apiKey",
                             "in": "header", "name": "X-API-Key"}}
api = Namespace(
    "api",
    description="Game data operations",
    authorizations=authorizations,
    security="apikey",
)
csv_upload = api.model(
    "CSVUpload",
    {"csv_file": fields.Raw(
        required=True, description="The CSV file to upload")},
)
UPLOAD_FOLDER = "uploads/"
ALLOWED_EXTENSIONS = {"csv"}


def validate_csv_params(encoding: str, delimiter: str) -> bool:
    """
    Validate CSV parameters.

    Args:
        encoding (str): The file encoding.
        delimiter (str): The CSV delimiter.

    Returns:
        bool: True if parameters are valid, False otherwise.
    """
    valid_encodings = ["utf-8", "ascii", "iso-8859-1"]
    if encoding not in valid_encodings or len(delimiter) != 1:
        return False
    return True


def allowed_file(filename: str) -> bool:
    """
    Check if the file has an allowed extension.

    Args:
        filename (str): The name of the file.

    Returns:
        bool: True if the file extension is allowed, False otherwise.
    """
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def check_secret_key() -> None:
    """
    Check if the provided API key is valid.

    Raises:
        ApiException: If the API key is invalid or missing.
    """
    secret_key = request.headers.get("X-API-Key")
    if not secret_key or secret_key != API_SECRET_KEY:
        api.abort(401, "Invalid or missing API Key")


def is_valid_csv_url(url: str) -> bool:
    """
    Validates if the given URL is a valid URL that ends with .csv.

    Args:
        url (str): The URL to be validated.

    Returns:
        bool: True if the URL is valid and ends with .csv, False otherwise.
    """
    # Regex pattern for validating a URL that ends with .csv
    pattern = re.compile(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+\.csv$"
    )
    return bool(pattern.match(url))


def require_api_key(func: Any) -> Any:
    """
    Decorator to require a valid API key for a function.

    Args:
        func (Any): The function to decorate.

    Returns:
        Any: The decorated function.
    """

    @wraps(func)
    def decorated(*args, **kwargs):
        check_secret_key()
        return func(*args, **kwargs)

    return decorated


csv_upload_parser = reqparse.RequestParser()
csv_upload_parser.add_argument("file", location="files", type="FileStorage")
csv_upload_parser.add_argument(
    "altname", type=str, required=False, help="Alternative name for the file"
)
csv_upload_parser.add_argument(
    "encoding", type=str, required=False, default="utf-8", help="File encoding"
)
csv_upload_parser.add_argument(
    "delimiter", type=str, required=False, default=",", help="CSV delimiter"
)
csv_import_parser = reqparse.RequestParser()
csv_import_parser.add_argument(
    "file_url", type=str, required=True, help="URL of the CSV file to import"
)
csv_import_parser.add_argument(
    "altname", type=str, required=False, help="Alternative name for the file"
)
csv_import_parser.add_argument(
    "encoding", type=str, required=False, default="utf-8", help="File encoding"
)
csv_import_parser.add_argument(
    "delimiter", type=str, required=False, default=",", help="CSV delimiter"
)


@api.route("/upload_csv")
class UploadCSV(Resource):
    """
    Resource endpoint for uploading and processing CSV files.

    Attributes:
        api : Flask-RESTX namespace for API documentation.
        csv_upload_parser : Request parser for CSV upload parameters.
        limiter : Rate limiter for restricting API requests.

    Methods:
        post(): Handles POST requests for uploading CSV files.
    """

    @api.doc(security="apikey")
    @api.expect(csv_upload_parser)
    @limiter.limit("2 per minute")
    @require_api_key
    def post(self) -> Tuple[Dict[str, str], int]:
        """
        Upload and process a CSV file.

        Returns:
            A tuple containing a message dictionary and HTTP status code.
        """
        file = request.files["file"]
        altname = request.args.get("altname")
        encoding = request.args.get("encoding")
        delimiter = request.args.get("delimiter")
        if validate_csv_params(encoding, delimiter) is False:
            return {"error": "Invalid encoding or delimiter"}, 400
        if file and allowed_file(file.filename):
            try:
                filename = (
                    f"{int(time.time())}_{altname}.csv"
                    if altname
                    else f"{int(time.time())}_{secure_filename(file.filename)}"
                )
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                file.save(file_path)
                event = Event(
                    original_url=None,
                    mode="upload",
                    altname=altname,
                    filepath=file_path,
                    encoding=encoding,
                    delimiter=delimiter,
                )
                db.session.add(event)
                db.session.commit()
                save_csv_to_db(file_path, encoding, delimiter, event.id)
                return {"message": "CSV data uploaded and imported successfully"}, 201
            except IOError as e:
                return {"error": f"File I/O error: {str(e)}"}, 500
            except BadRequest as e:
                return {"error": f"Bad request: {str(e)}"}, 400
            except SQLAlchemyError as e:
                return {"error": f"Database error: {str(e)}"}, 500
            except Exception as e:
                current_app.logger.error(
                    f"Unexpected error in UploadCSV: {str(e)}")
                return {"error": "An unexpected error occurred"}, 500
        else:
            return {"error": "Invalid file type. Allowed file types: csv"}, 400


@api.route("/import_csv")
class ImportCSV(Resource):
    """
    Resource endpoint for importing and processing CSV files from URLs.

    Attributes:
        api : Flask-RESTX namespace for API documentation.
        csv_import_parser : Request parser for CSV import parameters.
        limiter : Rate limiter for restricting API requests.

    Methods:
        post(): Handles POST requests for importing CSV files from URLs and saving them locally.
    """

    @api.doc(security="apikey")
    @api.expect(csv_import_parser)
    @limiter.limit("2 per minute")
    @require_api_key
    def post(self) -> Tuple[Dict[str, str], int]:
        """
        Query game data based on various filters.

        Returns:
            Tuple[Dict[str, Any], int]: A tuple containing query results and HTTP status code.
        """
        args = csv_import_parser.parse_args()
        file_url = args.get("file_url")
        altname = args.get("altname")
        encoding = args.get("encoding")
        delimiter = args.get("delimiter")
        if not is_valid_csv_url(file_url):
            return {
                "error": "Invalid file URL. URL must be valid and end with .csv"
            }, 400
        if validate_csv_params(encoding, delimiter) is False:
            return {"error": "Invalid encoding or delimiter"}, 400
        if not os.path.exists(UPLOAD_FOLDER):
            os.makedirs(UPLOAD_FOLDER)
        os.chmod(UPLOAD_FOLDER, 0o755)
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                filename = (
                    f"{int(time.time())}_{altname}.csv"
                    if altname
                    else f"{int(time.time())}_{secure_filename(file_url.rsplit('/', 1)[-1])}"
                )
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                with open(file_path, "wb") as f:
                    f.write(response.content)
                event = Event(
                    original_url=file_url,
                    mode="import",
                    altname=altname,
                    filepath=file_path,
                    encoding=encoding,
                    delimiter=delimiter,
                )
                db.session.add(event)
                db.session.commit()
                save_csv_to_db(file_path, encoding, delimiter, event.id)
                return {"message": "CSV data imported successfully from URL"}, 201
            return {
                "error": f"Failed to fetch file: {file_url}. Code: {response.status_code}"
            }, 400
        except RequestException as e:
            return {"error": f"Failed to fetch file: {str(e)}"}, 400
        except IOError as e:
            return {"error": f"File I/O error: {str(e)}"}, 500
        except SQLAlchemyError as e:
            return {"error": f"Database error: {str(e)}"}, 500
        except Exception as e:
            # Log the unexpected exception
            current_app.logger.error(
                f"Unexpected error in ImportCSV: {str(e)}")
            return {"error": "An unexpected error occurred"}, 500


@api.route("/query")
class QueryData(Resource):
    """
    Resource endpoint for importing and processing CSV files from URLs.

    Attributes:
        api : Flask-RESTX namespace for API documentation.
        csv_import_parser : Request parser for CSV import parameters.
        limiter : Rate limiter for restricting API requests.

    Methods:
        post(): Handles POST requests for importing CSV files from URLs.
    """

    @api.doc(
        params={
            "app_id": {"description": "App ID", "type": "integer"},
            "name": {"description": "Name of the game", "type": "string"},
            "release_date": {
                "description": "Release date of the game",
                "type": "string",
                "format": "date",
            },
            "required_age": {
                "description": "Required age to play the game",
                "type": "integer",
            },
            "price": {"description": "Price of the game", "type": "number"},
            "dlc_count": {"description": "Number of DLCs", "type": "integer"},
            "about_game": {"description": "Description of the game", "type": "string"},
            "supported_languages": {
                "description": "Supported languages",
                "type": "string",
            },
            "windows": {"description": "Windows support", "type": "boolean"},
            "mac": {"description": "Mac support", "type": "boolean"},
            "linux": {"description": "Linux support", "type": "boolean"},
            "positive": {
                "description": "Number of positive reviews",
                "type": "integer",
            },
            "negative": {
                "description": "Number of negative reviews",
                "type": "integer",
            },
            "score_rank": {"description": "Score rank of the game", "type": "integer"},
            "developers": {"description": "Developers of the game", "type": "string"},
            "publishers": {"description": "Publishers of the game", "type": "string"},
            "categories": {"description": "Categories of the game", "type": "string"},
            "genres": {"description": "Genres of the game", "type": "string"},
            "tags": {"description": "Tags of the game", "type": "string"},
            "before": {
                "description": "Filter games released before this date",
                "type": "string",
                "format": "date",
            },
            "after": {
                "description": "Filter games released after this date",
                "type": "string",
                "format": "date",
            },
            "min_price": {"description": "Minimum price of the game", "type": "number"},
            "max_price": {"description": "Maximum price of the game", "type": "number"},
            "cursor": {
                "description": "Cursor for pagination",
                "type": "integer",
                "default": 0,
            },
            "limit": {
                "description": "Limit for pagination",
                "type": "integer",
                "default": 10,
            },
        }
    )
    @limiter.limit("10 per minute")
    def get(self) -> Tuple[Dict[str, Any], int]:
        """
        Query game data based on various filters.

        Returns:
            Tuple[Dict[str, Any], int]: A tuple containing query results and HTTP status code.
        """
        filters = request.args.to_dict()
        cursor = int(filters.pop("cursor", 0))
        limit = int(filters.pop("limit", 10))
        try:
            results, total = query_data(filters, cursor, limit)
            return {
                "status": f"{total} found",
                "results": results,
                "cursor": cursor + limit if cursor + limit < total else None,
            }, 200
        except ValueError as e:
            return {"error": f"Invalid input: {str(e)}"}, 400
        except SQLAlchemyError as e:
            return {"error": f"Database error: {str(e)}"}, 500
        except Exception as e:
            # Log the unexpected exception
            current_app.logger.error(
                f"Unexpected error in QueryData: {str(e)}")
            return {"error": "An unexpected error occurred"}, 500


@api.route("/stats")
class StatsData(Resource):
    """
    Resource endpoint for retrieving statistical data for game numerical attributes.

    Attributes:
        api : Flask-RESTX namespace for API documentation.
        limiter : Rate limiter for restricting API requests.

    Methods:
        get(): Handles GET requests to retrieve statistical data based on specified aggregate function and column.
    """

    @api.doc(
        params={
            "aggregate": {
                "description": "Type of aggregate function",
                "type": "string",
                "enum": [
                    "all",
                    "min",
                    "max",
                    "median",
                    "mean",
                    "range",
                    "iqr",
                    "std_dev",
                    "variance",
                    "sum",
                    "count",
                    "percentiles",
                    "skewness",
                    "kurtosis",
                ],
                "required": True,
            },
            "column": {
                "description": "Column to apply aggregate function on (price, dlc_count, positive, negative)",
                "type": "string",
                "enum": ["all", "price", "dlc_count", "positive", "negative"],
                "required": True,
            },
        }
    )
    @limiter.limit("10 per minute")
    def get(self) -> Tuple[Dict[str, Any], int]:
        """
        Get statistical data for game numerical attributes.

        Returns:
            Tuple[Dict[str, Any], int]: A tuple containing statistical results and HTTP status code.
        """
        aggregate = request.args.get("aggregate")
        column = request.args.get("column")

        if aggregate not in [
            "all",
            "min",
            "max",
            "median",
            "mean",
            "range",
            "iqr",
            "std_dev",
            "variance",
            "sum",
            "count",
            "percentiles",
            "skewness",
            "kurtosis",
        ]:
            return {"error": "Invalid aggregate function"}, 400

        if column and column not in [
            "all",
            "price",
            "dlc_count",
            "positive",
            "negative",
        ]:
            return {"error": f"Column {column} does not exist or is not allowed"}, 400

        try:
            result = query_aggregate_data(aggregate, column)
            return {"result": result}, 200
        except ValueError as e:
            return {"error": f"Invalid input: {str(e)}"}, 400
        except SQLAlchemyError as e:
            return {"error": f"Database error: {str(e)}"}, 500
        except Exception as e:
            # Log the unexpected exception
            current_app.logger.error(
                f"Unexpected error in StatsData: {str(e)}")
            return {"error": "An unexpected error occurred"}, 500


@api.route("/similar_games")
class SimilarGames(Resource):
    """
    Resource endpoint for finding similar games based on a given game name.

    Attributes:
        api : Flask-RESTX namespace for API documentation.
        limiter : Rate limiter for restricting API requests.

    Methods:
        get(): Handles GET requests to find similar games based on a provided game name.
    """

    @api.doc(
        params={
            "name": {
                "description": "Name of the game to find similar games",
                "type": "string",
                "required": True,
            }
        }
    )
    @limiter.limit("10 per minute")
    def get(self) -> Tuple[Dict[str, Any], int]:
        """
        Find similar games based on a given game name.

        Returns:
            A tuple containing similar games results and HTTP status code.
        """
        game_name = request.args.get("name", "")
        if game_name:
            return get_similar_games(game_name), 200
        return {"error": "Please provide a game name"}, 400


@api.route("/ping")
class Ping(Resource):
    """
    Resource endpoint for testing server connectivity.
    """

    def get(self):
        """
        Ping the server.

        Returns:
            str: A simple "pong" response.
        """
        return {"response": "pong"}
