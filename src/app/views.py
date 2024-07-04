import os
import time
from datetime import datetime
from functools import wraps

import requests
from dotenv import load_dotenv
from flask import request
from flask_restx import Api, Namespace, Resource, fields, reqparse
from werkzeug.utils import secure_filename

from . import db, limiter
from .models import Event, GameData
from .utils import query_aggregate_data, save_csv_to_db

load_dotenv()
authorizations = {"apikey": {"type": "apiKey", "in": "header", "name": "X-API-Key"}}
api = Namespace(
    "api",
    description="Game data operations",
    authorizations=authorizations,
    security="apikey",
)
csv_upload = api.model(
    "CSVUpload",
    {"csv_file": fields.Raw(required=True, description="The CSV file to upload")},
)
UPLOAD_FOLDER = "uploads/"
ALLOWED_EXTENSIONS = {"csv"}


def validate_csv_params(encoding, delimiter):
    valid_encodings = ["utf-8", "ascii", "iso-8859-1"]  # Add more as needed
    if encoding not in valid_encodings or len(delimiter) != 1:
        return False
    return True


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


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
    "file_url", type=str, help="URL of the CSV file to import"
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


def check_secret_key():
    secret_key = request.headers.get("X-API-Key")
    if not secret_key or secret_key != "test":
        api.abort(401, "Invalid or missing API Key")


def require_api_key(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        check_secret_key()
        return func(*args, **kwargs)

    return decorated


@api.route("/upload_csv")
class UploadCSV(Resource):
    @api.doc(security="apikey")
    @api.expect(csv_upload_parser)
    @limiter.limit("2 per minute")
    @require_api_key
    def post(self):
        file = request.files["file"]
        altname = request.args.get("altname")
        encoding = request.args.get("encoding")
        delimiter = request.args.get("delimiter")
        if validate_csv_params(encoding, delimiter) == False:
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
            except Exception as e:
                return {"error": str(e)}, 500
        else:
            return {"error": "Invalid file type. Allowed file types: csv"}, 400


@api.route("/import_csv")
class ImportCSV(Resource):
    @api.doc(security="apikey")
    @api.expect(csv_import_parser)
    @limiter.limit("2 per minute")
    @require_api_key
    def post(self):
        args = csv_import_parser.parse_args()
        file_url = args.get("file_url")
        altname = args.get("altname")
        encoding = args.get("encoding")
        delimiter = args.get("delimiter")
        if validate_csv_params(encoding, delimiter) == False:
            return {"error": "Invalid encoding or delimiter"}, 400
        try:
            response = requests.get(file_url, stream=True)
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
            else:
                return {
                    "error": f"Failed to fetch file from URL: {file_url}. Status code: {response.status_code}"
                }, 400
        except Exception as e:
            return {"error": str(e)}, 500


@api.route("/query")
class QueryData(Resource):
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
    def get(self):
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
        except Exception as e:
            return {"error": str(e)}, 500


def query_data(filters, cursor, limit):
    query = db.session.query(GameData)
    for key, value in filters.items():
        if key == "before":
            date_value = datetime.strptime(value, "%Y-%m-%d").date()
            query = query.filter(GameData.release_date < date_value)
        elif key == "after":
            date_value = datetime.strptime(value, "%Y-%m-%d").date()
            query = query.filter(GameData.release_date > date_value)
        elif key == "release_date":
            date_value = datetime.strptime(value, "%Y-%m-%d").date()
            query = query.filter(GameData.release_date == date_value)
        else:
            if hasattr(GameData, key):
                # Convert the value to the appropriate type
                column = getattr(GameData, key)
                if column.property.columns[0].type.python_type in (int, float):
                    value = column.property.columns[0].type.python_type(value)
                    query = query.filter(column == value)
                elif column.property.columns[0].type.python_type is str:
                    query = query.filter(column.contains(value))
                elif column.property.columns[0].type.python_type is bool:
                    value = value.lower() in ["true", "1", "yes"]
                    query = query.filter(column == value)
                else:
                    query = query.filter(column == value)
            else:
                print(f"Attribute {key} not found in GameData model")
    total = query.count()
    query = query.offset(cursor).limit(limit)
    results = []
    for row in query.all():
        results.append(
            {
                "AppID": row.app_id,
                "Name": row.name,
                "Release date": row.release_date,
                "Required age": row.required_age,
                "Price": row.price,
                "DLC count": row.dlc_count,
                "About the game": row.about_game,
                "Supported languages": row.supported_languages,
                "Windows": row.windows,
                "Mac": row.mac,
                "Linux": row.linux,
                "Positive": row.positive,
                "Negative": row.negative,
                "Score rank": row.score_rank,
                "Developers": row.developers,
                "Publishers": row.publishers,
                "Categories": row.categories,
                "Genres": row.genres,
                "Tags": row.tags,
            }
        )
    return results, total


@api.route("/stats")
class StatsData(Resource):
    @api.doc(
        params={
            "aggregate": {
                "description": "Type of aggregate function (max, min, mean)",
                "type": "string",
                "enum": ["all", "max", "min", "mean"],
                "required": True,
            },
            "column": {
                "description": "Column to apply the aggregate function on (price, dlc_count, positive, negative)",
                "type": "string",
                "enum": ["all", "price", "dlc_count", "positive", "negative"],
                "required": True,
            },
        }
    )
    @limiter.limit("10 per minute")
    def get(self):
        aggregate = request.args.get("aggregate")
        column = request.args.get("column")

        if aggregate not in ["all", "total", "max", "min", "mean"]:
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
            return {"error": str(e)}, 400
        except Exception as e:
            return {"error": str(e)}, 500
