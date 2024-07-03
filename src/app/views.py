import os

from flask import request
from flask_restx import Namespace, Resource, fields
from werkzeug.utils import secure_filename

from . import db, limiter
from .models import GameData
from .utils import save_csv_to_db

api = Namespace("api", description="Game data operations")

csv_upload = api.model(
    "CSVUpload",
    {"csv_file": fields.Raw(
        required=True, description="The CSV file to upload")},
)

UPLOAD_FOLDER = "uploads/"
ALLOWED_EXTENSIONS = {"csv"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


csv_upload_parser = api.parser()
csv_upload_parser.add_argument(
    "file", location="files", type="FileStorage", required=True
)


@api.route("/upload")
class UploadCSV(Resource):
    @api.expect(csv_upload_parser)
    @limiter.limit("2 per minute")
    def post(self):
        file = request.files["file"]

        if file and allowed_file(file.filename):
            try:
                filename = secure_filename(file.filename)
                file.save(os.path.join(UPLOAD_FOLDER, filename))
                save_csv_to_db(os.path.join(UPLOAD_FOLDER, filename))
                return {"message": "CSV data uploaded successfully"}, 201
            except Exception as e:
                return {"error": str(e)}, 500
        else:
            return {"error": "Invalid file type. Allowed file types: csv"}, 400


@api.route("/query")
class QueryData(Resource):
    @limiter.limit("10 per minute")
    def get(self):
        filters = request.args.to_dict()
        try:
            results = query_data(filters)
            return results, 200
        except Exception as e:
            return {"error": str(e)}, 500


def query_data(filters):
    query = db.session.query(GameData)

    for key, value in filters.items():
        if hasattr(GameData, key):
            column = getattr(GameData, key)
            # For numerical fields, exact match
            if column.property.columns[0].type.python_type in (int, float):
                query = query.filter(column == value)
            # For string fields, substring match
            elif column.property.columns[0].type.python_type is str:
                query = query.filter(column.contains(value))
            # For other types, exact match
            else:
                query = query.filter(column == value)

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

    return results
