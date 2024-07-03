import os
import requests
from flask import request
from flask_restx import Namespace, Resource, fields,reqparse
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

csv_upload_parser = reqparse.RequestParser()
csv_upload_parser.add_argument('file', location='files', type='FileStorage')

csv_import_parser = reqparse.RequestParser()
csv_import_parser.add_argument('file_url', type=str, help='URL of the CSV file to import')


@api.route('/upload_csv')
class UploadCSV(Resource):
    @api.expect(csv_upload_parser)
    @limiter.limit("2 per minute")
    def post(self):
        file = request.files['file']

        if file and allowed_file(file.filename):
            try:
                filename = secure_filename(file.filename)
                file.save(os.path.join(UPLOAD_FOLDER, filename))
                save_csv_to_db(os.path.join(UPLOAD_FOLDER, filename))
                return {"message": "CSV data uploaded and imported successfully"}, 201
            except Exception as e:
                return {"error": str(e)}, 500
        else:
            return {"error": "Invalid file type. Allowed file types: csv"}, 400

@api.route('/import_csv')
class ImportCSV(Resource):
    @api.expect(csv_import_parser)
    @limiter.limit("2 per minute")
    def post(self):
        args = csv_import_parser.parse_args()
        file_url = args.get('file_url')

        try:
            response = requests.get(file_url, stream=True)
            if response.status_code == 200:
                filename = secure_filename(file_url.rsplit('/', 1)[-1])
                file_path = os.path.join(UPLOAD_FOLDER, filename)

                with open(file_path, 'wb') as f:
                    f.write(response.content)

                save_csv_to_db(file_path)

                return {"message": "CSV data imported successfully from URL"}, 201
            else:
                return {"error": f"Failed to fetch file from URL: {file_url}. Status code: {response.status_code}"}, 400
        except Exception as e:
            return {"error": str(e)}, 500


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
