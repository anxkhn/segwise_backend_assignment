import os
import requests
from flask import request
from flask_restx import Namespace, Resource, fields,reqparse
from werkzeug.utils import secure_filename
import time
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

def validate_csv_params(encoding, delimiter):
    valid_encodings = ['utf-8', 'ascii', 'iso-8859-1']  # Add more as needed
    if encoding not in valid_encodings or len(delimiter) != 1:
        return False
    return True

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

csv_upload_parser = reqparse.RequestParser()
csv_upload_parser.add_argument('file', location='files', type='FileStorage')
csv_upload_parser.add_argument('altname', type=str, required=False, help='Alternative name for the file')
csv_upload_parser.add_argument('encoding', type=str, required=False, default='utf-8', help='File encoding')
csv_upload_parser.add_argument('delimiter', type=str, required=False, default=',', help='CSV delimiter')


csv_import_parser = reqparse.RequestParser()
csv_import_parser.add_argument('file_url', type=str, help='URL of the CSV file to import')
csv_import_parser.add_argument('altname', type=str, required=False, help='Alternative name for the file')
csv_import_parser.add_argument('encoding', type=str, required=False, default='utf-8', help='File encoding')
csv_import_parser.add_argument('delimiter', type=str, required=False, default=',', help='CSV delimiter')


@api.route('/upload_csv')
class UploadCSV(Resource):
    @api.expect(csv_upload_parser)
    @limiter.limit("2 per minute")
    def post(self):
        file = request.files['file']
        altname = request.args.get('altname')
        encoding = request.args.get('encoding')
        delimiter = request.args.get('delimiter')
        if validate_csv_params(encoding, delimiter) == False:
            return {"error": "Invalid encoding or delimiter"}, 400

        if file and allowed_file(file.filename):
            try:
                # Append unix epoch to filename to avoid conflicts
                filename = f"{int(time.time())}_{altname}.csv" if altname else f"{int(time.time())}_{secure_filename(file_url.rsplit('/', 1)[-1])}"
                file.save(os.path.join(UPLOAD_FOLDER, filename))
                save_csv_to_db(os.path.join(UPLOAD_FOLDER, filename),encoding,delimiter)
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
        altname = args.get('altname')
        encoding = args.get('encoding')
        delimiter = args.get('delimiter')
        if validate_csv_params(encoding, delimiter) == False:
            return {"error": "Invalid encoding or delimiter"}, 400

        try:
            response = requests.get(file_url, stream=True)
            if response.status_code == 200:
                # Append unix epoch to filename to avoid conflicts
                filename = f"{int(time.time())}_{altname}.csv" if altname else f"{int(time.time())}_{secure_filename(file_url.rsplit('/', 1)[-1])}"
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                save_csv_to_db(file_path,encoding,delimiter)
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
