"""
run.py

This script creates and runs the Flask application.

It imports the create_app function from the app module to initialize the Flask app,
and starts the Flask development server when run directly.

Usage:
    python run.py

"""
from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
