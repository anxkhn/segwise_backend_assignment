# Segwise.ai Backend Assignment

#### Starting : 9pm 3rd July 2024

#### Deadline : 11am 8th July 2024

<details>
  <summary><strong>Backend Assignment Details</strong></summary>
  
## Intern Assignment

Consider you are working for a game analytics company and have been tasked to create a web service where data analysts can upload a csv file and then run analysis on it. As an enterprising intern at that company you decide to solve this problem on your own. Your mission if you chose to accept is to implement the below three parts.

- Api
  - Api to upload csv data
    - Api to analyse data based on filters
- DB
  - Store the csv data.
  - Sqlite or clickhouse
- Deployment
  - Create a docker image so that code/db can be deployed anywhere.
  - Deploy the system to a **free tier of any cloud provider** for analysts to try.

You can use any online resource at your disposal like google, chatgpt for completing the assignment but make sure you understand what you are doing. We would ask you to explain your choices in next rounds.

**Judging criteria**

You will be judged on 4 main elements in order.

- Completeness of solution.
- Completeness of instructions and documentation you write.
- Code readability and maintainability.
- Performance of the solution.
- Cost effectiveness.

**Submission**

- Submit **link to the github project**. No zip files in google drive or email would be accepted. **Add** **shobhit@segwise.ai as collaborator** to your github project. Any documentation or instructions you want to provide should be in a README in the repo itself.
- Also submit the **link to the deployed solution**.
- Please also submit sample api request/response for the apis. Either in README or separately in the email.
- Along with this, also submit the cost of running your system in production 24x7 for 30 days, assuming one file upload and 100 queries a day. You can send your calculations/estimates on the submission email.
- Submission needs to be made by **11 am IST on 8th July.** Sorry but because we want to hire ASAP, we would not be able to extend deadline.

**API**

- Uploading csv endpoint
  - This endpoint should accept a single csv file link as input. The link will be open to public and does not require any authentication. The file could be as big as 150 MB in size.
  - You should save this file in some format as all future queries will be querying data from this file.
  - The exact api design and how you want to take input params and values is your choice.
    - Columns as well as data types and format in final csv will remain same as the sample csv. Sample csv has 100 rows to give you an idea about the data.
    - sample file: [https://docs.google.com/spreadsheets/d/1ShbFMzRUuIJY8amTA58UuEHwsc3UmAnd_LzduB wcBhE/edit?usp=sharing](https://docs.google.com/spreadsheets/d/1ShbFMzRUuIJY8amTA58UuEHwsc3UmAnd_LzduBwcBhE/edit?usp=sharing)
    - BONUS: if you can add some simple authentication so not everybody can make a call.
- Query endpoint
  - This endpoint should accept parameters with values to query the data from csv. The service should support query by any field in the csv. For numerical field, exact match should work. For string fields, response should contain anything that matches the input as a substring. For eg:
    - For a age, age=20 would mean return all records where age is exactly 20.
    - But for string field like a name, substring match should work. So name=Raj should match Raj, Rajesh, Rajan …. and so on.
    - Date fields should have exact match like numbers.
  - The exact api design and how you want to take input params and values is your choice.
    - BONUS: if you can have a simple UI to query data.
  - BONUS: if you can allow aggregate searches like records where total (x) > 100 or get max/min/mean value of a numerical column etc.
  - BONUS: if you can allow greater than and less than searches for dates.
    - BONUS: if you can add some simple authentication so not everybody can make a call.

**DB**

- Save data in a sqlite or clickhouse. Clickhouse might help with performance but might need more resources to run. Based on the free limits of the cloud provider you chose, you should decide the DB type.
- You are allowed to use the cloud version of these DBs if you can get a free trial anywhere but remember to cancel it after the assignment review is done.
- You are encouraged to decide and come up with creative data model for best performance and insights given the data.

**Deployment**

- Create a docker image for your submission. Commit the dockerfile in the repo with rest of the code.
- Deploy your solution on a free tier of any cloud provider. Make sure you are not getting charged for it. There will be no reimbursement or cloud credits provided.

Time is of the essence. Happy building.

</details>

This repository contains the code for the backend assignment for the Segwise.ai internship. The project is focused on creating an API to upload CSV data, store it in a database, and run analysis based on filters. The application is built using Flask, SQLite, and Docker, and it is deployed on PythonAnywhere.

## Tech Stack

- **Language**: Python 3.10
- **Framework**: Flask
- **Database**: SQLite (SQLAlchemy)

## Architecture Overview

The application follows a modular architecture:

1. **API Layer**: Implemented using Flask and Flask-RESTx, providing endpoints for CSV upload, data querying, and statistical analysis.
2. **Database Layer**: Utilizes SQLAlchemy with SQLite for data persistence.
3. **Data Processing Layer**: Employs Pandas, NumPy, and scikit-learn for data manipulation and analysis.
4. **Rate Limiting**: Implements Flask-Limiter to prevent API abuse.
5. **Authentication**: Basic API key authentication for secure endpoints.

## Design Choices

1. **Flask /w RestX**:

Flask itself is lightweight and flexible, perfect for building APIs. Flask-RESTX adds powerful tools for building RESTful APIs, which is exactly what we need for data upload and querying.

Flask-RESTX is actually a well-maintained fork of Flask-RESTful, which itself stemmed from Flask-RESTplus. This means it inherits the best aspects of both, plus a key advantage: built-in Swagger UI for API documentation. This will save us time and make it easier for analysts to understand how to interact with the service.

2. **SQLite**:

SQLite is a lightweight, file-based, and self-contained database management system. It is chosen for its simplicity and ease of use in this project. A relational database is a no brainer for its ACID properties and strong validation, ORM, as there is a lot of structured data insertion in this project.

Moreover, it will perform faster than non-relational databases, as most relational databases and some NoSQL databases (e.g., MongoDB) use B-trees, which are better for reads than writes. SQLite was chosen as it is file-based and eases deployment and development.

Clickhouse might help with performance but might need more resources to run. I wanted to keep the project simple and easy to run without any external dependencie, so I chose SQLite.

3. **Cursor-based Pagination**:

Cursor-based pagination is implemented to retrieve gamedata in a paginated manner. This approach is more efficient than offset-based pagination, especially when dealing with large datasets.

It was specifically chosen in cases where a user requests a large number of games, as offset-based pagination can become slow and inefficient. Cursor-based pagination allows for faster retrieval of data and better performance.

Cursor-based pagination simplifies that. For further reading on cursor pagination, see [here](https://dev.to/appwrite/this-is-why-you-should-use-cursor-pagination-4nh5).

4. **Modular Structure**:

The application is organized into separate modules (models, views, utils) for better maintainability and scalability.

5. **Data Processing**:

Utilizes Pandas and NumPy for efficient data manipulation and analysis.

6. **ML - Similarity Search**:

Implements TF-IDF and cosine similarity for finding similar games.

7. **Rate Limiting**:

Prevents API abuse and ensures fair usage.
Higher restrictions are applied on the `/upload_csv` and import_csv endpoints to prevent abuse and ensure that the system remains responsive for all users. There are no rate limits on the `/query` and `/stats` endpoints, as they are read-only and do not modify the database.

8. **Docker**:

Docker is used to containerize the application, making it easier to deploy and run in different environments.

## Getting Started

To run the project locally, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/anxkhn/segwise_backend_assignment.git
```

2. Create a `.env` file
   - Rename `.example.env` to `.env`.

- Fill in:

  - `DATABASE_URL` (Database connection string)
  - `SECRET_KEY` (Secret key for Flask app)
  - `RATELIMIT_DEFAULT` (Rate limit for API endpoints)
  - `API_SECRET_KEY` (Secret key for API authentication)

## Running the application

This application can be run either locally or using Docker. Choose the method that best suits your environment.

**Setup**

1. **Prerequisites:**
   - Python 3.10+
   - pip (package installer)
2. **Clone the repository:**
   ```bash
   git clone https://github.com/anxkhn/segwise_backend_assignment
   ```

**Virtual Environment Setup**

3. **Create a virtual environment:**

   ```bash
   cd segwise_backend_assignment
   python -m venv env
   ```

4. **Activate the virtual environment:**

   - On Windows:
     ```bash
     .\env\Scripts\activate
     ```
   - On macOS and Linux:
     ```bash
     source env/bin/activate
     ```

5. **Install dependencies within the virtual environment:**
   ```bash
   pip install -r requirements.txt
   ```

**Local Setup**

7. **Run the application within the virtual environment:**

   ```bash
   cd src && python run.py
   ```

   The API will be accessible at `http://localhost:5123`.

With these instructions, users will be guided on how to set up and run the application within a virtual environment.

**OR: Running with Docker**

3. **Prerequisites:**

   - Docker installed on your system: [https://www.docker.com/](https://www.docker.com/)
   - Docker Compose installed: [https://docs.docker.compose/install/](https://docs.docker.compose/install/)

4. **Build the docker image:**

```bash
   docker compose build
```

This will build a Docker image, start the necessary services (app and Redis), and run the application on port 5123.

5. Run the Docker container:

```bash
docker compose up
```

The API will be accessible at `http://localhost:5123`.

### Swagger UI Integration

#### `/docs`

Swagger UI provides an interactive documentation interface for exploring the API endpoints. It offers a user-friendly way to understand the available endpoints, their parameters, and expected responses.

- **Screenshot 1**: Overview of the Swagger UI interface.
  ![Screenshot 1](docs/screenshots/screenshot1.png)

#### Ping (HealthCheck)

- **Screenshot 2**: Shows API key authentication for the `/api/upload_csv` and `/api/import_csv` endpoints.
  ![Screenshot 2](docs/screenshots/screenshot3.png)

- **Screenshot 3**: Shows the endpoint for health check (`GET /api/ping`).
  ![Screenshot 6](docs/screenshots/screenshot6.png)
  ![Screenshot 3](docs/screenshots/screenshot21.png)

#### Import CSV

- **Screenshot 4,5 & 6**: Illustrates the endpoint for importing CSVs (`GET /api/import_csv`) in Swagger UI.
  ![Screenshot 4](docs/screenshots/screenshot4.png)

  ![Screenshot 5](docs/screenshots/screenshot5.png)

- **Screenshot 7**: Shows an example of a response when fetching queries using the `/api/query` endpoint.
  ![Screenshot 7](docs/screenshots/screenshot7.png)
  ![Screenshot 8](docs/screenshots/screenshot8.png)
  ![Screenshot 9](docs/screenshots/screenshot9.png)
  ![Screenshot 10](docs/screenshots/screenshot10.png)

  - **Screenshot 12**: Shows an example of a response when fetching games released between certain dates using the `/api/query` endpoint.

  ![Screenshot 12](docs/screenshots/screenshot12.png)

- **Screenshot 14**: Shows an example of a response when fetching games demonstrating multiple queries using the `/api/query` endpoint.

  ![Screenshot 14](docs/screenshots/screenshot14.png)

  - **Screenshot 15**: Shows an example of a response when fetching games demonstrating finding games using fizzy search and finding similar games using cosine similarity using the `/api/similar_games` endpoint.

  ![Screenshot 15](docs/screenshots/screenshot15.png)
  ![Screenshot 16](docs/screenshots/screenshot16.png)

- **Screenshot 17**: Shows an example of a response when fetching games demonstrating fetching all stats of all games in database using `/api/stats` endpoint.

  ![Screenshot 17](docs/screenshots/screenshot17.png)
  ![Screenshot 18](docs/screenshots/screenshot18.png)

- **Screenshot 19**: Illustrates the endpoint for importing CSVs via user upload (`GET /api/upload_csv`) in Swagger UI.

  ![Screenshot 19](docs/screenshots/screenshot19.png)

### API Endpoints

#### Health Check

- **Endpoint**: `GET /api/ping`
- **Purpose**: This endpoint serves as a simple health check for the API. It returns a "Pong!" message, indicating that the API is operational.

### 1. Upload CSV

- **Endpoint**: `/api/upload_csv`
- **Method**: POST
- **Description**: Upload a CSV file containing game data.
- **Parameters**:
  - `file`: The CSV file to upload (multipart/form-data)
  - `altname`: Alternative name for the file (optional)
  - `encoding`: File encoding (default: utf-8)
  - `delimiter`: CSV delimiter (default: ,)
- **Authentication**: Requires API key in the `X-API-Key` header

Note: The `upload_csv` endpoint is used to upload a CSV file containing game data. The file should be in the same format as the sample CSV provided, with the same columns and data types. The endpoint accepts a CSV file as a multipart/form-data request, along with optional parameters for alternative name, encoding, and delimiter. The API key is required in the `X-API-Key` header for authentication. Size limit is set to 150MB.

### 2. Import CSV from URL

- **Endpoint**: `/api/import_csv`
- **Method**: POST
- **Description**: Import a CSV file from a given URL.
- **Parameters**:
  - `file_url`: URL of the CSV file to import
  - `altname`: Alternative name for the file (optional)
  - `encoding`: File encoding (default: utf-8)
  - `delimiter`: CSV delimiter (default: ,)
- **Authentication**: Requires API key in the `X-API-Key` header

Note: The `import_csv` endpoint is used to import CSV files from a URL. This can be useful when the file is hosted externally and needs to be imported into the system. The url must be publicly accessible, and follow the same format as the sample CSV, and the url validation is in place. Size limit is set to 150MB.

### 3. Query Data

- **Endpoint**: `/api/query`
- **Method**: GET
- **Description**: Query game data based on various filters.
- **Parameters**:

  - `app_id` (integer): Filter by the game's App ID
  - `name` (string): Filter by the game's name (substring match)
  - `release_date` (string, format: YYYY-MM-DD): Filter by exact release date
  - `required_age` (integer): Filter by the required age to play the game
  - `price` (number): Filter by the exact price of the game
  - `dlc_count` (integer): Filter by the number of DLCs
  - `about_game` (string): Filter by the game's description (substring match)
  - `supported_languages` (string): Filter by supported languages (substring match)
  - `windows` (boolean): Filter by Windows support (true/false)
  - `mac` (boolean): Filter by Mac support (true/false)
  - `linux` (boolean): Filter by Linux support (true/false)
  - `positive` (integer): Filter by the number of positive reviews
  - `negative` (integer): Filter by the number of negative reviews
  - `score_rank` (integer): Filter by the game's score rank
  - `developers` (string): Filter by developers (substring match)
  - `publishers` (string): Filter by publishers (substring match)
  - `categories` (string): Filter by categories (substring match)
  - `genres` (string): Filter by genres (substring match)
  - `tags` (string): Filter by tags (substring match)
  - `before` (string, format: YYYY-MM-DD): Filter games released before this date
  - `after` (string, format: YYYY-MM-DD): Filter games released after this date
  - `min_price` (number): Filter games with price greater than or equal to this value
  - `max_price` (number): Filter games with price less than or equal to this value
  - `cursor` (integer, default: 0): Cursor for pagination
  - `limit` (integer, default: 10): Number of results to return per page

- **Notes**:

  - For string fields (name, about_game, supported_languages, developers, publishers, categories, genres, tags), the API performs a case-insensitive substring match.
  - For boolean fields (windows, mac, linux), use "true" or "false" (case-insensitive).
  - Date filters (`before`, `after`, `release_date`) should be in YYYY-MM-DD format.
  - Numeric filters (app_id, required_age, price, dlc_count, positive, negative, score_rank) perform exact matches unless using `min_price` or `max_price`.
  - The `cursor` and `limit` parameters are used for pagination. The API returns a `cursor` value in the response for the next page of results.

- **Example Request**:

  ```
  GET /api/query?name=Portal&min_price=5&max_price=20&after=2010-01-01&limit=5
  ```

- **Example Response**:
  ```json
  {
    "status": "10 found",
    "results": [
      {
        "app_id": 400,
        "name": "Portal",
        "release_date": "2007-10-10",
        "price": 9.99,
        ...
      },
      ...
    ],
    "cursor": 5
  }
  ```

### 4. Statistical Data

- **Endpoint**: `/api/stats`
- **Method**: GET
- **Description**: Retrieve statistical data for game numerical attributes.
- **Parameters**:

  - `aggregate` (string, required): Type of aggregate function to apply. Valid values are:
    - `all`: Returns all available statistics
    - `min`: Minimum value
    - `max`: Maximum value
    - `median`: Median value
    - `mean`: Average value
    - `range`: Range (difference between max and min)
    - `iqr`: Interquartile range
    - `std_dev`: Standard deviation
    - `variance`: Variance
    - `sum`: Sum of all values
    - `count`: Count of non-null values
    - `percentiles`: 25th, 50th, and 75th percentiles
    - `skewness`: Skewness of the distribution
    - `kurtosis`: Kurtosis of the distribution
  - `column` (string, required): Column to apply the aggregate function on. Valid values are:
    - `all`: Applies the aggregate function to all numeric columns
    - `price`: Game price
    - `dlc_count`: Number of DLCs
    - `positive`: Number of positive reviews
    - `negative`: Number of negative reviews

- **Notes**:

  - The `all` option for the `column` parameter will return statistics for all numeric columns (price, dlc_count, positive, negative).
  - When using `all` for both `aggregate` and `column`, the API will return all available statistics for all numeric columns.
  - Some aggregate functions which involves scipy (kurtosis, skew) is computationally expensive and is omitted in lite deployed version.

- **Example Request**:

  ```
  GET /api/stats?aggregate=all&column=price
  ```

- **Example Response**:

  ```json
  {
    "result": {
      "price": {
        "min": 0.0,
        "max": 59.99,
        "median": 9.99,
        "mean": 12.34,
        "range": 59.99,
        "iqr": 15.0,
        "std_dev": 11.25,
        "variance": 126.56,
        "sum": 12340.0,
        "count": 1000,
        "percentiles": {
          "25th": 4.99,
          "50th": 9.99,
          "75th": 19.99
        },
        "skewness": 1.23,
        "kurtosis": 2.45
      }
    }
  }
  ```

- **Example Request** (Multiple columns):

  ```
  GET /api/stats?aggregate=mean&column=all
  ```

  `

- **Example Response** (Multiple columns):
  ```json
  {
    "result": {
      "price": {
        "mean": 12.34
      },
      "dlc_count": {
        "mean": 2.5
      },
      "positive": {
        "mean": 5000.75
      },
      "negative": {
        "mean": 500.25
      }
    }
  }
  ```

## Deployed Version

A live version of the application is available at [https://segwiseaibackend.pythonanywhere.com/docs](https://segwiseaibackend.pythonanywhere.com/docs).

The lite version of the application is deployed on PythonAnywhere, which can not support the full functionality of the application such as ML based Game Similarity and large CSV uploads.

The source code for stripped down version of the application is available in the `pythonanywhere` branch of the repository.

Please be mindful of the limitations of PythonAnywhere.

Upon accessing the deployed version, please note the following:

1. **Cold Start Delay**: There might be a delay of up to 5 seconds for the initial response due to the nature of free services on PythonAnywhere. This delay occurs during the startup of the server, please refresh once if you face this issue.

2. **Health Check**: To ensure that the server is operational, you can access the `/api/ping` endpoint [here](https://segwiseaibackend.pythonanywhere.com/api/ping). A successful response indicates that the server is healthy and operational.

Please note that the deployed version might experience frequent downtime or performance issues due to resource limitations in the free tier. Therefore, it is **not recommeneded** to explore the API endpoints and functionality of the project without setting up the application locally.

## File Structure and Breakdown

```
src/
├── app/
│   ├── __init__.py    # Application factory and configuration
│   ├── models.py      # Database models (Event, GameData)
│   ├── utils.py       # Utility functions for data processing and analysis
│   └── views.py       # API endpoint definitions and request handling
├── config.py          # Configuration settings
├── requirements.txt   # Project dependencies
└── run.py             # Application entry point
```

### Breakdown of Key Files

1. **app/**init**.py**:

   - Creates and configures the Flask application
   - Initializes database, migrations, rate limiting, and API

2. **app/models.py**:

   - Defines database models (Event and GameData)
   - Includes methods for data serialization

3. **app/utils.py**:

   - Contains utility functions for data processing and analysis
   - Implements functions for loading game data, finding similar games, and performing statistical analysis

4. **app/views.py**:

   - Defines API endpoints using Flask-RESTx
   - Implements request handling and response generation for each endpoint

5. **config.py**:

   - Stores configuration settings for the application
   - Includes database URI, secret keys, and rate limit settings

6. **requirements.txt**:

   - Lists all Python dependencies required for the project

7. **run.py**:
   - Serves as the entry point for running the application

Certainly! I'll add a section to the README that concisely lists all implemented features, bonus items, and handled cases using emojis. Here's how it could look:

## Implementation Checklist

### Core Requirements

✅ CSV file upload API  
✅ CSV file import from URL API  
✅ Data querying API with multiple filters  
✅ SQLite database for data storage  
✅ Docker containerization  
✅ Deployment to free tier cloud provider

### Bonus Features

✅ Simple authentication (API key)  
✅ Aggregate searches (max/min/mean of numerical columns)  
✅ Greater than and less than searches for dates  
✅ Substring matching for string fields  
✅ Pagination support for query results

### Additional Implementations

✅ Rate limiting to prevent API abuse  
✅ Detailed error handling and logging  
✅ Machine learning-based similarity search for games  
✅ Comprehensive API documentation  
✅ Statistical analysis endpoint

### Edge Cases Handled

✅ Large file uploads (up to 150MB)  
✅ Invalid CSV formats or data  
✅ Malformed API requests  
✅ Database connection errors  
✅ File I/O errors during CSV processing

### Code Quality

✅ Modular and maintainable code structure  
✅ PEP 8 compliant (9.46/10 pylint score)  
![Screenshot 20](docs/screenshots/screenshot20.png)
✅ Comprehensive inline code documentation  
✅ Type hinting for improved code readability  
✅ Efficient data processing using Pandas and NumPy

## AWS Cost Estimation

This section provides an estimated monthly cost for running the Game Analytics Web Service on AWS, assuming one file upload and 100 queries per day.

### Assumptions:

- 1 file upload (up to 150MB) per day
- 100 queries per day
- 30 days of operation per month
- Using AWS Free Tier where possible

### AWS Services Used:

1. **EC2 (Elastic Compute Cloud)**

   - t2.micro instance (Free Tier eligible)
   - 1 vCPU, 1 GiB memory
   - Cost: $0.00 (Free Tier) or ~$8.50/month after Free Tier

2. **RDS (Relational Database Service) for SQLite alternative**

   - db.t3.micro instance (Free Tier eligible for 12 months)
   - 1 vCPU, 1 GiB memory
   - 20 GB of storage
   - Cost: $0.00 (Free Tier) or ~$12.50/month after Free Tier

3. **S3 (Simple Storage Service)**

   - Used for storing CSV files
   - 5 GB storage (150MB \* 30 days + buffer)
   - 100 GET requests per day (for queries)
   - 1 PUT request per day (for uploads)
   - Cost: ~$0.12/month (S3 Standard storage) + ~$0.00004/month (requests)

4. **Data Transfer**
   - Assuming 1GB/month outbound (conservative estimate)
   - Cost: $0.00 (Free Tier includes 1GB/month) or $0.09/GB after Free Tier

### Total Estimated Monthly Cost:

- During Free Tier period (first 12 months): **$0.12**
- After Free Tier period: **$21.12**

### Notes:

1. This estimation assumes optimal use of AWS Free Tier offerings. Actual costs may vary based on usage patterns and any changes to AWS pricing.
2. The estimation does not include potential costs for AWS support plans, data transfer exceeding Free Tier limits, or additional services that might be added for scaling or improved performance (e.g., Elastic Load Balancing, CloudFront).
3. Costs can be optimized by:
   - Using reserved instances for long-term use
   - Implementing auto-scaling to handle variable loads
   - Optimizing data storage and transfer patterns

## Additional Notes and Future Enhancements

While the current implementation meets the assignment requirements and includes several bonus features, there are always opportunities for further enhancements. Some potential improvements and extensions that could be considered in the future include:

1. **Caching Layer**: Implementing Redis for caching frequently accessed data could significantly improve response times and reduce database load.

2. **API Segmentation**: Further dividing the APIs into microservices could enhance scalability and maintainability.

3. **Data Visualization**: Integrating Chart.js or similar libraries to provide visual representations of the statistical data would greatly enhance the user experience.

4. **Frontend Development**: Creating a user-friendly web interface for interacting with the API would make the service more accessible to non-technical users.

5. **Expanded Machine Learning Features**: While the current implementation includes similarity search using TF-IDF and cosine similarity, there's potential to expand on this with more advanced ML techniques for game recommendation and trend analysis.

These enhancements were not implemented in the current version due to time constraints, as the project was completed over a 3-day period while balancing my summer internship. However, the existing codebase has been designed with best practices in mind, ensuring it's well-documented, follows PEP 8 guidelines (achieving a 9.46/10 score in pylint), and is structured for easy future expansion.

## Conclusion

This project demonstrates a comprehensive approach to building a game analytics web service, balancing functionality, code quality, and time constraints. While there's always room for expansion, the current implementation provides a solid foundation for game data analysis and showcases key skills in backend development, API design, and data processing.

I forward to any feedback and the opportunity to discuss this project and potential future collaborations with Segwise.ai
