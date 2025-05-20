# Python APIs Data Integration Exercise

## Overview

This project demonstrates how to integrate data from the Open Library API into a Neo4j graph database. The goal is to fetch book data based on a user-specified subject, clean and transform the data, and load it into a Neo4j database for further analysis and visualization. The project also includes functionality to run analytics queries on the data stored in Neo4j.

## Features

1. **Fetch Data from Open Library API**:
   - Dynamically fetch book data by subject using the Open Library API.
   - Handle API errors (e.g., retry mechanism for `503 Service Unavailable`).

2. **Data Transformation**:
   - Extract relevant book information (e.g., title, authors, publication year, subject).
   - Clean and enhance the data (e.g., normalize titles, handle missing values, generate unique IDs).

3. **Neo4j Integration**:
   - Load the cleaned data into a Neo4j graph database.
   - Create nodes for books, authors, subjects, and publication years.
   - Establish relationships between these entities (e.g., `WRITTEN_BY`, `HAS_SUBJECT`, `PUBLISHED_IN`).

4. **Analytics Queries**:
   - Run queries to analyze the data in Neo4j, such as:
     - Top authors by the number of books.
     - Distribution of books by decade.
     - Most popular subjects.

5. **Error Handling**:
   - Gracefully handle API errors and database connection issues.
   - Log errors and provide meaningful feedback to the user.

## Project Structure

```
.
├── config.py                # Configuration file for environment variables
├── neo4j_connection.py      # Handles Neo4j database operations
├── open_library_API.py      # Fetches and processes data from the Open Library API
├── test_neo4j_connection.py # Script to test the Neo4j connection
├── .env                     # Environment variables (e.g., Neo4j credentials)
├── README.md                # Project documentation
```

## Prerequisites

- Python 3.8 or higher
- Neo4j database installed and running locally or remotely
- `.env` file with the following variables:
  ```
  NEO4J_URI=bolt://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=<your_password>
  ```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd Python-APIs-data-integration-exercise
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the `.env` file with your Neo4j credentials.

4. Start your Neo4j database.

## Usage

1. Run the main script to fetch data and load it into Neo4j:
   ```bash
   python open_library_API.py
   ```

2. Follow the prompts to enter a subject (e.g., "python", "ships").

3. View the data in Neo4j or run analytics queries.

4. Test the Neo4j connection (optional):
   ```bash
   python test_neo4j_connection.py
   ```

## Example Workflow

1. Enter a subject (e.g., "ships").
2. The script fetches book data from the Open Library API.
3. The data is cleaned and transformed into a structured format.
4. The cleaned data is loaded into Neo4j as nodes and relationships.
5. Analytics queries are run to extract insights from the data.

## Key Files

- **`open_library_API.py`**: Handles API requests, data extraction, cleaning, and Neo4j integration.
- **`neo4j_connection.py`**: Manages Neo4j database operations (e.g., creating nodes, relationships, and constraints).
- **`test_neo4j_connection.py`**: Tests the connection to the Neo4j database.
- **`config.py`**: Loads environment variables for Neo4j credentials.

## Analytics Queries

The following analytics queries are run on the Neo4j database:

1. **Node Counts**:
   - Count the number of nodes by type (e.g., books, authors, subjects).

2. **Top Authors**:
   - Find the top 5 authors with the most books.

3. **Books by Decade**:
   - Analyze the distribution of books published by decade.

4. **Top Subjects**:
   - Identify the top 5 subjects with the most books.

## Troubleshooting

- **API Errors**: If the API returns a `503` error, the script retries the request with exponential backoff.
- **Neo4j Connection Issues**: Ensure the Neo4j database is running and the credentials in the `.env` file are correct.

## License

This project is for educational purposes and is not licensed for commercial use.

## Acknowledgments

- [Open Library API](https://openlibrary.org/developers/api) for providing book data.
- [Neo4j](https://neo4j.com/) for the graph database platform.
