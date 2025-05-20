import requests  # For making HTTP requests to APIs
import pandas as pd  # For handling and manipulating tabular data
from datetime import datetime  # For working with date and time
import re  # For regular expression operations
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


def fetch_books_by_subject(subject, limit=20):
    """Fetch books from Open Library API by subject."""
    print(f"Fetching books related to '{subject}'...")

    url = f"http://openlibrary.org/subjects/{subject}.json?limit={limit}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: API returned status code {response.status_code}")
        return None


def extract_book_data(api_response):
    """Extract relevant book information from API response."""
    if not api_response or 'works' not in api_response:
        return []

    books = []
    for work in api_response['works']:
        # Extract authors (handling potential missing data)
        authors = []
        if 'authors' in work:
            authors = [author.get('name', 'Unknown') for author in work['authors']]

        # Extract year (handling potential missing data)
        year = None
        if 'first_publish_year' in work:
            year = work['first_publish_year']

        book = {
            'title': work.get('title', 'Unknown Title'),
            'authors': ', '.join(authors) if authors else 'Unknown',
            'publish_year': year,
            'subject': api_response.get('name', 'Unknown Subject'),
            'edition_count': work.get('edition_count', 0),
            'key': work.get('key', '')
        }
        books.append(book)

    return books


def clean_data(df):
    """Clean and validate the dataframe."""
    # Create a copy to avoid SettingWithCopyWarning
    cleaned_df = df.copy()

    # Clean titles (remove extra whitespace, normalize capitalization)
    cleaned_df['title'] = cleaned_df['title'].str.strip().str.title()

    # Replace missing years with NaN
    cleaned_df['publish_year'] = pd.to_numeric(cleaned_df['publish_year'], errors='coerce')

    # Clean author names
    cleaned_df['authors'] = cleaned_df['authors'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # Create a unique identifier for each book
    cleaned_df['id'] = cleaned_df.apply(
        lambda x: f"book_{re.sub(r'[^a-zA-Z0-9]', '', x['title'].lower())[:20]}_{x['publish_year']}",
        axis=1
    )

    # Add metadata for knowledge graph integration
    cleaned_df['entity_type'] = 'book'
    cleaned_df['date_ingested'] = datetime.now().strftime('%Y-%m-%d')

    # Add a confidence score column (could be based on data completeness)
    cleaned_df['confidence_score'] = cleaned_df.apply(
        lambda x: 1.0 if pd.notnull(x['publish_year']) and x['authors'] != 'Unknown' else 0.7,
        axis=1
    )

    return cleaned_df


def generate_knowledge_graph_triples(df):
    """Generate example knowledge graph triples from the cleaned data."""
    triples = []

    for _, book in df.iterrows():
        # Subject-Predicate-Object triples
        triples.append(f"{book['id']} - HAS_TITLE - {book['title']}")

        if book['authors'] != 'Unknown':
            authors = book['authors'].split(', ')
            for author in authors:
                triples.append(f"{book['id']} - WRITTEN_BY - {author}")

        if pd.notnull(book['publish_year']):
            triples.append(f"{book['id']} - PUBLISHED_IN - {int(book['publish_year'])}")

        triples.append(f"{book['id']} - HAS_SUBJECT - {book['subject']}")

    return triples


def main():
    from neo4j_connection import Neo4jConnector
    # 1. Extract data from API and returns a JSON response
    subject = input("Enter the subject to fetch books for: ")  # Prompt user for subject
    api_data = fetch_books_by_subject(subject)

    if not api_data:
        print("Failed to fetch data. Exiting.")
        return

    # 2. Transform API data into structured format
    books = extract_book_data(api_data)
    if not books:
        print("No book data found. Exiting.")
        return

    # 3. Create DataFrame
    df = pd.DataFrame(books)
    print(f"\nRaw data sample (first 3 records):")
    print(df.head(3))

    # 4. Clean and enhance the data
    cleaned_df = clean_data(df)
    print(f"\nCleaned data sample (first 3 records):")
    print(cleaned_df.head(3))

    # 5. Save to CSV
    output_file = f"books_{subject}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    cleaned_df.to_csv(output_file, index=False)
    print(f"\nData saved to {output_file}")

    # 6. Generate example knowledge graph triples
    print("\nExample knowledge graph triples:")
    triples = generate_knowledge_graph_triples(cleaned_df)
    for i, triple in enumerate(triples[:10]):  # Show first 10 triples
        print(f"  {i + 1}. {triple}")

    if len(triples) > 10:
        print(f"  ... and {len(triples) - 10} more triples")

    print("\nThese triples are ready to be loaded into the knowledge graph database.")

    # Initialize Neo4j connection
    connector = Neo4jConnector(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

    if connector.connect():
        # Load the cleaned data into Neo4j
        connector.clear_database()  # Optional: Clear the database before loading
        connector.create_constraints()
        connector.load_books(cleaned_df)

        connector.close()


if __name__ == "__main__":
    main()

