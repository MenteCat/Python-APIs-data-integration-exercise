import logging
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from neo4j import GraphDatabase
from open_library_API import fetch_books_by_subject, extract_book_data, clean_data
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Neo4jConnector:
    """Class to handle Neo4j database operations"""

    def __init__(self, uri, user, password):
        """Initialize Neo4j connection"""
        self.uri = NEO4J_URI
        self.user = NEO4J_USER
        self.password = NEO4J_PASSWORD
        self.driver = None

    def connect(self):
        """Establish connection to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            logger.info("Successfully connected to Neo4j database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            return False

    def close(self):
        """Close the Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    def verify_connection(self):
        """Verify that the connection is working"""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 'Connection successful' AS message")
                for record in result:
                    logger.info(record["message"])
                return True
        except Exception as e:
            logger.error(f"Connection verification failed: {e}")
            return False

    def clear_database(self):
        """Clear all nodes and relationships in the database"""
        try:
            with self.driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
                logger.info("Database cleared")
        except Exception as e:
            logger.error(f"Failed to clear database: {e}")

    def create_constraints(self):
        """Create constraints to ensure uniqueness"""
        try:
            with self.driver.session() as session:
                session.run("""
                    CREATE CONSTRAINT book_id_constraint IF NOT EXISTS
                    FOR (b:Book) REQUIRE b.id IS UNIQUE
                """)
                session.run("""
                    CREATE CONSTRAINT author_name_constraint IF NOT EXISTS
                    FOR (a:Author) REQUIRE a.name IS UNIQUE
                """)
                session.run("""
                    CREATE CONSTRAINT subject_name_constraint IF NOT EXISTS
                    FOR (s:Subject) REQUIRE s.name IS UNIQUE
                """)
                session.run("""
                    CREATE CONSTRAINT year_value_constraint IF NOT EXISTS
                    FOR (y:Year) REQUIRE y.value IS UNIQUE
                """)
                logger.info("Constraints created successfully")
        except Exception as e:
            logger.error(f"Failed to create constraints: {e}")

    def load_books(self, books_df):
        """Load books data into Neo4j"""
        try:
            with self.driver.session() as session:
                def create_book_nodes(tx, batch):
                    for _, book in batch.iterrows():
                        tx.run("""
                            MERGE (b:Book {id: $id})
                            SET b.title = $title,
                                b.edition_count = $edition_count,
                                b.confidence_score = $confidence_score,
                                b.date_ingested = $date_ingested
                        """, id=book['id'], title=book['title'],
                               edition_count=int(book['edition_count']),
                               confidence_score=float(book['confidence_score']),
                               date_ingested=book['date_ingested'])
                        tx.run("""
                            MERGE (s:Subject {name: $subject})
                            MERGE (b:Book {id: $book_id})
                            MERGE (b)-[:HAS_SUBJECT]->(s)
                        """, subject=book['subject'], book_id=book['id'])
                        if pd.notnull(book['publish_year']):
                            year = int(book['publish_year'])
                            tx.run("""
                                MERGE (y:Year {value: $year})
                                MERGE (b:Book {id: $book_id})
                                MERGE (b)-[:PUBLISHED_IN]->(y)
                            """, year=year, book_id=book['id'])
                        if book['authors'] != 'Unknown':
                            for author in book['authors'].split(', '):
                                tx.run("""
                                    MERGE (a:Author {name: $author_name})
                                    MERGE (b:Book {id: $book_id})
                                    MERGE (b)-[:WRITTEN_BY]->(a)
                                """, author_name=author, book_id=book['id'])

                batch_size = 50
                for i in range(0, len(books_df), batch_size):
                    batch = books_df.iloc[i:i + batch_size]
                    session.execute_write(create_book_nodes, batch)
                    logger.info(f"Loaded batch {i // batch_size + 1}/{(len(books_df) - 1) // batch_size + 1}")
                logger.info(f"Successfully loaded {len(books_df)} books into Neo4j")
        except Exception as e:
            logger.error(f"Failed to load books into Neo4j: {e}")

    def run_analytics_queries(self):
        """Run analytics queries and return results"""
        analytics_results = {}
        try:
            with self.driver.session() as session:
                result = session.run("""
                    MATCH (n)
                    RETURN labels(n)[0] AS label, count(n) AS count
                    ORDER BY count DESC
                """)
                analytics_results['node_counts'] = {record["label"]: record["count"] for record in result}
                result = session.run("""
                    MATCH (a:Author)<-[:WRITTEN_BY]-(b:Book)
                    RETURN a.name AS author, count(b) AS book_count
                    ORDER BY book_count DESC
                    LIMIT 5
                """)
                analytics_results['top_authors'] = [(record["author"], record["book_count"]) for record in result]
                result = session.run("""
                    MATCH (b:Book)-[:PUBLISHED_IN]->(y:Year)
                    WITH y.value/10 AS decade, count(b) AS book_count
                    RETURN decade*10 AS decade_start, book_count
                    ORDER BY decade_start
                """)
                analytics_results['books_by_decade'] = [(record["decade_start"], record["book_count"]) for record in
                                                        result]
                result = session.run("""
                    MATCH (s:Subject)<-[:HAS_SUBJECT]-(b:Book)
                    RETURN s.name AS subject, count(b) AS book_count
                    ORDER BY book_count DESC
                    LIMIT 5
                """)
                analytics_results['top_subjects'] = [(record["subject"], record["book_count"]) for record in result]
                logger.info("Analytics queries completed successfully")
                return analytics_results
        except Exception as e:
            logger.error(f"Failed to run analytics queries: {e}")
            return {}




