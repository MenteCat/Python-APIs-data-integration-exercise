from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def test_connection():
    """Test the connection to the Neo4j database."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            result = session.run("RETURN 'Connection successful' AS message")
            for record in result:
                print(record["message"])
        driver.close()
        print("Connection test completed!")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()