from neo4j import GraphDatabase
import csv
from dotenv import load_dotenv
import os
import pandas as pd
# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# Neo4j driver setup
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def clear_database(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def ingest_transaction(tx, row):
    tx.run(
        """
        MERGE (u:User {id: $user_id})
        MERGE (l:Location {name: $location})
        MERGE (c:Channel {name: $channel})
        MERGE (t:Transaction {
            transaction_id: $transaction_id,
            timestamp: datetime($timestamp),
            amount: $amount,
            type: $transaction_type
        })
        MERGE (u)-[:MADE]->(t)
        MERGE (t)-[:HAPPENED_IN]->(l)
        MERGE (t)-[:VIA]->(c)
        """,
        transaction_id=row['transaction_id'],
        user_id=row['user_id'],
        timestamp=pd.to_datetime(row['timestamp'], format='%d-%m-%Y %H:%M').isoformat(),
        amount=float(row['amount']),
        location=row['location'],
        transaction_type=row['txn_type'],
        channel=row['channel']
    )

# Ingest CSV into Neo4j
def ingest_csv_to_neo4j(file_path):
    with driver.session() as session:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                session.write_transaction(ingest_transaction, row)
    print("✅ Transactions successfully ingested into Neo4j graph!")

if __name__ == "__main__":
    ingest_csv_to_neo4j("data/transactions_50k.csv")
