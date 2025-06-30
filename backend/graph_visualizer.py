# backend/graph_visualizer.py

from pyvis.network import Network
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test1234")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def fetch_graph_data(txn_id):
    query = """
    MATCH (u:User)-[:MADE]->(t:Transaction {transaction_id: $txn_id})
    OPTIONAL MATCH (t)-[:HAPPENED_IN]->(l:Location)
    OPTIONAL MATCH (t)-[:VIA]->(c:Channel)
    RETURN u.id AS user_id, t.transaction_id AS txn_id, t.amount AS amount,
           l.name AS location, c.name AS channel
    """
    with driver.session() as session:
        result = session.run(query, txn_id=txn_id)
        return result.single()

def create_pyvis_graph(txn_id):
    data = fetch_graph_data(txn_id)
    if not data:
        return None

    user = data["user_id"]
    txn = data["txn_id"]
    amount = data["amount"]
    location = data["location"]
    channel = data["channel"]

    net = Network(height="400px", width="100%", bgcolor="#222222", font_color="white")
    net.barnes_hut()

    # Add nodes
    net.add_node(user, label=f"User {user}", color="#00ff00")
    net.add_node(txn, label=f"Txn ₹{amount}", color="#ff0000")
    if location:
        net.add_node(location, label=f"📍 {location}", color="#007bff")
    if channel:
        net.add_node(channel, label=f"📱 {channel}", color="#ffc107")

    # Add edges
    net.add_edge(user, txn, label="MADE")
    if location:
        net.add_edge(txn, location, label="HAPPENED_IN")
    if channel:
        net.add_edge(txn, channel, label="VIA")

    net.set_options("""
    var options = {
      "physics": {
        "barnesHut": {
          "gravitationalConstant": -30000,
          "centralGravity": 0.3,
          "springLength": 95
        },
        "minVelocity": 0.75
      }
    }
    """)

    return net
