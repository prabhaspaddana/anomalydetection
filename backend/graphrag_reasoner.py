# File: graphrag_reasoner.py

from neo4j import GraphDatabase
from dotenv import load_dotenv
import requests
import os

load_dotenv()

# Neo4j config
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test1234")

# Gemini + Ollama config
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OLLAMA_URL = "http://localhost:11434/api/generate"
LLM_MODEL = "mistral"  # Ollama model name

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def fetch_graph_context(tx, transaction_id):
    query = """
    MATCH (u:User)-[:MADE]->(t:Transaction {transaction_id: $txn_id})
    OPTIONAL MATCH (t)-[:HAPPENED_IN]->(l:Location)
    OPTIONAL MATCH (t)-[:VIA]->(c:Channel)
    OPTIONAL MATCH (u)-[:MADE]->(prev:Transaction)
    WHERE prev.timestamp < t.timestamp
    RETURN u.id AS user_id, t.amount AS amount, t.timestamp AS timestamp,
           l.name AS location, c.name AS channel, t.type AS txn_type,
           collect(prev.amount) AS prev_amounts, collect(prev.timestamp) AS prev_times
    """
    result = tx.run(query, txn_id=transaction_id)
    return result.single()


def generate_graph_prompt(data):
    return f"""
You are a financial anomaly analyst. A transaction has been flagged.

Transaction:
- User: {data['user_id']}
- Timestamp: {data['timestamp']}
- Amount: ₹{data['amount']}
- Location: {data['location']}
- Channel: {data['channel']}
- Type: {data['txn_type']}

Previous Transactions:
- Amounts: {data['prev_amounts']}
- Timestamps: {data['prev_times']}

Based on this user's history and this transaction's context, explain whether this transaction is anomalous or not. Be precise and insightful.
"""


def call_ollama_llm(prompt):
    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
            timeout=None
        )
        return response.json()["response"].strip()
    except Exception as e:
        return f"❌ Error from Ollama: {e}"


def call_gemini_llm(prompt, model_name="gemini-1.5-pro"):
    try:
        url = f"https://generativelanguage.googleapis.com/v1/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [
                {"parts": [{"text": prompt}]}
            ]
        }

        res = requests.post(url, headers=headers, json=payload)
        res.raise_for_status()
        return res.json()["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        return f"❌ Error from Gemini ({model_name}): {e}"


def explain_transaction(transaction_id, model_choice="mistral"):
    with driver.session() as session:
        result = session.read_transaction(fetch_graph_context, transaction_id)
        if not result:
            return f"❌ No transaction with ID {transaction_id} found."

        prompt = generate_graph_prompt(result)

        model_choice = model_choice.lower()
        if "gemini" in model_choice:
            # Try Pro first, then fallback to Flash if Pro fails
            pro_response = call_gemini_llm(prompt, model_name="gemini-1.5-pro")
            if "❌" in pro_response:
                print("[⚠️] Falling back to gemini-1.5-flash...")
                return call_gemini_llm(prompt, model_name="gemini-1.5-flash")
            return pro_response

        return call_ollama_llm(prompt)


def explain_transaction_ids(transaction_id, model_choice="mistral"):
    return explain_transaction(transaction_id, model_choice)


if __name__ == "__main__":
    txn_id = "txn003"
    print(f"\n🔍 Reasoning for Transaction ID: {txn_id}\n")
    print(explain_transaction(txn_id, model_choice="gemini"))
