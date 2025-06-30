import requests
import json

def get_mistral_explanation(transaction):
    prompt = f"""
    Explain why the following transaction might be considered anomalous:

    Transaction Details:
    ID: {transaction[0]}
    Timestamp: {transaction[1]}
    Amount: ₹{transaction[2]}
    Location: {transaction[3]}
    Type: {transaction[4]}
    Channel: {transaction[5]}

    Provide a short and insightful explanation.
    """

    response = requests.post(
        'http://localhost:11434/api/generate',
        json={
            "model": "mistral",
            "prompt": prompt,
            "stream": False
        }
    )

    try:
        content = response.json()["response"]
        return content.strip()
    except Exception as e:
        return f"❌ Error: {e}"

# Example usage (grabbed from your earlier anomaly)
anomalous_txn = (101, "2024-01-01 10:27:45", 95000.0, 'New York', 'transfer', 'mobile')

print("🤖 Mistral says:\n")
print(get_mistral_explanation(anomalous_txn))
