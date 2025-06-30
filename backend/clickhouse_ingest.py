import pandas as pd
from clickhouse_connect import get_client

# Load CSV
df = pd.read_csv('data/transactions_50k.csv')

# Parse timestamp with coercion
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d-%m-%Y %H:%M', errors='coerce')
df = df.dropna(subset=['timestamp'])
df['timestamp'] = pd.to_datetime(df['timestamp'])

# ✅ Sanity check
print(df.dtypes)
print(type(df['timestamp'].iloc[0]))

# Connect to ClickHouse
client = get_client(host='localhost', port=8123)

# Drop + create table
client.command("DROP TABLE IF EXISTS transactions")
client.command('''
    CREATE TABLE transactions (
        transaction_id String,
        user_id UInt32,
        timestamp DateTime,
        amount Float32,
        location String,
        transaction_type String,
        channel String
    ) ENGINE = MergeTree()
    ORDER BY timestamp
''')

# ✅ Use itertuples to ensure types are correct
records = [(
    row.transaction_id,
    int(row.user_id),
    row.timestamp.to_pydatetime(),
    float(row.amount),
    row.location,
    row.txn_type,
    row.channel
) for row in df.itertuples(index=False)]

# Insert data
client.insert('transactions', records)

print("✅ Successfully re-ingested transactions into ClickHouse.")
