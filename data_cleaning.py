import pandas as pd
import psycopg2

# Database connection details
DB_NAME = "ecommercek_db"
DB_USER = "postgres"
DB_PASSWORD = "delivyliana@1221"  # Change this to your actual password
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

# Load cleaned CSV file
df = pd.read_csv("cleaned_K.csv")

# Convert event_time to a valid datetime format
df['event_time'] = pd.to_datetime(df['event_time'])

# Insert data into PostgreSQL
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO ecommerce_data (event_time, event_type, product_id, brand, price)
        VALUES (%s, %s, %s, %s, %s)
    """, (row['event_time'], row['event_type'], row['product_id'], row['brand'], row['price']))

# Commit & close connection
conn.commit()
cursor.close()
conn.close()

print("Data successfully loaded into PostgreSQL!")
