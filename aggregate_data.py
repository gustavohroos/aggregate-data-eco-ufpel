import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import datetime
from tqdm import tqdm

load_dotenv()

DB_IP = os.getenv("ECO_UFPEL_DATABASE_IP")
DB_PORT = os.getenv("ECO_UFPEL_DATABASE_PORT")
DB_USER = os.getenv("ECO_UFPEL_DATABASE_USER")
DB_PASS = os.getenv("ECO_UFPEL_DATABASE_PASSWORD")
DB_NAME = os.getenv("ECO_UFPEL_DATABASE_NAME")

migration = """
CREATE TABLE IF NOT EXISTS sensor_data.classroom_data_aggregation (
    classroom_id varchar(3) NOT NULL,
    avg_consumption INT,
    min_consumption INT,
    max_consumption INT,
    std_consumption INT,
    aggregation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT classroom_data_aggregation_pkey PRIMARY KEY (classroom_id, aggregation_date),
    CONSTRAINT aggregation_classroom_id_fkey FOREIGN KEY (classroom_id) REFERENCES ufpel_data.classrooms(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);
"""

def load(start_date, end_date):
    conn = psycopg2.connect(host=DB_IP, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM sensor_data.classroom_energy_consumption\
                WHERE date_time >= '{start_date}' AND date_time < '{end_date}';")
    rows = cur.fetchall()
    conn.close()
    return rows

def aggregate(rows):
    df = pd.DataFrame(rows, columns=["classroom_id", "consumption", "date_time"])
    
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['aggregation_date'] = df['date_time'].dt.floor('H')
    
    grouped = df.groupby(['classroom_id', 'aggregation_date'])
    
    agg_df = grouped['consumption'].agg(
        avg_consumption='mean',
        min_consumption='min',
        max_consumption='max',
        std_consumption='std'
    ).reset_index()

    if agg_df['std_consumption'].isnull().values.any():
        agg_df['std_consumption'] = 0
    
    return agg_df

def save(df):
    conn = psycopg2.connect(host=DB_IP, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)
    cur = conn.cursor()
    
    for _, row in df.iterrows():
        cur.execute(f"INSERT INTO sensor_data.classroom_data_aggregation\
                    (classroom_id, avg_consumption, min_consumption, max_consumption, std_consumption, aggregation_date)\
                    VALUES ('{row['classroom_id']}', {row['avg_consumption']}, {row['min_consumption']}, {row['max_consumption']}, {row['std_consumption']}, '{row['aggregation_date']}')\
                    ON CONFLICT (classroom_id, aggregation_date) DO NOTHING;")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    start_date = datetime.datetime(2024, 1, 1)
    end_date = datetime.datetime(2024, 4, 30)
    total_days = (end_date - start_date).days
    current_date = start_date

    print("Executing migration...")
    conn = psycopg2.connect(host=DB_IP, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)
    cur = conn.cursor()
    cur.execute(migration)
    conn.commit()
    conn.close()

    print("Aggregating data...")
    try:
        for _ in tqdm(range(total_days), desc="Processing days"):
            next_date = current_date + datetime.timedelta(days=1)
            
            rows = load(current_date, next_date)
            if rows:
                df = aggregate(rows)
                save(df)
            
            current_date = next_date
    except Exception as e:
        print(f"An error occurred: {e}")
