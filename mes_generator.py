import yaml
import os
import random
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
import psycopg2
from psycopg2 import extras

# --- 1. DATABASE CONFIGURATION ---
DB_CONFIG = {
    "dbname": "MES",
    "user": "postgres",
    "password": "3555",
    "host": "localhost",
    "port": "5432"
}

# --- 2. THE GENERATOR ---
def generate_raw_files():
    """Generates the 3 file formats automatically"""
    data_pool = []
    for i in range(15):
        data_pool.append({
            "id": f"REC-{random.randint(1000, 9999)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "machine_name": random.choice(["Press-01", "Drill-04", "Lathe-02"]),
            "output_quantity": random.randint(1, 100),
            "operator_id": f"OP-{random.randint(10, 99)}",
            "status": random.choice(["COMPLETED", "PENDING"])
        })
    
    # Write YAML
    with open("mes_data.yaml", "w") as f:
        yaml.dump(data_pool[:5], f)
    # Write TXT
    with open("mes_data.txt", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data_pool[0].keys())
        writer.writeheader(); writer.writerows(data_pool[5:10])
    # Write XML
    root = ET.Element("root")
    for d in data_pool[10:]:
        item = ET.SubElement(root, "record")
        for k, v in d.items(): ET.SubElement(item, k).text = str(v)
    ET.ElementTree(root).write("mes_data.xml")
    print("--- STEP 1: Files Generated Successfully ---")

# --- 3. THE READER & PROCESSOR ---
def read_and_process():
    raw_list = []
    
    # Reading logic for all formats
    if os.path.exists("mes_data.yaml"):
        with open("mes_data.yaml", 'r') as f: raw_list.extend(yaml.safe_load(f) or [])
    if os.path.exists("mes_data.txt"):
        with open("mes_data.txt", 'r') as f: raw_list.extend(list(csv.DictReader(f)))
    if os.path.exists("mes_data.xml"):
        root = ET.parse("mes_data.xml").getroot()
        for rec in root.findall('record'): raw_list.append({c.tag: c.text for c in rec})

    print(f"--- STEP 2: Python read {len(raw_list)} raw records from files ---")

    # Processing logic
    processed = []
    for rec in raw_list:
        # 1. Filter Status
        if str(rec.get("status")).strip().upper() == "COMPLETED":
            ts = rec.get("timestamp")
            dt_obj = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            
            # 2. Extract Date and Time separately
            clean_rec = (
                rec['id'],
                dt_obj.date(),      # Date object
                dt_obj.time(),      # Time object
                rec['machine_name'],
                int(rec['output_quantity']),
                rec['operator_id'],
                rec['status']
            )
            processed.append(clean_rec)

    # --- INTERNAL DATA PREVIEW ---
    print("\n--- STEP 3: Data Preview Inside Python (Filtered & Split) ---")
    print(f"{'ID':<12} | {'DATE':<12} | {'TIME':<10} | {'STATUS'}")
    print("-" * 50)
    for row in processed[:5]: # Show first 5 rows
        print(f"{row[0]:<12} | {str(row[1]):<12} | {str(row[2]):<10} | {row[6]}")
    
    return processed

# --- 4. THE LOADER ---
def load_to_postgres(data):
    if not data: return
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mes_production (
                id VARCHAR(50) PRIMARY KEY,
                prod_date DATE,
                prod_time TIME,
                machine_name VARCHAR(50),
                output_quantity INTEGER,
                operator_id VARCHAR(20),
                status VARCHAR(20)
            );
        """)
        query = "INSERT INTO mes_production VALUES %s ON CONFLICT (id) DO NOTHING"
        extras.execute_values(cur, query, data)
        conn.commit()
        print(f"\n--- STEP 4: Successfully loaded {len(data)} records to Postgres ---")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    generate_raw_files()
    processed_data = read_and_process()
    load_to_postgres(processed_data)
