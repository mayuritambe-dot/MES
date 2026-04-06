import yaml
import os
import random
from datetime import datetime

def generate_mes_yaml(filename="mes_data.yaml"):
    # 1. Load existing data if the file exists
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                # Load the list of records
                data = yaml.safe_load(f) or []
            except yaml.YAMLError:
                data = []
    else:
        data = []

    # 2. Generate 10 new records
    for _ in range(10):
        new_record = {
            "id": f"REC-{datetime.now().strftime('%M%S')}-{random.randint(1000, 9999)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "machine_name": random.choice(["Press-01", "Drill-04", "Lathe-02"]),
            "output_quantity": random.randint(1, 100),
            "operator_id": f"OP-{random.randint(10, 99)}",
            "status": "COMPLETED"
        }
        data.append(new_record)

    # 3. Save back to YAML file
    with open(filename, 'w', encoding='utf-8') as f:
        # sort_keys=False keeps the order we defined in the dictionary
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    print(f"Success! 10 records added to {filename}.")
    print(f"Total records in file: {len(data)}")

if __name__ == "__main__":
    generate_mes_yaml()