import boto3
import json
import math

TABLE_NAME = "CountriesTable"
ITEMS_FILE = "items.json"

def batch_write(table, items):
    with table.batch_writer(overwrite_by_pkeys=["countrycode"]) as batch:
        for item in items:
            batch.put_item(Item=item)

def insert_items():
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    print(f"Reading items from {ITEMS_FILE}...")
    with open(ITEMS_FILE, "r") as f:
        items = json.load(f)

    print(f"Inserting {len(items)} items...")

    batch_size = 25
    total_batches = math.ceil(len(items) / batch_size)

    for i in range(total_batches):
        batch_items = items[i * batch_size : (i + 1) * batch_size]
        print(f"--> Batch {i+1}/{total_batches}: inserting {len(batch_items)} items")
        batch_write(table, batch_items)

    print("All items inserted successfully!")

if __name__ == "__main__":
    insert_items()
