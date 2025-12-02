import boto3

def create_table(table_name):
    dynamodb = boto3.client("dynamodb")

    try:
        existing = dynamodb.list_tables()["TableNames"]
        if table_name in existing:
            print(f"Table '{table_name}' already exists.")
            return

        print(f"Creating table '{table_name}'...")

        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    "AttributeName": "countrycode",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "countrycode",
                    "AttributeType": "S"
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        print("Waiting for table to become ACTIVE...")
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=table_name)

        print(f"Table '{table_name}' created successfully.")

    except Exception as e:
        print("Error creating table:", e)


if __name__ == "__main__":
    create_table("CountriesTable")
