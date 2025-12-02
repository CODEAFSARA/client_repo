const fs = require('fs');
const path = require('path');
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, BatchWriteCommand } = require("@aws-sdk/lib-dynamodb");

if (!process.env.TABLE_NAME) {
  console.error("TABLE_NAME environment variable is required");
  process.exit(1);
}

const TABLE_NAME = process.env.TABLE_NAME;
const ITEMS_FILE = "items.json";

async function main() {
  const filePath = path.resolve(ITEMS_FILE);

  if (!fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  const items = JSON.parse(fs.readFileSync(filePath, 'utf8'));

  const client = new DynamoDBClient({});
  const doc = DynamoDBDocumentClient.from(client);

  const chunkSize = 25;

  for (let i = 0; i < items.length; i += chunkSize) {
    const chunk = items.slice(i, i + chunkSize);

    const requestItems = {
      [TABLE_NAME]: chunk.map(item => ({
        PutRequest: { Item: item }
      }))
    };

    console.log(`Writing batch of ${chunk.length} items...`);

    await doc.send(new BatchWriteCommand({
      RequestItems: requestItems
    }));
  }

  console.log("All country records inserted successfully!");
}

main().catch(err => {
  console.error("ERROR inserting items:", err);
  process.exit(1);
});
