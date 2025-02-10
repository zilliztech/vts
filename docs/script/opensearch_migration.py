import time
from datetime import datetime

import boto3
from opensearchpy import Urllib3AWSV4SignerAuth, Urllib3HttpConnection, OpenSearch
from pymilvus import DataType, CollectionSchema
from pymilvus.bulk_writer import LocalBulkWriter, BulkFileType

# ==========================
# 🔹 OpenSearch Configuration
# ==========================
aws_access_key = ""
aws_secret_key = ""
region = "us-east-2"
host = ""
service = "aoss"
index_name = ""  # 🔹 Change to your actual index
page_size = 5000  # 🔹 Max OpenSearch batch size
search_after = None  # 🔹 Pagination cursor
total_docs_to_fetch = 1000000  # 🔹 Total documents to fetch

# Define the collection name in Milvus
COLLECTION_NAME = "opensearch_vectors"
# Initialize AWS Auth
session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
credentials = session.get_credentials()
auth = Urllib3AWSV4SignerAuth(credentials, region, service)

# Create OpenSearch Client
client = OpenSearch(
    hosts=[host],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=Urllib3HttpConnection,
    pool_maxsize=20
)

# ==========================
# 🔹 Milvus Configuration
# ==========================

# Connect to Milvus
# milvus_client = MilvusClient(uri=ZILLIZ_URI,
#                              token=TOKEN)

# ==========================
# 🔹 Define Milvus Schema
# ==========================
collectionSchema = CollectionSchema([], check_fields=False)
collectionSchema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=True)
collectionSchema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=1536)
collectionSchema.add_field("text", datatype=DataType.VARCHAR, max_length=65535)
collectionSchema.add_field("date", datatype=DataType.INT32)  # 🔹 Store `YYYYMMDD` format as `INT32`
collectionSchema.add_field("user_id", datatype=DataType.VARCHAR, max_length=50)
collectionSchema.add_field("updated_at", datatype=DataType.VARCHAR, max_length=50)
collectionSchema.add_field("ref_doc_id", datatype=DataType.VARCHAR, max_length=50)
collectionSchema.add_field("doc_type", datatype=DataType.VARCHAR, max_length=50)
collectionSchema.add_field("scenario", datatype=DataType.VARCHAR, max_length=50)
collectionSchema.add_field("language", datatype=DataType.VARCHAR, max_length=50)
collectionSchema.add_field("speakers", datatype=DataType.ARRAY, element_type=DataType.VARCHAR, max_length = 50, max_capacity=2000)
collectionSchema.add_field("start_times", datatype=DataType.ARRAY, element_type=DataType.INT32, max_capacity=2000)
collectionSchema.add_field("end_times", datatype=DataType.ARRAY, element_type=DataType.INT32, max_capacity=2000)
collectionSchema.verify()

# milvus_client.create_collection(collection_name=COLLECTION_NAME, dimension=1536, schema=collectionSchema)

writer = LocalBulkWriter(
    schema=collectionSchema,
    local_path="./parquet",
    chunk_size=512*1024*1024,
    file_type=BulkFileType.JSON
)

# ==========================
# 🔹 Data Processing Functions
# ==========================
def convert_date_to_int(date_str):
    """Convert 'YYYY-MM-DDTHH:MM:SS' format to `YYYYMMDD` as INT32"""
    try:
        return int(datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%Y%m%d"))
    except Exception:
        return None  # Handle missing or incorrect formats

def convert_to_int_list(str_list):
    """Convert a list of numeric strings to a list of integers"""
    return [int(x) for x in str_list if isinstance(x, str) and x.isdigit()]

# ==========================
# 🔹 Fetch and Insert Data
# ==========================
def write_data(data_to_insert):
    # milvus_client.insert(collection_name=COLLECTION_NAME, data=data_to_insert)
    # Append a row to the writer
    for data in data_to_insert:
        writer.append_row(data)
total_docs = 0
completed = False
def fetch_all_documents():
    global search_after, completed, total_docs

    while True:
        query = {
            "size": page_size,
            "_source": ["id", "vector_field", "text", "metadata"],  # 🔹 Adjust fields as needed
            "query": {"match_all": {}},
            "sort": [{"_id": "asc"}]
        }
        if search_after:
            query["search_after"] = search_after

        response = client.search(index=index_name, body=query)
        hits = response["hits"]["hits"]
        if not hits:
            break  # 🔹 No more data, stop fetching

        data_to_insert = []
        for doc in hits:
            metadata = doc["_source"].get("metadata", {})
            vector = doc["_source"].get("vector_field", [])
            text = doc["_source"].get("text", "")

            # Convert fields
            date = convert_date_to_int(metadata.get("date", ""))
            speakers = metadata.get("speakers", []) if isinstance(metadata.get("speakers"), list) else []
            start_times = convert_to_int_list(metadata.get("start_times", []))
            end_times = convert_to_int_list(metadata.get("end_times", []))

            if vector:
                data = {
                    "vector": vector, "text": text, "date": date,
                    "user_id": metadata.get("user_id", ""), "updated_at": metadata.get("updated_at", ""),
                    "ref_doc_id": metadata.get("ref_doc_id", ""), "doc_type": metadata.get("doc_type", ""),
                    "scenario": metadata.get("scenario", ""), "language": metadata.get("language", ""),
                    "speakers": speakers, "start_times": start_times, "end_times": end_times
                }
                data_to_insert.append(data)
            total_docs += 1
            if total_docs >= total_docs_to_fetch:
                completed = True
                break

        # Insert batch into Milvus
        if data_to_insert:
            write_data(data_to_insert)
            print(f"✅ Inserted {len(data_to_insert)} vectors into Milvus")
        if completed:
            break
        search_after = hits[-1]["sort"]
        # time.sleep(0.1)  # 🔹 Small delay to prevent API overload

    print(f"✅ Finished! Total documents processed: {total_docs}")
    writer.commit()

# ==========================
# 🔹 Run Data Migration
# ==========================
print("🚀 Starting OpenSearch to Milvus migration...")
start_time = time.time()
fetch_all_documents()
elapsed_time = time.time() - start_time  # Calculate total elapsed time

# ✅ Print final stats
print(f"⏱️ Total time taken: {elapsed_time:.2f} seconds")
print(f"🚀 Speed: {total_docs / elapsed_time:.2f} docs/sec")
print("✅ All OpenSearch data successfully migrated to Milvus!")