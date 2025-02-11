import time

import boto3
from opensearchpy import Urllib3AWSV4SignerAuth, Urllib3HttpConnection, OpenSearch

# ==========================
# 🔹 Source OpenSearch Configuration
# ==========================
source_aws_access_key = ""
source_aws_secret_key = ""
source_region = "us-east-2"
source_host = ""
source_index_name = "plaud_index"  # 🔹 Change to your actual index

# ==========================
# 🔹 Target OpenSearch Configuration
# ==========================
target_aws_access_key = ""
target_aws_secret_key = ""
target_region = "us-east-2"
target_host = ""
target_index_name = "plaud_index_copy"  # 🔹 Change to your actual index

page_size = 5000  # 🔹 Max OpenSearch batch size
search_after = None  # 🔹 Pagination cursor
total_docs_to_fetch = 1000000  # 🔹 Total documents to fetch

def create_opensearch_client(host, aws_access_key, aws_secret_key, region):
    session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    credentials = session.get_credentials()
    auth = Urllib3AWSV4SignerAuth(credentials, region, "aoss")
    return OpenSearch(
        hosts=[host],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=Urllib3HttpConnection,
        pool_maxsize=20
    )

# Initialize OpenSearch clients
source_client = create_opensearch_client(source_host, source_aws_access_key, source_aws_secret_key, source_region)
target_client = create_opensearch_client(target_host, target_aws_access_key, target_aws_secret_key, target_region)

def fetch_and_insert_documents():
    global search_after
    total_docs = 0
    completed = False

    while not completed:
        query = {
            "size": page_size,
            "query": {"match_all": {}},
            "sort": [{"_id": "asc"}]
        }
        if search_after:
            query["search_after"] = search_after

        response = source_client.search(index=source_index_name, body=query)
        hits = response["hits"]["hits"]
        if not hits:
            break

        bulk_data = []
        for doc in hits:
            bulk_data.append({"index": {"_index": target_index_name, "_id": doc["_id"]}})
            bulk_data.append(doc["_source"])
            total_docs += 1
            if total_docs >= total_docs_to_fetch:
                completed = True
                break

        if bulk_data:
            target_client.bulk(body=bulk_data)
            print(f"✅ Inserted {len(hits)} documents into target OpenSearch")

        if completed:
            break
        search_after = hits[-1]["sort"]

    print(f"✅ Finished! Total documents processed: {total_docs}")

# ==========================
# 🔹 Run Data Migration
# ==========================
print("🚀 Starting OpenSearch to OpenSearch migration...")
start_time = time.time()
fetch_and_insert_documents()
elapsed_time = time.time() - start_time

# ✅ Print final stats
print(target_client.indices.get(index=target_index_name))
print(f"⏱️ Total time taken: {elapsed_time:.2f} seconds")
print(f"🚀 Speed: {total_docs_to_fetch / elapsed_time:.2f} docs/sec")
print("✅ All source OpenSearch data successfully migrated to target OpenSearch!")