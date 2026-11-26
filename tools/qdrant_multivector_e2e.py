#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run against disposable local Qdrant + Milvus services and a freshly built VTS.

pip install qdrant-client pymilvus
python tools/qdrant_multivector_e2e.py --seatunnel-home /path/to/distribution

Requires StructArray/EmbeddingList-capable Milvus and PyMilvus. No existing
collection is changed: names are random and cleanup only removes these names.
"""

import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seatunnel-home", type=Path, required=True)
    parser.add_argument("--qdrant-host", default="localhost")
    parser.add_argument("--qdrant-http-port", type=int, default=6333)
    parser.add_argument("--qdrant-grpc-port", type=int, default=6334)
    parser.add_argument("--milvus-uri", default="http://localhost:19530")
    args = parser.parse_args()

    from qdrant_client import QdrantClient, models
    from pymilvus import MilvusClient
    from pymilvus.client.embedding_list import EmbeddingList

    qdrant_key = os.environ.get("QDRANT_API_KEY", "")
    milvus_token = os.environ.get("MILVUS_TOKEN", "")
    qdrant = QdrantClient(
        host=args.qdrant_host, port=args.qdrant_http_port,
        grpc_port=args.qdrant_grpc_port, api_key=qdrant_key or None,
    )
    milvus = MilvusClient(uri=args.milvus_uri, token=milvus_token)
    name = "vts_multivector_" + uuid.uuid4().hex
    documents = {
        1: [[1.0, 0.0]],
        2: [[1.0, 0.0], [0.0, 1.0]],
        3: [[-1.0, 0.0], [-0.6, -0.8], [0.0, -1.0]],
    }
    created = False
    try:
        qdrant.create_collection(
            collection_name=name,
            vectors_config={
                "colbert": models.VectorParams(
                    size=2, distance=models.Distance.COSINE,
                    multivector_config=models.MultiVectorConfig(
                        comparator=models.MultiVectorComparator.MAX_SIM,
                    ),
                ),
                "dense": models.VectorParams(size=2, distance=models.Distance.COSINE),
            },
        )
        created = True
        qdrant.upsert(name, points=[
            models.PointStruct(id=key, vector={"colbert": value, "dense": value[0]},
                               payload={"title": f"document {key}"})
            for key, value in documents.items()
        ], wait=True)
        # JSON is a HOCON subset, avoiding quoting bugs in endpoints and credentials.
        config = {
            "env": {"parallelism": 2, "job.mode": "BATCH"},
            "source": {"Qdrant": {
                "host": args.qdrant_host, "port": args.qdrant_grpc_port,
                "use_tls": False, "api_key": qdrant_key,
                "collection_name": name, "batch_size": 2,
            }},
            "sink": {"Milvus": {
                "url": args.milvus_uri, "token": milvus_token, "database": "default",
                "batch_size": 2, "create_index": True, "enable_dynamic_field": True,
            }},
        }
        with tempfile.TemporaryDirectory(prefix="vts-multivector-") as work:
            config_file = Path(work) / "migration.conf"
            config_file.write_text(json.dumps(config), encoding="utf-8")
            config_file.chmod(0o600)
            subprocess.run([
                str(args.seatunnel_home.resolve() / "bin" / "seatunnel.sh"),
                "--config", str(config_file), "-m", "local",
            ], check=True, timeout=600)

        milvus.flush(collection_name=name)
        indexes = milvus.prepare_index_params()
        indexes.add_index(field_name="colbert[vector]", index_type="AUTOINDEX", metric_type="MAX_SIM_COSINE")
        indexes.add_index(field_name="dense", index_type="AUTOINDEX", metric_type="COSINE")
        milvus.create_index(collection_name=name, index_params=indexes)
        milvus.load_collection(collection_name=name)
        rows = milvus.query(collection_name=name, filter="id >= 0",
                            output_fields=["id", "colbert", "title"], consistency_level="Strong")
        assert len(rows) == len(documents), rows
        for row in rows:
            expected = documents[row["id"]]
            actual = [element["vector"] for element in row["colbert"]]
            assert len(actual) == len(expected), row
            for actual_vector, expected_vector in zip(actual, expected):
                assert len(actual_vector) == len(expected_vector)
                assert all(math.isclose(a, b, abs_tol=1e-6)
                           for a, b in zip(actual_vector, expected_vector)), row
            assert row["title"] == f"document {row['id']}"

        matrix = [[1.0, 0.0], [0.0, 1.0]]
        q_hits = qdrant.query_points(name, using="colbert", query=matrix, limit=3).points
        embedding_list = EmbeddingList()
        embedding_list.add_batch(matrix)
        m_hits = milvus.search(collection_name=name, anns_field="colbert[vector]",
                               data=[embedding_list], limit=3, consistency_level="Strong")[0]
        assert [hit.id for hit in q_hits] == [hit["id"] for hit in m_hits]
        for q_hit, m_hit in zip(q_hits, m_hits):
            assert math.isclose(q_hit.score, m_hit["distance"], abs_tol=1e-5)
        print("PASS: variable-length vectors, payload, mixed indexes and MaxSim results")
    finally:
        if milvus.has_collection(name):
            milvus.drop_collection(name)
        if created:
            qdrant.delete_collection(name)
        qdrant.close()
        milvus.close()


if __name__ == "__main__":
    main()
