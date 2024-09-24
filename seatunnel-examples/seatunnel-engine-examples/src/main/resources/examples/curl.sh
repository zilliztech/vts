curl --location --request POST 'http://127.0.0.1:5801/hazelcast/rest/maps/submit-job' \
--header 'Content-Type: application/json' \
--data-raw '
{
  "env": {
    "execution.parallelism": 2,
    "job.mode": "BATCH"
  },
  "source": [
    {
      "plugin_name": "Milvus",
      "url": "https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19530",
      "token": "***",
      "database": "default",
      "collection": "book",
      "result_table_name": "book"
    }
  ],

  "transform": [
      {
              "plugin_name" : "TablePathMapper",
              "source_table_name" : "book",
              "result_table_name" : "book_transform1",
              "table_mapper" : {
                  "book_all" : "book_all2"
              }
          },
          {
              "plugin_name" : "FieldMapper",
              "source_table_name" : "book_transform1",
              "result_table_name" : "book_transform2",
              "field_mapper" : {
                  "pk_field" : "pk_field",
                  "vector_field" : "vector_field",
                  "array_field" : "array_field",
                  "bool_field" : "bool_field",
                  "float_field" : "float_field",
                  "int16_field" : "int16_field",
                  "int32_field" : "int32_field",
                  "int64_field" : "int64_field",
                  "int8_field" : "int8_field",
                  "json_field" : "json_field",
                  "varchar_field" : "varchar_field"
              }
          }
  ],
  "sink": [
    {
      "plugin_name": "Milvus",
      "source_table_name": "book_transform2",
      "url": "https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19542",
      "token": "***",
      "database": "default",
      "batch_size": 10
    }
  ]
}
'

#curl --location --request GET 'http://127.0.0.1:5801/hazelcast/rest/maps/running-job/878214447376629761'
