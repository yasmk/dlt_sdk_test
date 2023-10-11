%pip install databricks-sdk

import dlt
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines

w = WorkspaceClient(host="", token="")

pipelines = w.pipelines.list_pipelines(filter="name LIKE 'yas_test_pipelineid'")

pipeline = next(pipelines)

pipeline_id = pipeline.pipeline_id

@dlt.table()
def test_table(name="pipeline_id_test"):
  df = spark.createDataFrame(data=[{
    "name":"yas_test_schema",
    "pipeline_id": pipeline_id
  }])
  return df
