import json
import data_analytics_agent.rest_api_helper as rest_api_helper
import data_analytics_agent.bigquery.bigquery_sql as bq_sql 

def get_bigquery_table_list() -> dict:
    """
    Gathers all the tables in BigQuery.
    This will gather all the tables along with the specific dataset in which they reside.
    This is useful when generating SQL for BigQuery.
    This tool should be called before calling run_bigquery_sql in order to get the data needed to construct a valid SQL statement.
    This tool also is useful to get the correct dataset and table names.  The user might use a shortened or mispelled name.

    Args:
        None

    Returns:
        dict:
        {
            "status": "success",
            "tool_name": "get_bigquery_table_list",
            "query": None,
            "messages": ["List of messages during processing"]
            "results": [ 
                            {
                                "project_id": "governed-data-1pqzajgatl",
                                "dataset_id": "governed_data_scan_quality_results",
                                "table_name": "data_quality_metrics",
                                "table_ddl": "CREATE TABLE `governed-data-1pqzajgatl.governed_data_scan_quality_results.data_quality_metrics`\n(\n  data_quality_scan STRUCT<resource_name STRING OPTIONS(description=\"The full resource name of the data scan.\"), project_id STRING OPTIONS(description=\"The project id of the data scan.\"), location STRING OPTIONS(description=\"The location of the data scan.\"), data_scan_id STRING OPTIONS(description=\"The data scan id.\"), display_name STRING OPTIONS(description=\"The display name of the data scan.\")> OPTIONS(description=\"Data quality scan information.\"),\n  data_source STRUCT<resource_name STRING OPTIONS(description=\"The full resource name of the data source.\"), dataplex_entity_project_id STRING OPTIONS(description=\"Data source - the project id of the source dataplex entity.\"), dataplex_entity_project_number INT64 OPTIONS(description=\"Data source - the project number of the source dataplex entity.\"), dataplex_lake_id STRING OPTIONS(description=\"Data source - the lake id of the source dataplex entity.\"), dataplex_zone_id STRING OPTIONS(description=\"Data source - the zone id of the source dataplex entity.\"), dataplex_entity_id STRING OPTIONS(description=\"Data source - the entity id of the source dataplex entity.\"), table_project_id STRING OPTIONS(description=\"Data source - the project id of the source BigQuery table.\"), table_project_number INT64 OPTIONS(description=\"Data source - the project number of the source BigQuery table.\"), dataset_id STRING OPTIONS(description=\"Data source - the dataset id of the source BigQuery table.\"), table_id STRING OPTIONS(description=\"Data source - the table id of the source BigQuery table.\")> OPTIONS(description=\"The data source of the data scan.\"),\n  data_quality_job_id STRING OPTIONS(description=\"Data quality scan job id.\"),\n  data_quality_job_configuration JSON OPTIONS(description=\"Data quality job configuration.\"),\n  job_labels JSON OPTIONS(description=\"The data scan job labels.\"),\n  job_start_time TIMESTAMP OPTIONS(description=\"The start time of the data scan job.\"),\n  job_end_time TIMESTAMP OPTIONS(description=\"The end time of the data scan job.\"),\n  job_quality_result STRUCT<passed BOOL OPTIONS(description=\"The result of whether all quality rules have passed.\"), score FLOAT64 OPTIONS(description=\"The measure of how well the data quality is based on all rules results.\"), incremental_start STRING OPTIONS(description=\"The incremental start row of the data scan.\"), incremental_end STRING OPTIONS(description=\"The incremental end row of the data scan.\")> OPTIONS(description=\"The overall result of the data quality job.\"),\n  job_dimension_result JSON OPTIONS(description=\"The dimension result of the data quality job.\"),\n  job_rows_scanned INT64 OPTIONS(description=\"The number of rows that have been scanned during this data scan job.\"),\n  rule_name STRING OPTIONS(description=\"Data quality rule name.\"),\n  rule_description STRING OPTIONS(description=\"Data quality rule description.\"),\n  rule_type STRING OPTIONS(description=\"Data quality rule type.\"),\n  rule_evaluation_type STRING OPTIONS(description=\"Data quality rule evaluation type.\"),\n  rule_column STRING OPTIONS(description=\"The column name in the source table of the rule runs against.\"),\n  rule_dimension STRING OPTIONS(description=\"Data quality rule dimension.\"),\n  rule_threshold_percent FLOAT64 OPTIONS(description=\"The minimum percent of passed rows required to pass this rule.\"),\n  rule_parameters JSON OPTIONS(description=\"Data quality rule parameters.\"),\n  rule_passed BOOL OPTIONS(description=\"The result of whether this rule has passed.\"),\n  rule_rows_evaluated INT64 OPTIONS(description=\"The number of rows that have been evaluated for this rule.\"),\n  rule_rows_passed INT64 OPTIONS(description=\"The number of rows that have passed for this rule.\"),\n  rule_rows_passed_percent FLOAT64 OPTIONS(description=\"The percentage of rows that have passed for this rule.\"),\n  rule_rows_null INT64 OPTIONS(description=\"The number of rows with null values for this rule.\"),\n  rule_failed_records_query STRING OPTIONS(description=\"The failed records query of this rule.\"),\n  created_on TIMESTAMP OPTIONS(description=\"The creation time of the data scan.\"),\n  last_updated TIMESTAMP OPTIONS(description=\"The last updated time of the data scan.\"),\n  rule_assertion_row_count INT64 OPTIONS(description=\"The number of rows failing this rule.\")\n)\nPARTITION BY DATE(job_start_time)\nOPTIONS(\n  labels=[(\"goog-drz-dataplex-uuid\", \"8919aecb-1401-4ae6-be0c-a715fc099c7f\"), (\"goog-drz-dataplex-location\", \"us-central1\"), (\"goog-dataplex-datascan-export-table-schema-version\", \"1_2_0\")]\n);"
                            },
                            {
                                "project_id": "governed-data-1pqzajgatl",
                                "dataset_id": "governed_data_curated",
                                "table_name": "sales",
                                "table_ddl": "CREATE TABLE `governed-data-1pqzajgatl.governed_data_curated.sales`\n(\n  product_name STRING,\n  product_description STRING,\n  product_category_name STRING,\n  product_category_description STRING,\n  region STRING,\n  order_datetime TIMESTAMP,\n  price FLOAT64,\n  quantity INT64,\n  customer_id INT64,\n  first_name STRING,\n  last_name STRING,\n  email STRING,\n  phone STRING,\n  gender STRING,\n  ip_address STRING,\n  ssn STRING,\n  address STRING,\n  city STRING,\n  state STRING,\n  zip INT64,\n  credit_card_number STRING\n)\nOPTIONS(\n  labels=[(\"dataplex-dp-published-scan\", \"governed-data-curated-sales-profile-scan\"), (\"dataplex-dp-published-project\", \"governed-data-1pqzajgatl\"), (\"dataplex-dq-published-scan\", \"governed-data-curated-sales-quality-scan\"), (\"dataplex-dp-published-location\", \"us-central1\"), (\"dataplex-dq-published-project\", \"governed-data-1pqzajgatl\"), (\"dataplex-dq-published-location\", \"us-central1\"), (\"dataplex-data-documentation-published-project\", \"governed-data-1pqzajgatl\"), (\"dataplex-data-documentation-published-location\", \"us-central1\"), (\"dataplex-data-documentation-published-scan\", \"afe2b14c2-0621-4cd9-b862-1a7376425430\")]\n);"
                            }            
                        ] 
        }        

    """
    import os

    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    bigquery_region = os.getenv("AGENT_ENV_BIGQUERY_REGION")
    messages = []

    sql = f"""
    SELECT
        table_catalog AS project_id,
        table_schema AS dataset_id,
        table_name,
        ddl as table_ddl
    FROM `{project_id}.region-{bigquery_region}.INFORMATION_SCHEMA.TABLES`
    """

    try:
        # Call the generic runner method to execute the SQL
        json_result = bq_sql.run_bigquery_sql(sql)
        print(f"get_bigquery_table_list -> response: {json.dumps(json_result, indent=2)}")

        return_value = { "status": "success", "tool_name": "get_bigquery_table_list", "query": None, "messages": messages, "results": json_result }
        print(f"get_bigquery_table_list -> return_value: {json.dumps(return_value, indent=2)}")

        return return_value
     
    except Exception as e:
        messages.append(f"Error when calling rest api: {e}")
        return_value = { "status": "failed", "tool_name": "get_bigquery_table_list", "query": None, "messages": messages, "results": None }   
        return return_value