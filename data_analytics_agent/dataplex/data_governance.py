import json
import data_analytics_agent.rest_api_helper as rest_api_helper
import data_analytics_agent.bigquery.run_bigquery_sql as bq_sql 

def get_data_governance_for_table(dataset_id: str, table_name: str) -> dict:
    """
    Gets all the data governance tags on a table (aspect types).

    Args:
        dataset_id (str): The dataset in which the table resides.  
        table_name (str): The name of the table.

    Returns:
        dict: 
        {
            "status": "success",
            "tool_name": "get_data_governance_for_table",
            "query": None,
            "messages": ["List of messages during processing"]
            "results": {
                "name": "projects/governed-data-1pqzajgatl/locations/us/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/governed-data-1pqzajgatl/datasets/governed_data_curated/tables/customer",
                "entryType": "projects/655216118709/locations/global/entryTypes/bigquery-table",
                "createTime": "2025-06-12T14:05:40.087281Z",
                "updateTime": "2025-06-23T15:51:16.656516Z",
                "aspects": {
                    "601982832853.global.data-domain-aspect-type": {
                    "aspectType": "projects/601982832853/locations/global/aspectTypes/data-domain-aspect-type",
                    "createTime": "2025-06-23T15:51:16.006056Z",
                    "updateTime": "2025-06-23T15:51:16.006056Z",
                    "data": {
                        "zone": "Curated"
                    },
                    "aspectSource": {}
                    },
                etc...
                }
        }     
    """
    import os

    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    bigquery_region = os.getenv("AGENT_ENV_BIGQUERY_REGION")
    messages = []

    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{bigquery_region}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_name}?view=ALL"

    try:
        response = rest_api_helper.rest_api_helper(url, "GET", None)
        print(f"get_data_governance_for_table -> response: {json.dumps(response, indent=2)}")

        return_value = { "status": "success", "tool_name": "get_data_governance_for_table", "query": None, "messages": messages, "results": response }
        print(f"get_data_governance_for_table -> return_value: {json.dumps(return_value, indent=2)}")

        return return_value            

    except Exception as e:
        messages.append(f"Error when calling rest api: {e}")
        return_value = { "status": "failed", "tool_name": "get_data_governance_for_table", "query": None, "messages": messages, "results": None }   
        return return_value
