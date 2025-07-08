import json
import data_beans_agent.rest_api_helper as rest_api_helper

def get_bigquery_table_schema(dataset_id: str, table_id: str) -> dict:
    """Fetches the schema and metadata for a specific BigQuery table.
    This contains more details than the get_bigquery_table_list tool.    

    Args:
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table whose schema you want to fetch.

    Returns:
        dict: This will return:         
        {
            "status": "success",
            "tool_name": "get_bigquery_table_schema",
            "query": None,
            "messages": ["List of messages during processing"]
            "results":                 {
                    "status": "success",
                    "schema": {
                        "name": "string",
                        "type": "string",
                        "mode": "string",
                        "schema": {
                            "fields": [
                                {
                                    "name": "customer_id",
                                    "type": "INTEGER",
                                    "description": "Unique identifier for the customer."
                                },
                                {
                                    "name": "first_name",
                                    "type": "STRING",
                                    "description": "The first name of the customer."
                                }
                            ]
                        }
                    }
                }
        }
    """
    import os

    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    messages = []
    return_value = None
  
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    try:
        response = rest_api_helper.rest_api_helper(url, "GET", None)
        print(f"get_bigquery_table_schema -> response: {json.dumps(response, indent=2)}")

        if "schema" in response:
            schema = response["schema"]        
            return_value = { "status": "success", "tool_name": "get_bigquery_table_schema", "query": None, "messages": messages, "results": schema }
            print(f"get_bigquery_table_schema -> return_value: {json.dumps(return_value, indent=2)}")
        else:
            messages.add(f"Schema not found in the API response for the specified table ({table_id}).")
            return_value = { "status": "failed", "tool_name": "get_bigquery_table_schema", "query": None, "messages": messages, "results": None }

        return return_value 
      
    except Exception as e:
        messages.add(f"Error when calling rest api: {e}")
        return_value = { "status": "failed", "tool_name": "get_bigquery_table_schema", "query": None, "messages": messages, "results": None }