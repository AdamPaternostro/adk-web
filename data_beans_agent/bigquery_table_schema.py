import json
import data_beans_agent.rest_api_helper as rest_api_helper

def get_bigquery_table_schema(dataset_id: str, table_id: str) -> dict:
    """Fetches the schema for a specific BigQuery table using the REST API.

    This function connects to the BigQuery REST API, retrieves the metadata
    for a given table, and extracts just the schema definition.

    Args:
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table whose schema you want to fetch.

    Returns:
        dict: This will return: 
                {
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
    """
    project_id = "governed-data-1pqzajgatl"
  
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    try:
        response = rest_api_helper.rest_api_helper(url, "GET", None)
        print(f"get_bigquery_table_schema -> response: {json.dumps(response, indent=2)}")

        if "schema" in response:
            schema = response["schema"]

            return_value =  { "status": "success", "schema": schema }
            print(f"get_bigquery_table_schema -> return_value: {json.dumps(return_value, indent=2)}")

            return return_value            
        else:
            # This case is unlikely for a valid table but is good defensive programming.
             return { "status": "failed", "message": "Schema not found in the API response for the specified table." }  
    except Exception as e:
        return { "status": "failed", "message": "Error when calling rest api: {e}" }