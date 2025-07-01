import json
import data_beans_agent.rest_api_helper as rest_api_helper
import data_beans_agent.bigquery_sql as bq_sql 

# https://cloud.google.com/dataplex/docs/search-syntax

def dataplex_search_catalog(query: str) -> dict:
    """Searches the Dataplex Data Catalog for entries matching a query string.

    This is the most powerful discovery tool for finding data assets (like BigQuery tables
    or filesets) across a project. It uses a specific key-value query syntax to filter
    results based on metadata like name, description, columns, or custom metadata tags
    (aspects). Refer to the detailed syntax guide below.

    --- Dataplex Query Syntax Guide ---

    A query consists of one or more predicates joined by logical operators.
    NOTE: type=TABLE and system=BIGQUERY are case sensitive and must be uppercase.

    **1. Basic Predicates (Key-Value Search):**
    - `name:x`: Matches substring 'x' in the resource ID (e.g., table name).
    - `displayname:x`: Matches substring 'x' in the resource's display name.
    - `description:x`: Matches token 'x' in the resource's description.
    - `column:x`: Matches substring 'x' in any column name of the resource's schema.
    - `type=TABLE`: Matches resources of a specific type. Common types: `TABLE`, `FILESET`.
    - `system=BIGQUERY`: Matches resources from a specific system. Common systems: `BIGQUERY`, `CLOUD_STORAGE`.
    - `location=us-central1`: Matches resources in an exact location.
    - `projectid:my-project`: Matches substring 'my-project' in the project ID.
    - `fully_qualified_name:path.to.asset`: Matches substring in the FQN.

    **2. Time-Based Search (createtime, updatetime):**
    - Use operators `=`, `>`, `<`, `>=`, `<=`. Timestamps must be GMT (YYYY-MM-DDThh:mm:ss).
    - `createtime>2023-10-01`: Finds resources created after Oct 1, 2023.
    - `updatetime<2023-01-01T12:00:00`: Finds resources updated before noon on Jan 1, 2023.

    **3. Label Search (for BigQuery resources):**
    - `label:my-label`: The key of the label contains 'my-label'.
    - `label=my-label`: The key of the label is exactly 'my-label'.
    - `label:my-label:some-value`: The value of the label with key 'my-label' contains 'some-value'.
    - `label=my-label=exact-value`: The value of the label with key 'my-label' is exactly 'exact-value'.

    **4. Aspect Search (Custom Metadata):**
    - Search based on custom metadata "aspects" attached to an entry.
    - `aspect:path.to.aspect_type`: Matches entries that have this aspect type attached.
    - `aspect:path.to.field=value`: Searches for specific values within an aspect's fields.
      - Example: `aspect:my_project.us.governance.is_sensitive=true`
      - Example: `aspect:governance.owner_email:"data-team@example.com"`

    **5. Logical Operators:**
    - `AND` is the default. `system=bigquery name:orders` finds BigQuery tables with 'orders' in the name.
    - `OR` must be explicit. `name:customers OR name:users`.
    - `NOT` or `-` for negation. `-system:BIGQUERY` finds assets not in BigQuery.
    - `()` for grouping. `system=bigquery AND (name:orders OR name:sales)`.

    --- Common Use Case Examples ---

    - **Find all BigQuery tables with 'customer' in the name or description:**
      `system=bigquery type=table (name:customer OR description:customer)`

    - **Find sensitive financial tables updated this year:**
      `description:financial updatetime>2024-01-01 aspect:governance.sensitivity=PII`

    - **Find tables in the 'sales' dataset owned by 'sales-team@example.com':**
      `fully_qualified_name:sales. aspect:stewardship.owner="sales-team@example.com"`

    When showing the results to the user ALWAYS show the query.      

    Args:
        query (str): The search query string following the Dataplex syntax outlined above.

    Returns:
        dict: This will return: 
          {
            "status": "success",
            "results": [
                {
                    "linkedResource": "projects/governed-data-1pqzajgatl/datasets/governed_data_raw/models/gemini_model",
                    "dataplexEntry": {
                        "name": "projects/601982832853/locations/us/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/governed-data-1pqzajgatl/datasets/governed_data_raw/models/gemini_model",
                        "entryType": "projects/655216118709/locations/global/entryTypes/bigquery-model",
                        "createTime": "2025-06-12T14:00:04.724264Z",
                        "updateTime": "2025-06-12T14:00:04.724264Z",
                        "parentEntry": "projects/601982832853/locations/us/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/governed-data-1pqzajgatl/datasets/governed_data_raw",
                        "fullyQualifiedName": "bigquery:governed-data-1pqzajgatl.governed_data_raw.gemini_model",
                        "entrySource": {
                            "resource": "projects/governed-data-1pqzajgatl/datasets/governed_data_raw/models/gemini_model",
                            "system": "BIGQUERY",
                            "displayName": "gemini_model",
                            "ancestors": [
                                {
                                    "name": "projects/governed-data-1pqzajgatl/datasets/governed_data_raw",
                                    "type": "dataplex-types.global.bigquery-dataset"
                                }
                            ],
                            "createTime": "2025-06-12T14:00:04.232Z",
                            "updateTime": "2025-06-12T14:00:04.280Z",
                            "location": "us"
                        }
                    },
                    "snippets": {}
                }
            ]
        }         
    

    """
    print("RUNNING: search_dataplex_catalog")
    project_id = "governed-data-1pqzajgatl"
    location = "us"

    # The searchEntries endpoint is a POST request with the query in the body
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{location}:searchEntries"

    # The payload for the POST request (add the project id and page size)
    payload = {
        "pageSize": 50,
        "query": f"projectid={project_id} query",
        "semanticSearch": True
    }

    try:
        response = rest_api_helper.rest_api_helper(url, "POST", payload)
        print(f"dataplex_search_catalog -> response: {json.dumps(response, indent=2)}")

        return_value =  { "status": "success", "query": query, "results": response["results"] }
        print(f"dataplex_search_catalog -> return_value: {json.dumps(return_value, indent=2)}")

        return return_value            

    except Exception as e:
        return { "status": "failed", "query": query, "message": "Error when calling rest api: {e}" }