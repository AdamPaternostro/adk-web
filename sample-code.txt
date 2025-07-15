import os
import json
import data_analytics_agent.rest_api_helper as rest_api_helper

def get_data_insight_scans() -> dict:
    """
    Lists all Dataplex data insight scans in the configured region.

    This function specifically filters the results to include only scans of
    type 'DATA_DOCUMENTATION', which corresponds to Data Insights.

    Returns:
        dict: A dictionary containing the status and the list of data insight scans.
        {
            "status": "success" or "failed",
            "tool_name": "get_data_insight_scans",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "dataScans": [ ... list of scan objects of type DATA_DOCUMENTATION ... ]
            }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []

    # The URL to list all data scans in the specified project and region.
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans"

    try:
        # Call the REST API to get the list of all existing data scans
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        messages.append("Successfully retrieved list of all data scans from the API.")

        print(f"get_data_insight_scans json_result: {json_result}")

        all_scans = json_result.get("dataScans", [])
        
        # Filter for Data Insights scans, which have the type 'DATA_DOCUMENTATION'
        insight_scans_only = [
            scan for scan in all_scans if scan.get("type") == "DATA_DOCUMENTATION"
        ]

        messages.append(f"Filtered results. Found {len(insight_scans_only)} data insight scans.")

        filtered_results = {"dataScans": insight_scans_only}

        return {
            "status": "success",
            "tool_name": "get_data_insight_scans",
            "query": None,
            "messages": messages,
            "results": filtered_results
        }
    except Exception as e:
        messages.append(f"An error occurred while listing data insight scans: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_insight_scans",
            "query": None,
            "messages": messages,
            "results": None
        }


def exists_data_insight_scan(data_insight_scan_name: str) -> dict:
    """
    Checks if a Dataplex data insight scan already exists.

    Args:
        data_insight_scan_name (str): The short name/ID of the data insight scan.

    Returns:
        dict: A dictionary containing the status and a boolean result.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")

    list_result = get_data_insight_scans()
    messages = list_result.get("messages", [])

    if list_result["status"] == "failed":
        return list_result

    try:
        scan_exists = False
        full_scan_name_to_find = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_insight_scan_name}"

        for item in list_result.get("results", {}).get("dataScans", []):
            if item.get("name") == full_scan_name_to_find:
                scan_exists = True
                messages.append(f"Found matching data insight scan: '{data_insight_scan_name}'.")
                break
        
        if not scan_exists:
            messages.append(f"Data insight scan '{data_insight_scan_name}' does not exist.")

        return {
            "status": "success",
            "tool_name": "exists_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": {"exists": scan_exists}
        }
    except Exception as e:
        messages.append(f"An unexpected error occurred while processing scan list: {e}")
        return {
            "status": "failed",
            "tool_name": "exists_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def create_data_insight_scan(data_insight_scan_name: str, data_insight_display_name: str, bigquery_dataset_name: str, bigquery_table_name: str) -> dict:
    """
    Creates a new Dataplex data insight scan if it does not already exist.

    Args:
        data_insight_scan_name (str): The short name/ID for the new scan.
        data_insight_display_name (str): The user-friendly display name for the scan.
        bigquery_dataset_name (str): The BigQuery dataset of the table to be scanned.
        bigquery_table_name (str): The BigQuery table to be scanned.

    Returns:
        dict: A dictionary containing the status and results of the operation.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    
    existence_check = exists_data_insight_scan(data_insight_scan_name)
    messages = existence_check.get("messages", [])
    
    if existence_check["status"] == "failed":
        return existence_check

    if existence_check["results"]["exists"]:
        full_scan_name = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_insight_scan_name}"
        return {
            "status": "success",
            "tool_name": "create_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": {"name": full_scan_name, "created": False}
        }

    messages.append(f"Creating Data Insight Scan '{data_insight_scan_name}'.")
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans?dataScanId={data_insight_scan_name}"
    bigquery_resource_path = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{bigquery_dataset_name}/tables/{bigquery_table_name}"

    request_body = {
        "displayName": data_insight_display_name,
        "type": "DATA_DOCUMENTATION",
        "dataDocumentationSpec": {},
        "data": {"resource": bigquery_resource_path}
    }

    try:
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        operation_name = json_result.get("name", "Unknown Operation")
        messages.append(f"Successfully initiated Data Insight Scan creation. Operation: {operation_name}")
        return {
            "status": "success",
            "tool_name": "create_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while creating the data insight scan: {e}")
        return {
            "status": "failed",
            "tool_name": "create_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def start_data_insight_scan(data_insight_scan_name: str) -> dict:
    """
    Triggers a run of an existing Dataplex data insight scan.

    Args:
        data_insight_scan_name (str): The short name/ID of the data insight scan to run.

    Returns:
        dict: A dictionary containing the status and the job information.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans/{data_insight_scan_name}:run"
    request_body = {}

    try:
        messages.append(f"Attempting to run Data Insight Scan '{data_insight_scan_name}'.")
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        
        job_info = json_result.get("job", {})
        job_name = job_info.get("name", "Unknown Job")
        job_state = job_info.get("state", "Unknown State")

        messages.append(f"Successfully started Data Insight Scan job: {job_name} - State: {job_state}")

        return {
            "status": "success",
            "tool_name": "start_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while starting the data insight scan: {e}")
        return {
            "status": "failed",
            "tool_name": "start_data_insight_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def get_data_insight_scan_state(data_insight_scan_job_name: str) -> dict:
    """
    Gets the current state of a running data insight scan job.

    Args:
        data_insight_scan_job_name (str): The full resource name of the scan job, e.g., 
                                          "projects/.../locations/.../dataScans/.../jobs/...".

    Returns:
        dict: A dictionary containing the status and the job state.
    """
    messages = []
    # The job name is the full URL path after the v1/
    url = f"https://dataplex.googleapis.com/v1/{data_insight_scan_job_name}"
    
    try:
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        state = json_result.get("state", "UNKNOWN")
        messages.append(f"Job '{data_insight_scan_job_name}' is in state: {state}")
        return {
            "status": "success",
            "tool_name": "get_data_insight_scan_state",
            "query": None,
            "messages": messages,
            "results": {"state": state}
        }
    except Exception as e:
        messages.append(f"An error occurred while getting the scan job state: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_insight_scan_state",
            "query": None,
            "messages": messages,
            "results": None
        }


def update_bigquery_table_dataplex_labels_for_insights(dataplex_scan_name: str, bigquery_dataset_name: str, bigquery_table_name: str) -> dict:
    """
    Updates a BigQuery table's labels to link it to a Dataplex data insight scan.

    Args:
        dataplex_scan_name (str): The short name/ID of the insight scan to link.
        bigquery_dataset_name (str): The BigQuery dataset containing the table.
        bigquery_table_name (str): The BigQuery table to update with labels.

    Returns:
        dict: A dictionary containing the status and the BigQuery API response.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{bigquery_dataset_name}/tables/{bigquery_table_name}"

    request_body = {
        "labels": {
            "dataplex-data-documentation-published-project": project_id,
            "dataplex-data-documentation-published-location": dataplex_region,
            "dataplex-data-documentation-published-scan": dataplex_scan_name,
        }
    }

    try:
        messages.append(f"Patching BigQuery table '{bigquery_dataset_name}.{bigquery_table_name}' with Data Insight labels.")
        json_result = rest_api_helper.rest_api_helper(url, "PATCH", request_body)
        messages.append("Successfully updated BigQuery table labels for data insights.")

        return {
            "status": "success",
            "tool_name": "update_bigquery_table_dataplex_labels_for_insights",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while updating the BigQuery table labels for insights: {e}")
        return {
            "status": "failed",
            "tool_name": "update_bigquery_table_dataplex_labels_for_insights",
            "query": None,
            "messages": messages,
            "results": None
        }