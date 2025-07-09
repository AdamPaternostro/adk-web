import os
import json
import data_analytics_agent.rest_api_helper as rest_api_helper

def get_data_profile_scans() -> dict:
    """
    Lists all Dataplex data profile scans in the configured region.

    This function specifically filters the results to include only scans of
    type 'DATA_PROFILE'.

    Returns:
        dict: A dictionary containing the status and the list of data profile scans.
        {
            "status": "success" or "failed",
            "tool_name": "get_data_profile_scans",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "dataScans": [ ... list of scan objects of type DATA_PROFILE ... ]
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

        # Filter the returned scans to only include those of type 'DATA_PROFILE'
        all_scans = json_result.get("dataScans", [])
        
        # Using a list comprehension for a concise filter
        profile_scans_only = [
            scan for scan in all_scans if scan.get("type") == "DATA_PROFILE"
        ]

        messages.append(f"Filtered results. Found {len(profile_scans_only)} data profile scans.")

        # Create the final results payload with the filtered list
        filtered_results = {"dataScans": profile_scans_only}

        return {
            "status": "success",
            "tool_name": "get_data_profile_scans",
            "query": None,
            "messages": messages,
            "results": filtered_results
        }
    except Exception as e:
        messages.append(f"An error occurred while listing data profile scans: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_profile_scans",
            "query": None,
            "messages": messages,
            "results": None
        }


def exists_data_profile_scan(data_profile_scan_name: str) -> dict:
    """
    Checks if a Dataplex data profile scan already exists by checking the full list.

    Args:
        data_profile_scan_name (str): The short name/ID of the data profile scan.

    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "exists_data_profile_scan",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "exists": True # or False
            }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")

    # Call the dedicated function to list all scans
    list_result = get_data_profile_scans()
    messages = list_result.get("messages", [])

    # If listing scans failed, propagate the failure
    if list_result["status"] == "failed":
        return {
            "status": "failed",
            "tool_name": "exists_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": None
        }

    try:
        scan_exists = False
        json_payload = list_result.get("results", {})
        full_scan_name_to_find = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_profile_scan_name}"

        # Loop through the list of scans from the results
        for item in json_payload.get("dataScans", []):
            if item.get("name") == full_scan_name_to_find:
                scan_exists = True
                messages.append(f"Found matching scan: '{data_profile_scan_name}'.")
                break
        
        if not scan_exists:
            messages.append(f"Scan '{data_profile_scan_name}' does not exist.")

        return {
            "status": "success",
            "tool_name": "exists_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": {"exists": scan_exists}
        }
    except Exception as e: # Catch potential errors while processing the list
        messages.append(f"An unexpected error occurred while processing scan list: {e}")
        return {
            "status": "failed",
            "tool_name": "exists_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def create_data_profile_scan(data_profile_scan_name: str, data_profile_display_name: str, bigquery_dataset_name: str, bigquery_table_name: str) -> dict:
    """
    Creates a new Dataplex data profile scan if it does not already exist.

    Args:
        data_profile_scan_name (str): The short name/ID for the new data profile scan.
        data_profile_display_name (str): The user-friendly display name for the scan.
        bigquery_dataset_name (str): The BigQuery dataset of the table to be scanned.
        bigquery_table_name (str): The BigQuery table to be scanned.

    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "create_data_profile_scan",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... response from the API call ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    
    # First, check if the data profile scan already exists.
    existence_check = exists_data_profile_scan(data_profile_scan_name)
    messages = existence_check.get("messages", [])
    
    # If the check failed, propagate the failure.
    if existence_check["status"] == "failed":
        return {
            "status": "failed",
            "tool_name": "create_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": None
        }

    # If the scan already exists, report success and stop.
    if existence_check["results"]["exists"]:
        full_scan_name = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_profile_scan_name}"
        return {
            "status": "success",
            "tool_name": "create_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": {"name": full_scan_name, "created": False}
        }

    # If the scan does not exist, proceed with creation.
    messages.append(f"Creating Data Profile Scan '{data_profile_scan_name}'.")
    
    # API endpoint to create a data scan. The scan ID is passed as a query parameter.
    # https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans/create
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans?dataScanId={data_profile_scan_name}"

    # The resource path for the BigQuery table to be scanned.
    bigquery_resource_path = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{bigquery_dataset_name}/tables/{bigquery_table_name}"

    request_body = {
        "dataProfileSpec": {"samplingPercent": 25},
        "data": {"resource": bigquery_resource_path},
        "description": data_profile_display_name,
        "displayName": data_profile_display_name
    }

    try:
        # The create API returns a long-running operation object.
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        
        operation_name = json_result.get("name", "Unknown Operation")
        messages.append(f"Successfully initiated Data Profile Scan creation. Operation: {operation_name}")

        return {
            "status": "success",
            "tool_name": "create_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }

    except Exception as e:
        messages.append(f"An error occurred while creating the data profile scan: {e}")
        return {
            "status": "failed",
            "tool_name": "create_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": None
        }    


def start_data_profile_scan(data_profile_scan_name: str) -> dict:
    """
    Triggers a run of an existing Dataplex data profile scan.

    This initiates a new scan job. To check the status of this job, you will
    need the job name from the results and use the 'get_state_data_profile_scan' tool.

    Args:
        data_profile_scan_name (str): The short name/ID of the data profile scan to run.

    Returns:
        dict: A dictionary containing the status and the job information.
        {
            "status": "success" or "failed",
            "tool_name": "start_data_profile_scan",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "job": {
                    "name": "projects/.../locations/.../dataScans/.../jobs/...",
                    "uid": "...",
                    "createTime": "...",
                    "startTime": "...",
                    "state": "RUNNING",
                    "dataProfileResult": {}
                }
            }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []

    # The API endpoint to run a data scan job. Note the custom ':run' verb at the end.
    # https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans/run
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans/{data_profile_scan_name}:run"

    # The run method requires a POST request with an empty body.
    request_body = {}

    try:
        messages.append(f"Attempting to run Data Profile Scan '{data_profile_scan_name}'.")
        
        # Call the REST API to trigger the scan run.
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        
        # Extract job details for a more informative message.
        # Use .get() for safe access in case the response structure is unexpected.
        job_info = json_result.get("job", {})
        job_name = job_info.get("name", "Unknown Job")
        job_state = job_info.get("state", "Unknown State")

        messages.append(f"Successfully started Data Profile Scan job: {job_name} - State: {job_state}")

        return {
            "status": "success",
            "tool_name": "start_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }

    except Exception as e:
        messages.append(f"An error occurred while starting the data profile scan: {e}")
        return {
            "status": "failed",
            "tool_name": "start_data_profile_scan",
            "query": None,
            "messages": messages,
            "results": None
        }    
    

def get_data_profile_scan_state(data_profile_scan_job_name: str) -> dict:
    """
    Gets the current state of a running data profile scan job.

    The job is created when a scan is started via 'start_data_profile_scan'.

    Args:
        data_profile_scan_job_name (str): The full resource name of the scan job, e.g., 
                                          "projects/.../locations/.../dataScans/.../jobs/...".

    Returns:
        dict: A dictionary containing the status and the job state.
        {
            "status": "success" or "failed",
            "tool_name": "get_data_profile_scan_state",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "state": "SUCCEEDED" # or "RUNNING", "FAILED", etc.
            }
        }
    """
    messages = []
    
    # The API endpoint for getting a job's status is generic. 
    # The job name itself is the full path after the API version.
    url = f"https://dataplex.googleapis.com/v1/{data_profile_scan_job_name}"
    
    try:
        # Make a GET request to the specific job URL.
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        
        # Safely extract the state from the response.
        state = json_result.get("state", "UNKNOWN")
        messages.append(f"Data Profile job '{data_profile_scan_job_name}' is in state: {state}")

        return {
            "status": "success",
            "tool_name": "get_data_profile_scan_state",
            "query": None,
            "messages": messages,
            "results": {"state": state}
        }
    except Exception as e:
        messages.append(f"An error occurred while getting the data profile scan job state: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_profile_scan_state",
            "query": None,
            "messages": messages,
            "results": None
        }
    

def update_bigquery_table_dataplex_labels(dataplex_scan_name: str, bigquery_dataset_name: str, bigquery_table_name: str) -> dict:
    """
    Updates a BigQuery table's labels to link it to a Dataplex data profile scan.

    This operation is necessary for the data profile results to appear in the
    "Data profile" tab of the table details page in the BigQuery Console.

    Args:
        dataplex_scan_name (str): The short name/ID of the data profile scan to link.
        bigquery_dataset_name (str): The BigQuery dataset containing the table.
        bigquery_table_name (str): The BigQuery table to update with labels.

    Returns:
        dict: A dictionary containing the status and the BigQuery API response.
        {
            "status": "success" or "failed",
            "tool_name": "update_bigquery_table_dataplex_labels",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... response from the BigQuery tables.patch API call ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []

    # API endpoint for patching a BigQuery table's metadata.
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{bigquery_dataset_name}/tables/{bigquery_table_name}"

    # The request body contains the specific labels that link the table to the scan.
    request_body = {
        "labels": {
            "dataplex-dp-published-project": project_id,
            "dataplex-dp-published-location": dataplex_region,
            "dataplex-dp-published-scan": dataplex_scan_name,
        }
    }

    try:
        messages.append(f"Patching BigQuery table '{bigquery_dataset_name}.{bigquery_table_name}' with Dataplex labels.")
        
        # Call the REST API using PATCH to update the table's labels.
        json_result = rest_api_helper.rest_api_helper(url, "PATCH", request_body)
        
        messages.append("Successfully updated BigQuery table labels.")

        return {
            "status": "success",
            "tool_name": "update_bigquery_table_dataplex_labels",
            "query": None,
            "messages": messages,
            "results": json_result
        }

    except Exception as e:
        messages.append(f"An error occurred while updating the BigQuery table labels: {e}")
        return {
            "status": "failed",
            "tool_name": "update_bigquery_table_dataplex_labels",
            "query": None,
            "messages": messages,
            "results": None
        }