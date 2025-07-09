import os
import json
import data_analytics_agent.rest_api_helper as rest_api_helper

def get_data_discovery_scans() -> dict:
    """
    Lists all Dataplex data discovery scans in the configured region.

    This function specifically filters the results to include only scans of
    type 'DATA_DISCOVERY'.

    Returns:
        dict: A dictionary containing the status and the list of data discovery scans.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans"

    try:
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        messages.append("Successfully retrieved list of all data scans from the API.")

        all_scans = json_result.get("dataScans", [])
        discovery_scans_only = [
            scan for scan in all_scans if scan.get("type") == "DATA_DISCOVERY"
        ]
        messages.append(f"Filtered results. Found {len(discovery_scans_only)} data discovery scans.")

        filtered_results = {"dataScans": discovery_scans_only}

        return {
            "status": "success",
            "tool_name": "get_data_discovery_scans",
            "query": None,
            "messages": messages,
            "results": filtered_results
        }
    except Exception as e:
        messages.append(f"An error occurred while listing data discovery scans: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_discovery_scans",
            "query": None,
            "messages": messages,
            "results": None
        }


def exists_data_discovery_scan(data_discovery_scan_name: str) -> dict:
    """
    Checks if a Dataplex data discovery scan already exists.

    Args:
        data_discovery_scan_name (str): The short name/ID of the data discovery scan.

    Returns:
        dict: A dictionary containing the status and a boolean result.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")

    list_result = get_data_discovery_scans()
    messages = list_result.get("messages", [])

    if list_result["status"] == "failed":
        return list_result

    try:
        scan_exists = False
        full_scan_name_to_find = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_discovery_scan_name}"

        for item in list_result.get("results", {}).get("dataScans", []):
            if item.get("name") == full_scan_name_to_find:
                scan_exists = True
                messages.append(f"Found matching data discovery scan: '{data_discovery_scan_name}'.")
                break
        
        if not scan_exists:
            messages.append(f"Data discovery scan '{data_discovery_scan_name}' does not exist.")

        return {
            "status": "success",
            "tool_name": "exists_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": {"exists": scan_exists}
        }
    except Exception as e:
        messages.append(f"An unexpected error occurred while processing scan list: {e}")
        return {
            "status": "failed",
            "tool_name": "exists_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def create_data_discovery_scan(data_discovery_scan_name: str, display_name: str, gcs_bucket_name: str, biglake_connection_name: str) -> dict:
    """
    Creates a new Dataplex data discovery scan for a GCS bucket to create BigLake tables.

    Args:
        data_discovery_scan_name (str): The short name/ID for the new scan.
        display_name (str): The user-friendly display name for the scan.
        gcs_bucket_name (str): The name of the GCS bucket to scan (e.g., 'my-bucket').
        biglake_connection_name (str): The full resource name of the BigLake connection.
                                      e.g., "projects/.../locations/.../connections/..."

    Returns:
        dict: A dictionary containing the status and results of the operation.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    
    existence_check = exists_data_discovery_scan(data_discovery_scan_name)
    messages = existence_check.get("messages", [])
    
    if existence_check["status"] == "failed":
        return existence_check

    if existence_check["results"]["exists"]:
        full_scan_name = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_discovery_scan_name}"
        return {
            "status": "success",
            "tool_name": "create_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": {"name": full_scan_name, "created": False}
        }

    messages.append(f"Creating Data Discovery Scan '{data_discovery_scan_name}'.")
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans?dataScanId={data_discovery_scan_name}"
    gcs_resource_path = f"//storage.googleapis.com/projects/{project_id}/buckets/{gcs_bucket_name}"

    request_body = {
        "displayName": display_name,
        "type": "DATA_DISCOVERY",
        "data": {"resource": gcs_resource_path},
        "dataDiscoverySpec": {
            "storageConfig": {
                "csvOptions": {"delimiter": ",", "headerRows": 1}
            },
            "bigqueryPublishingConfig": {
                "connection": biglake_connection_name,
                "tableType": "BIGLAKE"
            }
        }
    }

    try:
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        messages.append(f"Successfully initiated Data Discovery Scan creation for '{data_discovery_scan_name}'.")
        return {
            "status": "success",
            "tool_name": "create_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while creating the data discovery scan: {e}")
        return {
            "status": "failed",
            "tool_name": "create_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def start_data_discovery_scan(data_discovery_scan_name: str) -> dict:
    """
    Triggers a run of an existing Dataplex data discovery scan.

    Args:
        data_discovery_scan_name (str): The short name/ID of the scan to run.

    Returns:
        dict: A dictionary containing the status and the job information.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans/{data_discovery_scan_name}:run"
    
    try:
        messages.append(f"Attempting to run Data Discovery Scan '{data_discovery_scan_name}'.")
        json_result = rest_api_helper.rest_api_helper(url, "POST", {})
        job_info = json_result.get("job", {})
        messages.append(f"Successfully started job: {job_info.get('name')} - State: {job_info.get('state')}")
        return {
            "status": "success",
            "tool_name": "start_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while starting the data discovery scan: {e}")
        return {
            "status": "failed",
            "tool_name": "start_data_discovery_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def get_data_discovery_scan_state(data_discovery_scan_job_name: str) -> dict:
    """
    Gets the current state of a running data discovery scan job.

    Args:
        data_discovery_scan_job_name (str): The full resource name of the scan job, e.g., 
                                          "projects/.../locations/.../dataScans/.../jobs/...".
    """
    messages = []
    url = f"https://dataplex.googleapis.com/v1/{data_discovery_scan_job_name}"
    
    try:
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        state = json_result.get("state", "UNKNOWN")
        messages.append(f"Job '{data_discovery_scan_job_name}' is in state: {state}")
        
        return {
            "status": "success",
            "tool_name": "get_data_discovery_scan_state",
            "query": None,
            "messages": messages,
            "results": {"state": state}
        }
    except Exception as e:
        messages.append(f"An error occurred while getting the data discovery scan job state: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_discovery_scan_state",
            "query": None,
            "messages": messages,
            "results": None
        }