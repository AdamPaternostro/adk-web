import os
import json
import data_analytics_agent.rest_api_helper as rest_api_helper
import data_analytics_agent.dataplex.data_profile as data_profile


def get_data_quality_scans() -> dict:
    """
    Lists all Dataplex data quality scans in the configured region.

    This function specifically filters the results to include only scans of
    type 'DATA_QUALITY'.

    Returns:
        dict: A dictionary containing the status and the list of data quality scans.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans"

    try:
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        messages.append("Successfully retrieved list of all data scans from the API.")

        all_scans = json_result.get("dataScans", [])
        quality_scans_only = [
            scan for scan in all_scans if scan.get("type") == "DATA_QUALITY"
        ]
        messages.append(f"Filtered results. Found {len(quality_scans_only)} data quality scans.")

        filtered_results = {"dataScans": quality_scans_only}

        return {
            "status": "success",
            "tool_name": "get_data_quality_scans",
            "query": None,
            "messages": messages,
            "results": filtered_results
        }
    except Exception as e:
        messages.append(f"An error occurred while listing data quality scans: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_quality_scans",
            "query": None,
            "messages": messages,
            "results": None
        }


def exists_data_quality_scan(data_quality_scan_name: str) -> dict:
    """
    Checks if a Dataplex data quality scan already exists.

    Args:
        data_quality_scan_name (str): The short name/ID of the data quality scan.

    Returns:
        dict: A dictionary containing the status and a boolean result.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")

    list_result = get_data_quality_scans()
    messages = list_result.get("messages", [])

    if list_result["status"] == "failed":
        return list_result

    try:
        scan_exists = False
        full_scan_name_to_find = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_quality_scan_name}"

        for item in list_result.get("results", {}).get("dataScans", []):
            if item.get("name") == full_scan_name_to_find:
                scan_exists = True
                messages.append(f"Found matching data quality scan: '{data_quality_scan_name}'.")
                break
        
        if not scan_exists:
            messages.append(f"Data quality scan '{data_quality_scan_name}' does not exist.")

        return {
            "status": "success",
            "tool_name": "exists_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": {"exists": scan_exists}
        }
    except Exception as e:
        messages.append(f"An unexpected error occurred while processing scan list: {e}")
        return {
            "status": "failed",
            "tool_name": "exists_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def get_data_quality_scan_recommendations(data_profile_scan_name: str) -> dict:
    """
    Gets recommended data quality rules based on a completed data profile scan.

    Args:
        data_profile_scan_name (str): The short name/ID of the *data profile* scan
                                      from which to generate recommendations.

    Returns:
        dict: A dictionary containing the status and the recommended rules.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans/{data_profile_scan_name}:generateDataQualityRules"

    try:
        messages.append(f"Requesting DQ recommendations from profile scan '{data_profile_scan_name}'.")
        json_result = rest_api_helper.rest_api_helper(url, "POST", {})
        messages.append("Successfully generated recommended data quality rules.")
        return {
            "status": "success",
            "tool_name": "get_data_quality_scan_recommendations",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while generating DQ rule recommendations: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_quality_scan_recommendations",
            "query": None,
            "messages": messages,
            "results": None
        }


def create_data_quality_scan(data_quality_scan_name: str, display_name: str, description: str, 
                             bigquery_dataset_name: str, bigquery_table_name: str,
                             data_profile_scan_name: str) -> dict:
    """
    Creates a new Dataplex data quality scan based on recommended rules from an
    existing data profile scan.

    This function automatically performs the following steps:
    1. Checks if the target data quality scan already exists.
    2. Finds the corresponding data profile scan for the table.
    3. If the profile scan exists, it fetches the recommended quality rules.
    4. It then creates the data quality scan using those recommended rules.

    Args:
        data_quality_scan_name (str): The short name/ID for the new data quality scan.
        display_name (str): The user-friendly display name for the scan.
        description (str): A description for the data quality scan.
        bigquery_dataset_name (str): The BigQuery dataset of the table to be scanned.
        bigquery_table_name (str): The BigQuery table to be scanned.
        data_profile_scan_name (str): The short name/ID of the data profile scan.

    Returns:
        dict: A dictionary containing the status and results of the operation.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []

    # 1. Check if the DATA QUALITY scan we want to create already exists.
    dq_existence_check = exists_data_quality_scan(data_quality_scan_name)
    messages.extend(dq_existence_check.get("messages", []))
    
    if dq_existence_check["status"] == "failed":
        return dq_existence_check # Propagate failure

    if dq_existence_check["results"]["exists"]:
        full_scan_name = f"projects/{project_id}/locations/{dataplex_region}/dataScans/{data_quality_scan_name}"
        return {
            "status": "success",
            "tool_name": "create_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": {"name": full_scan_name, "created": False, "reason": "Scan already exists."}
        }

    # 2. Determine the conventional name of the prerequisite DATA PROFILE scan.
    #data_profile_scan_name = f"{bigquery_dataset_name}-{bigquery_table_name}-profile-scan".lower().replace("_","-")
    #messages.append(f"Checking for prerequisite data profile scan: '{data_profile_scan_name}'")

    # 3. Check if the prerequisite DATA PROFILE scan exists.
    profile_existence_check = data_profile.exists_data_profile_scan(data_profile_scan_name)
    messages.extend(profile_existence_check.get("messages", []))

    if profile_existence_check["status"] == "failed":
        return profile_existence_check # Propagate failure
    
    if not profile_existence_check["results"]["exists"]:
        messages.append(f"Prerequisite data profile scan '{data_profile_scan_name}' not found. Cannot create data quality scan without it.")
        return {
            "status": "failed",
            "tool_name": "create_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": None
        }

    # 4. Get the recommended rules from the existing profile scan.
    recommended_rules_result = get_data_quality_scan_recommendations(data_profile_scan_name)
    messages.extend(recommended_rules_result.get("messages", []))

    if recommended_rules_result["status"] == "failed":
        return recommended_rules_result # Propagate failure

    # 5. Construct the data quality specification from the recommendations.
    rules = recommended_rules_result.get("results", {}).get("rule", [])
    if not rules:
        messages.append("No recommended rules were generated from the profile scan. Cannot create an empty quality scan.")
        return {
            "status": "failed",
            "tool_name": "create_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": None
        }

    # As per the notebook, a default sampling percent of 100 is used.
    data_quality_spec = {
        "rules": rules,
        "samplingPercent": 100
    }
    messages.append(f"Successfully built data quality spec with {len(rules)} recommended rules.")

    # 6. Now, proceed with creating the Data Quality scan.
    messages.append(f"Creating Data Quality Scan '{data_quality_scan_name}' with the generated spec.")
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans?dataScanId={data_quality_scan_name}"
    resource_path = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{bigquery_dataset_name}/tables/{bigquery_table_name}"

    request_body = {
        "dataQualitySpec": data_quality_spec,
        "data": {"resource": resource_path},
        "description": description,
        "displayName": display_name
    }

    try:
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        messages.append(f"Successfully initiated Data Quality Scan creation for '{data_quality_scan_name}'.")
        return {
            "status": "success",
            "tool_name": "create_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred during the final step of creating the data quality scan: {e}")
        return {
            "status": "failed",
            "tool_name": "create_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": None
        }
    

def start_data_quality_scan(data_quality_scan_name: str) -> dict:
    """
    Triggers a run of an existing Dataplex data quality scan.

    Args:
        data_quality_scan_name (str): The short name/ID of the scan to run.

    Returns:
        dict: A dictionary containing the status and the job information.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://dataplex.googleapis.com/v1/projects/{project_id}/locations/{dataplex_region}/dataScans/{data_quality_scan_name}:run"
    
    try:
        messages.append(f"Attempting to run Data Quality Scan '{data_quality_scan_name}'.")
        json_result = rest_api_helper.rest_api_helper(url, "POST", {})
        job_info = json_result.get("job", {})
        messages.append(f"Successfully started job: {job_info.get('name')} - State: {job_info.get('state')}")
        return {
            "status": "success",
            "tool_name": "start_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while starting the data quality scan: {e}")
        return {
            "status": "failed",
            "tool_name": "start_data_quality_scan",
            "query": None,
            "messages": messages,
            "results": None
        }


def get_data_quality_scan_state(data_quality_scan_job_name: str) -> dict:
    """
    Gets the current state of a running data quality scan job.

    Args:
        data_quality_scan_job_name (str): The full resource name of the scan job, e.g., 
                                          "projects/.../locations/.../dataScans/.../jobs/...".
    """
    messages = []
    url = f"https://dataplex.googleapis.com/v1/{data_quality_scan_job_name}"
    
    try:
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        state = json_result.get("state", "UNKNOWN")
        messages.append(f"Job '{data_quality_scan_job_name}' is in state: {state}")
        
        return {
            "status": "success",
            "tool_name": "get_data_quality_scan_state",
            "query": None,
            "messages": messages,
            "results": {"state": state}
        }
    except Exception as e:
        messages.append(f"An error occurred while getting the data quality scan job state: {e}")
        return {
            "status": "failed",
            "tool_name": "get_data_quality_scan_state",
            "query": None,
            "messages": messages,
            "results": None
        }


def update_bigquery_table_dataplex_labels_for_quality(dataplex_scan_name: str, bigquery_dataset_name: str, bigquery_table_name: str) -> dict:
    """
    Updates a BigQuery table's labels to link it to a Dataplex data quality scan.

    Args:
        dataplex_scan_name (str): The short name/ID of the quality scan to link.
        bigquery_dataset_name (str): The BigQuery dataset containing the table.
        bigquery_table_name (str): The BigQuery table to update.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataplex_region = os.getenv("AGENT_ENV_DATAPLEX_REGION")
    messages = []
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{bigquery_dataset_name}/tables/{bigquery_table_name}"

    request_body = {
        "labels": {
            "dataplex-dq-published-project": project_id,
            "dataplex-dq-published-location": dataplex_region,
            "dataplex-dq-published-scan": dataplex_scan_name,
        }
    }

    try:
        messages.append(f"Patching BigQuery table '{bigquery_dataset_name}.{bigquery_table_name}' with Data Quality labels.")
        json_result = rest_api_helper.rest_api_helper(url, "PATCH", request_body)
        messages.append("Successfully updated BigQuery table labels for data quality.")
        return {
            "status": "success",
            "tool_name": "update_bigquery_table_dataplex_labels_for_quality",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        messages.append(f"An error occurred while updating the BigQuery table labels for quality: {e}")
        return {
            "status": "failed",
            "tool_name": "update_bigquery_table_dataplex_labels_for_quality",
            "query": None,
            "messages": messages,
            "results": None
        }