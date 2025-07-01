import os
import json
import time
import requests
import google.auth
import google.auth.transport.requests


# Helper function to avoid code duplication for processing paginated results
def _process_and_paginate_results(session, initial_response_data, project_id, job_id, location, headers):
    """Processes a query result set, handling pagination."""
    all_rows = []
    
    # Process the first page of results
    schema = [field['name'] for field in initial_response_data['schema']['fields']]
    for row in initial_response_data.get('rows', []):
        all_rows.append({schema[i]: cell.get('v') for i, cell in enumerate(row['f'])})

    # Handle subsequent pages
    page_token = initial_response_data.get('pageToken')
    while page_token:
        print(f"Fetching next page of results with pageToken...")
        results_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_id}?location={location}&pageToken={page_token}"
        response = session.get(results_url, headers=headers)
        response.raise_for_status()
        
        page_data = response.json()
        for row in page_data.get('rows', []):
             all_rows.append({schema[i]: cell.get('v') for i, cell in enumerate(row['f'])})
        
        page_token = page_data.get('pageToken')
        
    return all_rows


def run_bigquery_sql(sql: str) -> dict:
    """Executes a SQL statement against Google BigQuery using the REST API.

    When showing the results to the user ALWAYS show the SQL statement.

    IMPORTANT: When formatting the table names in the join clause make sure you use backticks.  e.g.: `governed-data-1pqzajgatl.governed_data_sdp_scan.customer`

    This function connects to the BigQuery REST API and runs the provided SQL.
    It intelligently handles two types of queries:
    1.  Data-returning queries (`SELECT`, `WITH`): It fetches all resulting rows,
        paginating if necessary, and returns them as a JSON array of objects.
    2.  DDL/DML statements (`CREATE`, `INSERT`): It runs the job, waits for
        completion, and returns a JSON object confirming success or raises an
        exception on failure.

    Args:
        sql (str): The full SQL statement to execute on BigQuery.

    Returns:
        dict: This will return one of the following: 
              1. For a SELECT: { "status": "success", "sql_type": "SELECT", "sql": "sql", "job_id": "job_id", "rows": [] }
              2. For a DML: { "status": "success", "sql_type": "DML", "sql": "sql", "job_id": "job_id", "rows_affected": 0 }
    """
    print("--- Starting BigQuery jobs.query Execution ---")

    project_id = "governed-data-1pqzajgatl"
    location="us"

    # 1. Authentication and Setup
    try:
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/bigquery"]
        )
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        print("Successfully authenticated.")
    except google.auth.exceptions.DefaultCredentialsError as e:
        raise Exception(f"Authentication failed. Run 'gcloud auth application-default login'. Error: {e}")

    headers = {"Authorization": f"Bearer {credentials.token}", "Content-Type": "application/json"}
    session = requests.Session()
    
    # 2. Submit Query to the Synchronous Endpoint (jobs.query)
    query_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries"
    
    # Set a reasonable timeout. If the query takes longer, jobComplete will be false.
    payload = {
        "query": sql,
        "useLegacySql": False,
        "timeoutMs": 60 * 1000 * 5 # seconds * ms * minutes
    }
    
    print(f"Submitting query to {query_url} with a 30s timeout...")
    is_select_query = sql.strip().upper().startswith(("SELECT", "WITH"))
    try:       
        response = session.post(query_url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        response_data = response.json()

        job_id = response_data['jobReference']['jobId']
        location = response_data['jobReference']['location']
        job_complete = response_data['jobComplete']
        
    except Exception as e:
        if is_select_query:
            return {"status": "error", "sql_type": "SELECT", "sql": sql, "message": f"{e}" }
        else: # DML/DDL
            return {"status": "error", "sql_type": "DML", "sql": sql, "message": f"{e}" }        

    # 3. Handle the response based on whether the job completed in time
    if job_complete:
        print("Query completed within timeout (fast path).")
        if response_data.get('errors'):
            if is_select_query:
                return {"status": "error", "sql_type": "SELECT", "sql": sql, "job_id": job_id, "message": f"{json.dumps(response_data['errors'], indent=2)}" }
            else: # DML/DDL
                return {"status": "error", "sql_type": "DML", "sql": sql, "job_id": job_id, "message": f"{json.dumps(response_data['errors'], indent=2)}" }        

        print(f"run_bigquery_sql -> response: {json.dumps(response_data, indent=2)}")

        if is_select_query:
            rows = _process_and_paginate_results(session, response_data, project_id, job_id, location, headers)
            return {"status": "success", "sql_type": "SELECT", "sql": sql, "job_id": job_id, "rows": rows}
        else: # DML/DDL
            rows_affected = int(response_data.get('numDmlAffectedRows', 0))
            return {"status": "success", "sql_type": "DML", "sql": sql, "job_id": job_id, "rows_affected": rows_affected}

    else:
        print("Query timed out, falling back to polling (slow path)...")
        # Fallback to polling the jobs.get endpoint
        job_status_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs/{job_id}?location={location}"
        while True:
            time.sleep(2)
            print("Polling job status...")
            job_status_response = session.get(job_status_url, headers=headers)
            job_status_response.raise_for_status()
            status_data = job_status_response.json()

            if status_data['status']['state'] == 'DONE':
                print("Job finished.")
                if status_data['status'].get('errorResult'):
                    #raise Exception(json.dumps(status_data['status']['errorResult'], indent=2))
                    if is_select_query:
                        return {"status": "error", "sql_type": "SELECT", "sql": sql, "job_id": job_id, "message": f"{json.dumps(status_data['status']['errorResult'], indent=2)}" }
                    else: # DML/DDL
                        return {"status": "error", "sql_type": "DML", "sql": sql, "job_id": job_id, "message": f"{json.dumps(status_data['status']['errorResult'], indent=2)}" }                   
                
                # Job is done, now process the final result
                if is_select_query:
                    # We need to fetch the results now that the job is complete
                    results_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs/{job_id}/results?location={location}"
                    final_results_res = session.get(results_url, headers=headers)
                    final_results_res.raise_for_status()
                    rows = _process_and_paginate_results(session, final_results_res.json(), project_id, job_id, location, headers)
                    return {"status": "success", "sql_type": "SELECT", "sql": sql, "job_id": job_id, "rows": rows}
                else: # DML/DDL
                    rows_affected = int(status_data.get('statistics', {}).get('query', {}).get('numDmlAffectedRows', 0))
                    return {"status": "success", "sql_type": "DML", "sql": sql, "job_id": job_id, "rows_affected": rows_affected}
                