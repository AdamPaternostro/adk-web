import os
import json
import base64
import re
from requests.exceptions import HTTPError 
import data_analytics_agent.rest_api_helper as rest_api_helper
import data_analytics_agent.gemini.gemini_helper as gemini_helper


def exists_bigquery_pipeline(name: str) -> dict:
    """
    Checks if a Dataplex repository with a specific ID already exists.

    Args:
        name (str): The ID of the repository to check for. This corresponds to the
                    last segment of the repository's full resource name.

    Returns:
        dict: A dictionary containing the status and a boolean result.
        {
            "status": "success" or "failed",
            "tool_name": "exists_bigquery_pipeline",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "exists": True or False,
                "name": "Full resource name of the repository if it exists"
            }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION")
    location = os.getenv("AGENT_ENV_DATAFORM_REGION")
    messages = []

    # The URL to list all repositories in the specified project and region. [1]
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories"

    try:
        messages.append(f"Checking for existence of BigQuery Pipeline (Dataform Repository) with ID: '{name}'.")
        # Call the REST API to get the list of all existing repositories. [1]
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        messages.append("Successfully retrieved list of all repositories from the API.")

        repo_exists = False
        repo_full_name = None

        for repo in json_result.get("repositories", []):
            # The full name is in the format: projects/{p}/locations/{l}/repositories/{id}
            full_name_from_api = repo.get("name", "")            

            if full_name_from_api == f"projects/{project_id}/locations/{location}/repositories/{name}":
                repo_exists = True
                repo_full_name = full_name_from_api
                messages.append(f"Found matching repository: '{repo_full_name}'.")
                break
        
        if not repo_exists:
            messages.append(f"Repository with ID '{name}' does not exist.")

        return {
            "status": "success",
            "tool_name": "exists_bigquery_pipeline",
            "query": None,
            "messages": messages,
            "results": {"exists": repo_exists, "name": repo_full_name}
        }

    except Exception as e:
        error_message = f"An error occurred while listing repositories: {e}"
        messages.append(error_message)
        return {
            "status": "failed",
            "tool_name": "exists_bigquery_pipeline",
            "query": None,
            "messages": messages,
            "results": None
        }


def create_bigquery_pipeline(name: str) -> dict:
    """
    Creates a new Dataform repository, referred to as a BigQuery Pipeline, if it does not already exist.

    This function uses a specific label '{"bigquery-workflow":"preview"}' which
    is a temporary method until an official API is released for this functionality.

    Args:
        name (str): The name/ID for the new repository. This will be used as the
                    repositoryId, displayName, and name.

    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "create_bigquery_pipeline",
            "query": None,
            "created": True / False if the workspace was created,
            "messages": ["List of messages during processing"],
            "results": { ... API response from Dataform ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION")
    service_account = os.getenv("AGENT_ENV_DATAFORM_SERVICE_ACCOUNT")

    # Check if the repository already exists before attempting to create it.
    existence_check = exists_bigquery_pipeline(name)
    messages = existence_check.get("messages", [])

    if existence_check["status"] == "failed":
        return existence_check

    if existence_check["results"]["exists"]:
        # If the repository exists, return a success message indicating it wasn't re-created.
        return {
            "status": "success",
            "tool_name": "create_bigquery_pipeline",
            "query": None,
            "created": False,
            "messages": messages,
            "results": {"name": existence_check["results"]["name"]}
        }

    messages.append(f"Creating BigQuery Pipeline (Dataform Repository) '{name}' in region '{dataform_region}'.")
    # The repositoryId is passed as a query parameter. [2]
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories?repositoryId={name}"

    # The request body contains the configuration for the new repository.
    request_body = {
        "serviceAccount": service_account,
        "displayName": name,
        "name": name,
        # This label is a temporary "hack" until a formal API is available.
        "labels": {"bigquery-workflow": "preview"}
    }

    print(f"request_body: {request_body}")

    try:
        # Call the REST API helper to execute the POST request. [2]
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)

        messages.append(f"Successfully initiated the creation of repository '{name}'.")
        print(f"create_bigquery_pipeline json_result: {json_result}")

        return {
            "status": "success",
            "tool_name": "create_bigquery_pipeline",
            "query": None,
            "created": True,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        error_message = f"An error occurred while creating the BigQuery Pipeline '{name}': {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "create_bigquery_pipeline",
            "query": None,
            "created": False,
            "messages": messages,
            "results": None
        }


def create_dataform_pipeline(name: str) -> dict:
    """
    Creates a new, standard Dataform repository if it does not already exist.

    Args:
        name (str): The name/ID for the new repository. This will be used as the
                    repositoryId, displayName, and name.

    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "create_dataform_pipeline",
            "query": None,
            "created": True / False if the workspace was created,
            "messages": ["List of messages during processing"],
            "results": { ... API response from Dataform ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION")
    service_account = os.getenv("AGENT_ENV_DATAFORM_SERVICE_ACCOUNT")

    # Check if the repository already exists before attempting to create it.
    existence_check = exists_bigquery_pipeline(name)
    messages = existence_check.get("messages", [])

    if existence_check["status"] == "failed":
        return existence_check

    if existence_check["results"]["exists"]:
        # If the repository exists, return a success message indicating it wasn't re-created.
        return {
            "status": "success",
            "tool_name": "create_dataform_pipeline",
            "query": None,
            "created": False,
            "messages": messages,
            "results": {"name": existence_check["results"]["name"]}
        }

    messages.append(f"Creating standard Dataform Repository '{name}' in region '{dataform_region}'.")
    # The repositoryId is passed as a query parameter. [2]
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories?repositoryId={name}"

    # The request body for a standard Dataform repository.
    request_body = {
        "serviceAccount": service_account,
        "displayName": name,
        "name": name,
    }

    try:
        # Call the REST API helper to execute the POST request. [2]
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)

        messages.append(f"Successfully initiated the creation of repository '{name}'.")
        print(f"create_dataform_pipeline json_result: {json_result}")

        return {
            "status": "success",
            "tool_name": "create_dataform_pipeline",
            "query": None,
            "created": True,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        error_message = f"An error occurred while creating the Dataform repository '{name}': {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "create_dataform_pipeline",
            "query": None,
            "created": False,            
            "messages": messages,
            "results": None
        }


def exists_workspace(repository_name: str, workspace_name: str) -> dict:
    """
    Checks if a Dataform workspace with a specific name exists within a repository.

    Args:
        repository_name (str): The ID of the repository to check within.
        workspace_name (str): The ID of the workspace to look for.

    Returns:
        dict: A dictionary containing the status and a boolean result.
        {
            "status": "success" or "failed",
            "tool_name": "exists_workspace",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "exists": True or False,
                "name": "Full resource name of the workspace if it exists"
            }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    location = os.getenv("AGENT_ENV_DATAFORM_REGION")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []

    # The URL to list all workspaces in the specified repository. [1]
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces"

    try:
        messages.append(f"Checking for existence of workspace '{workspace_name}' in repository '{repository_name}'.")
        # Call the REST API to get the list of all existing workspaces. [1]
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        messages.append("Successfully retrieved list of workspaces from the API.")

        workspace_exists = False
        workspace_full_name = None

        for ws in json_result.get("workspaces", []):
            full_name_from_api = ws.get("name", "")

            if full_name_from_api == f"projects/{project_id}/locations/{location}/repositories/{repository_name}/workspaces/{workspace_name}":
                workspace_exists = True
                workspace_full_name = full_name_from_api
                messages.append(f"Found matching workspace: '{workspace_full_name}'.")
                break
        
        if not workspace_exists:
            messages.append(f"Workspace with ID '{workspace_name}' does not exist in repository '{repository_name}'.")

        return {
            "status": "success",
            "tool_name": "exists_workspace",
            "query": None,
            "created" : True,
            "messages": messages,
            "results": {"exists": workspace_exists, "name": workspace_full_name}
        }

    except Exception as e:
        error_message = f"An error occurred while listing workspaces: {e}"
        messages.append(error_message)
        return {
            "status": "failed",
            "tool_name": "exists_workspace",
            "query": None,
            "messages": messages,
            "results": None
        }


def create_workspace(repository_name: str, workspace_name: str) -> dict:
    """
    Creates a new Dataform workspace in a repository if it does not already exist.

    Args:
        repository_name (str): The ID of the repository where the workspace will be created.
        workspace_name (str): The name/ID for the new workspace.

    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "create_workspace",
            "query": None,
            "created": True / False if the workspace was created,
            "messages": ["List of messages during processing"],
            "results": { ... API response from Dataform ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")

    # Check if the workspace already exists before attempting to create it.
    existence_check = exists_workspace(repository_name, workspace_name)
    messages = existence_check.get("messages", [])

    if existence_check["status"] == "failed":
        return existence_check

    if existence_check["results"]["exists"]:
        # If the workspace exists, return a success message indicating it wasn't re-created.
        return {
            "status": "success",
            "tool_name": "create_workspace",
            "query": None,
            "created": False,
            "messages": messages,
            "results": {"name": existence_check["results"]["name"]}
        }

    messages.append(f"Creating workspace '{workspace_name}' in repository '{repository_name}'.")
    # The workspaceId is passed as a query parameter. [2]
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces?workspaceId={workspace_name}"

    # The request body for creating a workspace.
    request_body = {
        "name": workspace_name
    }

    try:
        # Call the REST API helper to execute the POST request. [2]
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)

        messages.append(f"Successfully initiated the creation of workspace '{workspace_name}'.")
        #print(f"create_workspace json_result: {json_result}")

        return {
            "status": "success",
            "tool_name": "create_workspace",
            "query": None,
            "created": True,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        error_message = f"An error occurred while creating the workspace '{workspace_name}': {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "create_workspace",
            "query": None,
            "created": False,
            "messages": messages,
            "results": None
        }    


def does_workspace_file_exist(repository_name: str, workspace_name: str, file_path: str) -> dict:
    """
    Checks if a Dataplex file already exists

    Args:
        repository_name (str): The ID of the repository where the workspace will be created.
        workspace_name (str): The name/ID for the new workspace.
        file_path (str): The full path to the file

    Returns:
        dict: A dictionary containing the status and a boolean result.
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION")
    messages = []

    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/default:readFile?path={file_path}"

    try:
        messages.append(f"Checking for existence of file '{file_path}' in workspace '{workspace_name}'.")
        rest_api_helper.rest_api_helper(url, "GET", None)
        messages.append("Successfully called the check for existence of file.")

        return {
            "status": "success",
            "tool_name": "does_workspace_file_exist",
            "query": None,
            "messages": messages,
            "results": {"exists": True}
        }

    except Exception as e:
        # Check if the string representation of the error contains '404'
        if '404' in str(e):
            messages.append(f"File '{file_path}' not found in workspace '{workspace_name}'. This is an expected outcome.")
            return {
                "status": "success",
                "tool_name": "does_workspace_file_exist",
                "query": None,
                "messages": messages,
                "results": {"exists": False}
            }
        else:
            # Handle all other errors as failures
            error_message = f"An unexpected error occurred while checking for existence of file: {e}"
            messages.append(error_message)
            return {
                "status": "failed",
                "tool_name": "does_workspace_file_exist",
                "query": None,
                "messages": messages,
                "results": None
            }


def write_workflow_settings_file(repository_name: str, workspace_name: str) -> dict:
    """
    Writes the 'workflow_settings.yaml' file to a Dataform workspace.

    This function creates the 'workflow_settings.yaml' file with a predefined
    template, populating it with the current project ID and location. This is
    a standard initialization step for Dataform workspaces.

    Args:
        repository_name (str): The ID of the repository containing the workspace.
        workspace_name (str): The ID of the workspace where the file will be written.

    Returns:
        dict: A dictionary containing the status and the result of the writeFile operation.
        {
            "status": "success" or "failed",
            "tool_name": "write_workflow_settings_file",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... API response from the writeFile operation ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []

    # Define the specific file path and content template within the function
    file_path = "workflow_settings.yaml"
    file_content_template = """defaultProject: {project_id}
defaultLocation: {location}
defaultDataset: dataform
defaultAssertionDataset: dataform_assertions
dataformCoreVersion: 3.0.16"""

    try:
        messages.append(f"Preparing to write file '{file_path}' to workspace '{workspace_name}'.")

        # Populate the template with the project and location details.
        final_file_contents = file_content_template.format(
            project_id=project_id,
            location=dataform_region
        )
        messages.append("Successfully formatted file content template.")

        # Base64 encode the populated string.
        encoded_contents = base64.b64encode(final_file_contents.encode('utf-8')).decode('utf-8')
        messages.append("Successfully Base64 encoded file contents.")

        write_url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}:writeFile"
        
        write_request_body = {
            "path": file_path,
            "contents": encoded_contents
        }

        # Execute the writeFile request
        write_result = rest_api_helper.rest_api_helper(write_url, "POST", write_request_body)
        messages.append(f"Successfully wrote file '{file_path}'.")
        #print(f"write_workflow_settings_file result: {write_result}")

        return {
            "status": "success",
            "tool_name": "write_workflow_settings_file",
            "query": None,
            "messages": messages,
            "results": write_result 
        }

    except Exception as e:
        error_message = f"An error occurred during the file write process: {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "write_workflow_settings_file",
            "query": None,
            "messages": messages,
            "results": None
        }
       

def write_actions_yaml_file(repository_name: str, workspace_name: str) -> dict:
    """
    Writes a placeholder 'actions.yaml' file to a Dataform workspace.

    This function is specifically designed to create the 'definitions/actions.yaml'
    file with the content 'actions: []', which is often required for initializing
    BigQuery Pipelines.

    Args:
        repository_name (str): The ID of the repository containing the workspace.
        workspace_name (str): The ID of the workspace where the file will be written.

    Returns:
        dict: A dictionary containing the status and the result of the writeFile operation.
        {
            "status": "success" or "failed",
            "tool_name": "write_actions_yaml_file",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... API response from the writeFile operation ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []
    
    # Define the specific file path and content within the function
    file_path = "definitions/actions.yaml"
    file_contents = "actions: []"

    try:
        messages.append(f"Writing placeholder file '{file_path}' to workspace '{workspace_name}'.")
        
        write_url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}:writeFile"
        
        # Base64 encode the predefined file contents
        encoded_contents = base64.b64encode(file_contents.encode('utf-8')).decode('utf-8')

        write_request_body = {
            "path": file_path,
            "contents": encoded_contents
        }

        # Execute the writeFile request
        write_result = rest_api_helper.rest_api_helper(write_url, "POST", write_request_body)
        messages.append(f"Successfully wrote file '{file_path}'.")
        #print(f"write_actions_yaml_file result: {write_result}")

        return {
            "status": "success",
            "tool_name": "write_actions_yaml_file",
            "query": None,
            "messages": messages,
            "results": write_result 
        }

    except Exception as e:
        error_message = f"An error occurred during the file write process: {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "write_actions_yaml_file",
            "query": None,
            "messages": messages,
            "results": None
        }   


def commit_to_workspace(repository_name: str, workspace_name: str, author_name: str, author_email: str, commit_message: str) -> dict:
    """
    Commits pending changes in a Dataform workspace.

    Args:
        repository_name (str): The ID of the repository containing the workspace.
        workspace_name (str): The ID of the workspace with pending changes to commit.
        author_name (str): The name of the user to be credited as the author of the commit.
        author_email (str): The email address of the commit author.
        commit_message (str): The message describing the changes being committed.

    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "commit_to_workspace",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... API response from Dataform ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []

    # The API endpoint for committing to a workspace.
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}:commit"

    # The request body containing the author and commit message.
    request_body = {
        "author": {
            "name": author_name,
            "emailAddress": author_email
        },
        "commitMessage": commit_message
    }

    try:
        messages.append(f"Attempting to commit changes to workspace '{workspace_name}' in repository '{repository_name}'.")

        # Call the REST API helper to execute the POST request.
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        
        messages.append(f"Successfully committed changes with message: '{commit_message}'.")
        #print(f"commit_to_workspace json_result: {json_result}")

        return {
            "status": "success",
            "tool_name": "commit_to_workspace",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        error_message = f"An error occurred while committing to the workspace '{workspace_name}': {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "commit_to_workspace",
            "query": None,
            "messages": messages,
            "results": None
        }
    

def rollback_workspace(repository_name: str, workspace_name: str) -> dict:
    """
    Rollsback pending changes in a Dataform workspace.

    Args:
        repository_name (str): The ID of the repository containing the workspace.
        workspace_name (str): The ID of the workspace with pending changes to commit.


    Returns:
        dict: A dictionary containing the status and results of the operation.
        {
            "status": "success" or "failed",
            "tool_name": "commit_to_workspace",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... API response from Dataform ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []

    # The API endpoint for committing to a workspace.
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}:reset"

    # The request body containing the author and commit message.
    request_body = {
        "clean": True
    }

    try:
        messages.append(f"Attempting to rollback changes to workspace '{workspace_name}' in repository '{repository_name}'.")

        # Call the REST API helper to execute the POST request.
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        
        messages.append(f"Successfully rolled back changes'.")
        #(f"commit_to_workspace json_result: {json_result}")

        return {
            "status": "success",
            "tool_name": "rollback_workspace",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        error_message = f"An error occurred while rolling back the workspace '{workspace_name}': {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "rollback_workspace",
            "query": None,
            "messages": messages,
            "results": None
        }
    

def perform_data_engineering_task(repository_name: str, workspace_name: str, prompt: str) -> dict:
    """
    Sends a natural language prompt to the Gemini Data Analytics service to generate and execute a data pipeline.

    Args:
        repository_name (str): The ID of the Dataform repository to use for the pipeline.
        workspace_name (str): The ID of the Dataform workspace within the repository.
        prompt (str): The natural language prompt describing the data engineering task to be performed.

    Returns:
        dict: A dictionary containing the status and the response from the API, which may include the generated code and task status.
        {
            "status": "success" or "failed",
            "tool_name": "perform_data_engineering_task",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... API response from Gemini Data Analytics service ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []

    # The global endpoint for the Gemini Data Analytics service.
    url = f"https://geminidataanalytics.googleapis.com/v1alpha1/projects/{project_id}/locations/global:run"

    # The pipeline_id is the full resource name of the Dataform workspace.
    pipeline_id = f"projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}"

    # The request body containing the pipeline and the user's prompt.
    request_body = {
      "parent": f"projects/{project_id}/locations/global",
      "pipeline_id": pipeline_id,
      "messages": [
        {
          "user_message": {
            "text": prompt
          }
        }
      ]
    }

    try:
        messages.append(f"Attempting to perform data engineering task in workspace '{workspace_name}' with prompt: '{prompt}'.")

        # Call the REST API helper to execute the POST request.
        json_result = rest_api_helper.rest_api_helper(url, "POST", request_body)
        
        messages.append("Successfully submitted the data engineering task to the Gemini Data Analytics service.")
        #print(f"perform_data_engineering_task json_result: {json_result}")

        return {
            "status": "success",
            "tool_name": "perform_data_engineering_task",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        error_message = f"An error occurred while performing the data engineering task: {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "perform_data_engineering_task",
            "query": None,
            "messages": messages,
            "results": None
        }    
    

def compile_and_run_dataform_workflow(repository_name: str, workspace_name: str) -> dict:
    """
    Compiles a Dataform repository from a workspace and then runs the resulting workflow.

    This function performs two sequential operations:
    1. It creates a compilation result from the specified workspace.
    2. It starts a workflow invocation using the successful compilation result.

    Args:
        repository_name (str): The ID of the Dataform repository to compile and run.
        workspace_name (str): The ID of the workspace containing the code to be compiled.

    Returns:
        dict: A dictionary containing the status and the final response from the workflow invocation API call.
        {
            "status": "success" or "failed",
            "tool_name": "compile_and_run_dataform_workflow",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": { ... API response from the workflow invocation ... }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    messages = []

    try:
        # --- Step 1: Compile the repository from the workspace ---
        messages.append(f"Step 1: Compiling repository '{repository_name}' from workspace '{workspace_name}'.")
        
        compile_url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/compilationResults"
        
        workspace_full_path = f"projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}"
        
        compile_request_body = {
            "workspace": workspace_full_path
        }

        compile_result = rest_api_helper.rest_api_helper(compile_url, "POST", compile_request_body)
        compilation_result_name = compile_result.get("name")

        if not compilation_result_name:
            raise Exception("Failed to get compilation result name from the compilation API response.")

        messages.append(f"Successfully compiled. Compilation result name: {compilation_result_name}")

        # --- Step 2: Run the workflow using the compilation result ---
        messages.append(f"Step 2: Starting workflow execution for compilation '{compilation_result_name}'.")

        invoke_url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workflowInvocations"
        
        invoke_request_body = {
            "compilationResult": compilation_result_name
        }
        
        invoke_result = rest_api_helper.rest_api_helper(invoke_url, "POST", invoke_request_body)
        
        messages.append("Successfully initiated workflow invocation.")
        #(f"compile_and_run_dataform_workflow invoke_result: {invoke_result}")

        return {
            "status": "success",
            "tool_name": "compile_and_run_dataform_workflow",
            "query": None,
            "messages": messages,
            "workflow_invocation_id": invoke_result["name"].rsplit('/', 1)[-1],
            "results": invoke_result 
        }

    except Exception as e:
        error_message = f"An error occurred during the compile and run process: {e}"
        messages.append(error_message)
        print(error_message)
        return {
            "status": "failed",
            "tool_name": "compile_and_run_dataform_workflow",
            "query": None,
            "messages": messages,
            "results": None
        }    
    
 
def execute_data_engineering_task(workflow_name: str, workflow_type: str, prompt: str) -> dict:
    """
    Orchestrates a complete data engineering workflow from a natural language prompt.

    This high-level function acts as a single point of entry for an LLM agent to
    create, configure, and execute a data transformation pipeline. It automates the
    entire lifecycle.

    Args:
        workflow_name (str): The user-friendly name for the data engineering workflow.
                             This will be sanitized to form the repository ID.
        workflow_type (str): The type of workflow to create. Must be either 'PIPELINE'
                             (for a BigQuery Pipeline) or 'DATAFORM' (for a standard
                             Dataform repository).
        prompt (str): A detailed, natural language prompt describing the data
                      transformation to be performed (e.g., "Make the files fields
                      (borough,zone) all uppercase in the table project.dataset.table
                      and save to a new table named new_table").

    Returns:
        dict: A dictionary containing the final status of the operation and the results
              from the workflow invocation API call.
              {
                  "status": "success" or "failed",
                  "tool_name": "execute_data_engineering_task",
                  "query": None,
                  "messages": ["A complete log of all steps taken"],
                  "workflow_name": "This is the clean generated name of the workflow (dataform repository).",
                  "workflow_invocation_id": "This is the id that can be used to check on the workflow status."
                  "results": { ... results of the final step, or error details ... }
              }
    """
    #    1.  **Sanitizes Workflow Name**: Creates a valid, safe repository name from the
    #        user-provided workflow_name.
    #    2.  **Ensures Repository Exists**: Creates a new Dataform repository of the specified
    #        type ('PIPELINE' for BigQuery Pipelines or 'DATAFORM' for standard repos)
    #        if one does not already exist.
    #    3.  **Ensures Workspace Exists**: Creates a default workspace within the repository.
    #    4.  **Initializes Workspace**: If the workspace is new, it initializes it with the
    #        necessary configuration files ('workflow_settings.yaml' and 'actions.yaml' for
    #        pipelines) and commits them.
    #    5.  **Generates Code**: Calls the Gemini Data Analytics service with the user's
    #        prompt to generate the required SQLX files for the data task.
    #    6.  **Commits Generated Code**: Commits the newly generated files to the workspace.
    #    7.  **Compiles and Executes**: Compiles the workspace and triggers a workflow
    #        invocation to run the data pipeline in BigQuery.   
    #  
    # Initialize the standard response object
    response = {
        "status": "success",
        "tool_name": "execute_data_engineering_task",
        "query": None,
        "messages": [],
        "results": None
    }

    try:
        # --- 1. Get Environment Variables and Sanitize Inputs ---
        author_name = os.getenv("AGENT_ENV_DATAFORM_AUTHOR_NAME", "Data Engineering Agent")
        author_email = os.getenv("AGENT_ENV_DATAFORM_AUTHOR_EMAIL", "agent@example.com")
        workspace_name = os.getenv("AGENT_ENV_DATAFORM_WORKSPACE_DEFAULT_NAME", "default")
        location =  os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
        
        s_name = re.sub(r'[^a-z0-9-]', '', workflow_name.lower().replace(" ", "-"))
        repository_name = f"{s_name}-{os.getenv('AGENT_ENV_UNIQUE_ID', 'workflow')}"
        response["messages"].append(f"Sanitized workflow name to '{repository_name}'.")
        response["workflow_name"] = repository_name

        print()
        print()
        print("########## execute_data_engineering_task ##########")
        print(f"execute_data_engineering_task: repository_name: {repository_name}")

        # --- Helper to process step results ---
        def process_step(step_name: str, result: dict):
            response["messages"].extend(result.get("messages", []))
            if result["status"] == "failed":
                raise Exception(f"Failed during step: {step_name}. See messages for details.")
        
        workflow_type = workflow_type.upper()

        # --- 2. Create Repository ---
        repo_result = None
        if workflow_type == "PIPELINE":
            create_bigquery_pipeline_result = create_bigquery_pipeline(repository_name)
            print()
            print()
            print(f"create_bigquery_pipeline_result: {create_bigquery_pipeline_result}")
            process_step("Create BigQuery Pipeline", create_bigquery_pipeline_result)
        else:
            create_dataform_pipeline_result = create_dataform_pipeline(repository_name)
            print()
            print()
            print(f"create_dataform_pipeline_result: {create_dataform_pipeline_result}")
            process_step("Create Dataform Pipeline", create_dataform_pipeline_result)

        # --- 3. Create Workspace and Initialize if New ---      
        create_workspace_result = create_workspace(repository_name, workspace_name)
        print()
        print()
        print(f"create_workspace_result: {create_workspace_result}")
        process_step("Create Workspace", create_workspace_result)

        does_workspace_file_exist_result = does_workspace_file_exist(repository_name, workspace_name, "workflow_settings.yaml")
        print()
        print()
        print(f"does_workspace_file_exist_result: {does_workspace_file_exist_result}")

        if does_workspace_file_exist_result["results"]["exists"] == False:
            write_workflow_settings_file_result = write_workflow_settings_file(repository_name, workspace_name)
            print()
            print()
            print(f"write_workflow_settings_file_result: {write_workflow_settings_file_result}")   
            process_step("Write Workflow Settings", write_workflow_settings_file_result)           

            commit_to_workspace_result = commit_to_workspace(repository_name, workspace_name, author_name, author_email, "Commit of workflow_settings.yaml")
            print()
            print()
            print(f"commit_to_workspace_result: {commit_to_workspace_result}")  
            process_step("Initial Commit", commit_to_workspace_result)
        else:
            print()
            print()
            print("Workspace workflow files exists.")
            response["messages"].append(f"Workspace workflow files exists.")

        
        print()
        print()
        print("Running agent.... please wait....")
        perform_data_engineering_task_result = perform_data_engineering_task(repository_name, workspace_name, prompt)
        print()
        print()
        print(f"perform_data_engineering_task_result: {perform_data_engineering_task_result}")
        process_step("Running BigQuery Data Engineering Agent", perform_data_engineering_task_result)
        

        llm_as_a_judge_result = gemini_helper.llm_as_a_judge(prompt, perform_data_engineering_task_result["results"])
        print()
        print()
        print(f"llm_as_a_judge_result: {llm_as_a_judge_result}")
        response["messages"].append(f"LLM As a Judge Result, did the Data Agent work? {llm_as_a_judge_result}")


        if llm_as_a_judge_result == True:
            commit_to_workspace_after_agent_response = commit_to_workspace(repository_name, workspace_name, author_name, author_email, "Commit data engineering agent code")
            print()
            print()
            print(f"commit_to_workspace_after_agent_response: c {commit_to_workspace_after_agent_response}")
            process_step("Commit data engineering agent code", commit_to_workspace_after_agent_response)

            does_actions_file_exist_result = does_workspace_file_exist(repository_name, workspace_name, "definitions/actions.yaml")
            print()
            print()
            print(f"does_actions_file_exist_result: {does_actions_file_exist_result}")
            process_step("Checking for definitions/actions.yaml", does_actions_file_exist_result)

            if does_actions_file_exist_result["results"]["exists"] == False:
                write_actions_yaml_file_result = write_actions_yaml_file(repository_name, workspace_name)
                print()
                print()
                print(f"write_actions_yaml_file_result: {write_actions_yaml_file_result}")
                process_step("Adding file definitions/actions.yaml", write_actions_yaml_file_result)

                commit_to_workspace_actions_result = commit_to_workspace(repository_name, workspace_name, author_name, author_email, "Commit of actions.yaml")
                print()
                print()
                print(f"commit_to_workspace_actions_result: {commit_to_workspace_actions_result}")
                process_step("Committing file definitions/actions.yaml", write_actions_yaml_file_result)

            compile_and_run_dataform_workflow_result = compile_and_run_dataform_workflow(repository_name, workspace_name)
            print()
            print()
            print(f"compile_and_run_dataform_workflow_result: {compile_and_run_dataform_workflow_result}")
            process_step("Compiling and starting the worfklow", compile_and_run_dataform_workflow_result)

            print()
            print()
            print(f"You can view your workflow at https://console.cloud.google.com/bigquery/dataform/locations/us-central1/repositories/{repository_name}/workspaces/{workspace_name}")
                
            response["messages"].append(f"You can view your workflow at https://console.cloud.google.com/bigquery/dataform/locations/{location}/repositories/{repository_name}/workspaces/{workspace_name}")

            # If all steps succeeded, set the final results
            response["workflow_invocation_id"] = compile_and_run_dataform_workflow_result["workflow_invocation_id"]
            response["results"] = compile_and_run_dataform_workflow_result["results"]        

        else:
            rollback_workspace_result = rollback_workspace(repository_name, workspace_name)
            print(f"rollback_workspace_result: {rollback_workspace_result}")
            response["status"] = "failed"
            response["results"] = perform_data_engineering_task_result["results"]  
        
        return response

    except Exception as e:
        response["status"] = "failed"
        error_message = f"An unrecoverable error occurred in the workflow: {e}"
        response["messages"].append(error_message)
        response["results"] = {"error": str(e)}
        print(error_message) # Also log to console

    return response


def get_worflow_invocation_status(repository_name: str, workflow_invocation_id: str) -> dict:
    """
    Checks on the execution status of a workflow.

    Args:
        repository_name (str): The ID of the Dataform repository to compile and run.
        workflow_invocation_id (str): The ID (guid) of workflow invocations id executing a pipeline.  It will return 
            a workflow_invocation_id value which can be used to check on the execution status.

    Returns:
        dict: A dictionary containing the status and a boolean result.
        {
            "status": "success" or "failed",
            "tool_name": "get_worflow_invocation_status",
            "query": None,
            "messages": ["List of messages during processing"],
            "results": {
                "name": "projects/governed-data-1pqzajgatl/locations/us-central1/repositories/adam-agent-10-workflow/workflowInvocations/1752598992-06e003bc-aad3-477f-b761-02629a4d554f",
                "compilationResult": "projects/601982832853/locations/us-central1/repositories/adam-agent-10-workflow/compilationResults/d4a2fa7c-c546-428a-814a-b8eece65a559",
                "state": "SUCCEEDED",
                "invocationTiming": {
                    "startTime": "2025-07-15T17:03:12.313196Z",
                    "endTime": "2025-07-15T17:03:17.650637343Z"
                },
                "resolvedCompilationResult": "projects/601982832853/locations/us-central1/repositories/adam-agent-10-workflow/compilationResults/d4a2fa7c-c546-428a-814a-b8eece65a559",
                "internalMetadata": "{\"db_metadata_insert_time\":\"2025-07-15T17:03:12.321373Z\",\"quota_server_enabled\":true,\"service_account\":\"service-601982832853@gcp-sa-dataform.iam.gserviceaccount.com\"}"
                }
        }
    """
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION")
    messages = []

    # The URL to list all repositories in the specified project and region. [1]
    url = f"https://dataform.googleapis.com/v1/projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workflowInvocations/{workflow_invocation_id}"
    print(url)

    try:
        messages.append(f"Checkin on workflow invoation status with workflow_invocation_id: '{workflow_invocation_id}'.")
        # Call the REST API to get the list of all existing repositories. [1]
        json_result = rest_api_helper.rest_api_helper(url, "GET", None)
        print(json_result)

        return {
            "status": "success",
            "tool_name": "get_worflow_invocation_status",
            "query": None,
            "messages": messages,
            "results": json_result
        }

    except Exception as e:
        print(e)
        # Check if the string representation of the error contains '404'
        if '404' in str(e):
            messages.append(f"Workflow Invocation not found for '{workflow_invocation_id}'. This is an expected outcome.")
            return {
                "status": "success",
                "tool_name": "get_worflow_invocation_status",
                "query": None,
                "messages": messages,
                "results": { "state" : "NOT_FOUND" }
            }
        else:
            # Handle all other errors as failures
            error_message = f"An unexpected error occurred while checking for existence of file: {e}"
            messages.append(error_message)
            return {
                "status": "failed",
                "tool_name": "get_worflow_invocation_status",
                "query": None,
                "messages": messages,
                "results": None
            }

