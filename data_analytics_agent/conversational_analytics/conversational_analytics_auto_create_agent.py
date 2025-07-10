import os
import json
import yaml
import data_analytics_agent.rest_api_helper as rest_api_helper
import data_analytics_agent.conversational_analytics.conversational_analytics_data_agent as conversational_analytics_data_agent
import data_analytics_agent.bigquery.bigquery_sql as bigquery_helper
import data_analytics_agent.gemini.gemini_helper as gemini_helper


def create_conversational_analytics_data_agent(conversational_analytics_data_agent_id:str, bigquery_table_list: list[dict]) -> dict:
    """
    Orchestrates the creation of a complete Conversational Analytics Data Agent.
    
    This high-level workflow performs the following steps:
    1. Checks if the agent already exists.
    2. Gathers BigQuery schema information for the provided tables.
    3. Uses a Large Language Model (LLM) to generate a comprehensive system instruction YAML.
    4. Creates the data agent using the generated instruction and data sources.
    
    Args:
        conversational_analytics_data_agent_id (str): The desired ID for the new data agent.
        bigquery_table_list (list[dict]): A list of dictionaries, each specifying a 
                                           BigQuery table with 'dataset_name' and 'table_name'.
                                           e.g.:
                                           {
                                                "dataset_name" : "governed_data_curated",
                                                "table_name" : "customer",
                                           }

    Returns:
        dict: A dictionary containing the status, a log of messages, and the results of the operation.
              {
                  "status": "success" or "failed",
                  "tool_name": "create_conversational_analytics_data_agent",
                  "query": None,
                  "messages": ["List of messages during processing"],
                  "results": { ... response from the API call or status info ... }
              }
    """
    tool_name = "create_conversational_analytics_data_agent"
    messages = []
    
    # --- Step 1: Check if the data agent already exists ---
    existence_check = conversational_analytics_data_agent.conversational_analytics_data_agent_exists(conversational_analytics_data_agent_id)
    messages.extend(existence_check.get("messages", []))

    if existence_check["status"] == "failed":
        return {
            "status": "failed", "tool_name": tool_name, "query": None, 
            "messages": messages, "results": None
        }

    if existence_check["results"]["exists"]:
        # This is a successful outcome, but no creation occurred.
        return {
            "status": "success", "tool_name": tool_name, "query": None,
            "messages": messages, "results": {"created": False, "reason": "Data agent already exists."}
        }

    # --- Step 2: Prepare data sources and prompts ---
    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    table_references = []
    table_descriptions = ""
    table_names = ""

    messages.append("Preparing data sources and gathering table schemas.")
    for item in bigquery_table_list:
        table_id = f"{project_id}.{item['dataset_name']}.{item['table_name']}"
        table_names += f"- {table_id}\n"
        table_references.append({
            "projectId": project_id,
            "datasetId": item["dataset_name"],
            "tableId": item["table_name"],
        })
        table_description = bigquery_helper.run_bigquery_get_table_schema(table_id) 
        table_descriptions += f"- {table_id}: {table_description}\n"

    bigquery_data_source = {"bq": {"tableReferences": table_references}}
    messages.append("Successfully prepared BigQuery data source references.")

    # --- Step 3: Use Gemini to generate the system instruction YAML ---
    # The full, long prompt from your original code goes here.
    conversational_analytics_prompt = f"""...""" 
    response_schema = {
        "type": "object",
        "properties": {"generated_yaml": {"type": "string"}},
        "required": ["generated_yaml"]
    }

    messages.append("Generating system instruction YAML with Gemini...")
    try:
        gemini_response = gemini_helper.gemini_llm(conversational_analytics_prompt, response_schema=response_schema)
        gemini_response_json = json.loads(gemini_response)
        system_instruction = gemini_response_json["generated_yaml"]
        print("*** system_instruction ***")
        print(system_instruction)
        print("*************************")
        yaml.safe_load(system_instruction) # Validate YAML syntax
        messages.append("✅ Generated System prompt YAML syntax is valid.")
    except Exception as e:
        messages.append(f"❌ ERROR: Failed to generate or validate system instruction YAML: {e}")
        return {
            "status": "failed", "tool_name": tool_name, "query": None,
            "messages": messages, "results": None
        }

    # --- Step 4: Create the Conversational Agent using the agent tool ---
    messages.append("Creating the Conversational Agent...")
    create_result = conversational_analytics_data_agent.conversational_analytics_data_agent_create(
        data_agent_id=conversational_analytics_data_agent_id,
        system_instruction=system_instruction,
        bigquery_data_source=bigquery_data_source,
        enable_python=False
    )
    messages.extend(create_result.get("messages", []))

    # Return the final result, packaging it in the standard format.
    return {
        "status": create_result["status"],
        "tool_name": tool_name,
        "query": None,
        "messages": messages,
        "results": create_result.get("results")
    }