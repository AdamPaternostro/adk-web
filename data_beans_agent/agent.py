from google.adk.agents import LlmAgent
import data_beans_agent.bigquery_sql as bq_sql 
import data_beans_agent.bigquery_table_schema as bq_schema
import data_beans_agent.bigquery_get_tables_in_project as bq_tables
import data_beans_agent.dataplex_get_data_governance_for_table as dataplex_table_governance
import data_beans_agent.dataplex_search_catalog as data_catalog_search
import data_beans_agent.google_search as google_search

# This is not using the ADK serach tool, it uses it seperately
search_agent = LlmAgent(name="Search",
                        description="Runs a Google internet search. Returns progress log and final results.",
                        tools=[google_search.google_search], # google_search.py now returns a dict
                        model="gemini-2.5-flash") # Aligning model with other agents

bigquery_agent = LlmAgent(name="BigQuery",
                          description="Runs BigQuery queries.",
                          tools=[ bq_sql.run_bigquery_sql, 
                                  bq_schema.get_bigquery_table_schema, 
                                  bq_tables.get_bigquery_get_tables_in_project
                                ],
                          model="gemini-2.5-flash")

datacatalog_agent = LlmAgent(name="DataCatalog", 
                             description="Searches the data catalog.",
                             tools=[ data_catalog_search.dataplex_search_catalog,
                                     bq_tables.get_bigquery_get_tables_in_project,
                                     dataplex_table_governance.get_data_governance_for_table],
                             model="gemini-2.5-flash")

coordinator_system_prmompt = """You are a helpful AI assistant that can utlize the below AI LLM Agents.

AI LLM Agents that you can use to answer questions:
- Search:
    - Assists with running searches on the Internet using Google for realtime information.
- BigQuery:
    - Assists with running SQL statements in BigQuery.
    - Assists with getting the schema of a table.
    - Assists with getting a list of all tables in a google cloud project.
- DataCatalog: 
    - Assists with searching the data catalog.
    - Assists with getting the data governance tags on a table (aspect types).

Rules:
- Do not call the same tool agent with the EXACT same parameters to prevent yourself from looping.
- You should use one of the agents to complete each task.  You may only do basic logic yourself.
- You should always call get_bigquery_get_tables_in_project to get the correct table and dataset names. 
    - Do not trust the user to state the correct name.

Your name is: Data Beans Agent.
"""

# --- AdamService Agent Definition ---
from .adam import adam_html_tool

adam_service_agent = LlmAgent(
    name="AdamService",
    description="Provides HTML content for the /adam command.",
    tools=[adam_html_tool],
    model="gemini-2.5-flash" # Placeholder if model is mandatory by LlmAgent
    # Ideally, this agent would not use an LLM for this simple tool.
    # The ADK framework might have a simpler agent base class or configuration for this.
)

from google.adk.planners import BuiltInPlanner
from google.genai.types import ThinkingConfig

root_agent = LlmAgent(
    name="Coordinator",
    model="gemini-2.5-pro",
    instruction=coordinator_system_prmompt,
    description="Main help desk router.",
    # allow_transfer=True is often implicit with sub_agents in AutoFlow
    sub_agents=[search_agent, bigquery_agent, datacatalog_agent, adam_service_agent],
        planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True))
)



# Note: For this `adam_service_agent` to be discoverable and callable:
# 1. The ADK server process needs to be aware of it. If the server loads all agents
#    from this module or a specified directory, this definition might be sufficient.
# 2. The `/list-apps` endpoint should ideally now include "AdamService".
# 3. The `/run_sse` endpoint should be able to route requests to it
#    when `appName: "AdamService"` is specified in the AgentRunRequest.
