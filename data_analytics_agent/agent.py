from dotenv import load_dotenv

from google.adk.agents import LlmAgent
from google.adk.planners import BuiltInPlanner
from google.genai.types import ThinkingConfig

import data_analytics_agent.bigquery.run_bigquery_sql as run_bigquery_sql 
import data_analytics_agent.bigquery.get_bigquery_table_schema as get_bigquery_table_schema
import data_analytics_agent.bigquery.get_bigquery_table_list as get_bigquery_table_list

import data_analytics_agent.google_search.google_search as google_search

import data_analytics_agent.dataplex.data_governance as data_governance
import data_analytics_agent.dataplex.search_data_catalog as data_catalog_search
import data_analytics_agent.dataplex.data_profile as data_profile
import data_analytics_agent.dataplex.data_insights as data_insights
import data_analytics_agent.dataplex.data_quality as data_quality
import data_analytics_agent.dataplex.data_discovery as data_discovery

import data_analytics_agent.conversational_analytics.conversational_analytics_auto_create_agent as conversational_analytics_auto_create_agent
import data_analytics_agent.conversational_analytics.conversational_analytics_chat as conversational_analytics_chat
import data_analytics_agent.conversational_analytics.conversational_analytics_conversation as conversational_analytics_conversation
import data_analytics_agent.conversational_analytics.conversational_analytics_data_agent as conversational_analytics_data_agent
import data_analytics_agent.data_engineering_agent.data_engineering_agent as data_engineering_agent


load_dotenv()

# This is not using the ADK serach tool, it uses it seperately
search_agent = LlmAgent(name="Search",
                        description="Runs a Google internet search. Returns progress log and final results.",
                        tools=[google_search.google_search], # google_search.py now returns a dict
                        model="gemini-2.5-flash") # Aligning model with other agents

bigquery_agent = LlmAgent(name="BigQuery",
                          description="Runs BigQuery queries.",
                          tools=[ get_bigquery_table_list.get_bigquery_table_list,
                                  get_bigquery_table_schema.get_bigquery_table_schema, 
                                  run_bigquery_sql.run_bigquery_sql,                                   
                                ],
                          model="gemini-2.5-flash")

datacatalog_agent = LlmAgent(name="DataCatalog", 
                             description="Searches the data catalog.",
                             tools=[ get_bigquery_table_list.get_bigquery_table_list,
                                     data_catalog_search.search_data_catalog,                                     
                                     data_governance.get_data_governance_for_table
                                   ],
                             model="gemini-2.5-flash")

datascan_agent = LlmAgent(name="DataScan", 
                             description="Provides the ability to manage data scans.",
                             tools=[ data_profile.create_data_profile_scan,
                                     data_profile.start_data_profile_scan,
                                     data_profile.exists_data_profile_scan,
                                     data_profile.get_data_profile_scan_state,
                                     data_profile.get_data_profile_scans,
                                     data_profile.update_bigquery_table_dataplex_labels
                                   ],
                             model="gemini-2.5-flash")

datainsight_agent = LlmAgent(name="DataInsight", 
                             description="Provides the ability to manage data insights.",
                             tools=[ data_insights.create_data_insight_scan,
                                     data_insights.start_data_insight_scan,
                                     data_insights.exists_data_insight_scan,
                                     data_insights.get_data_insight_scan_state,
                                     data_insights.get_data_insight_scans,
                                     data_insights.update_bigquery_table_dataplex_labels_for_insights
                                   ],
                             model="gemini-2.5-flash")

dataquality_agent = LlmAgent(name="DataQuality", 
                             description="Provides the ability to manage data quality scans.",
                             tools=[ data_quality.create_data_quality_scan,
                                     data_quality.start_data_quality_scan,
                                     data_quality.exists_data_quality_scan,
                                     data_quality.get_data_quality_scans,
                                     data_quality.get_data_quality_scan_state,
                                     data_quality.update_bigquery_table_dataplex_labels_for_quality
                                   ],
                             model="gemini-2.5-flash")

dataquality_agent = LlmAgent(name="DataQuality", 
                             description="Provides the ability to manage data quality scans.",
                             tools=[ data_quality.create_data_quality_scan,
                                     data_quality.start_data_quality_scan,
                                     data_quality.exists_data_quality_scan,
                                     data_quality.get_data_quality_scans,
                                     data_quality.get_data_quality_scan_state,
                                     data_quality.update_bigquery_table_dataplex_labels_for_quality
                                   ],
                             model="gemini-2.5-flash")

datadiscovery_agent = LlmAgent(name="DataDiscovery", 
                             description="Provides the ability to manage data discovery of files on Google Cloud Storage scans.",
                             tools=[ data_discovery.create_data_discovery_scan,
                                     data_discovery.start_data_discovery_scan,
                                     data_discovery.exists_data_discovery_scan,
                                     data_discovery.get_data_discovery_scans,
                                     data_discovery.get_data_discovery_scan_state
                                   ],
                             model="gemini-2.5-flash")

conversational_analytics_agent = LlmAgent(name="ConversationalAnalyticsAPI", 
                             description="This agent is used manage and create Google Conversational Analytics API resources.",
                             tools=[ get_bigquery_table_list.get_bigquery_table_list,
                                     conversational_analytics_auto_create_agent.create_conversational_analytics_data_agent,
                                    
                                     #conversational_analytics_chat.conversational_analytics_data_agent_chat_stateful,
                                     #conversational_analytics_chat.conversational_analytics_data_agent_chat_stateless,

                                     conversational_analytics_conversation.conversational_analytics_data_agent_conversations_list,
                                     conversational_analytics_conversation.conversational_analytics_data_agent_conversations_get,
                                     conversational_analytics_conversation.conversational_analytics_data_agent_conversations_exists,
                                     conversational_analytics_conversation.conversational_analytics_data_agent_conversations_create,

                                     conversational_analytics_data_agent.conversational_analytics_data_agent_list,
                                     conversational_analytics_data_agent.conversational_analytics_data_agent_exists,
                                     conversational_analytics_data_agent.conversational_analytics_data_agent_get,
                                     conversational_analytics_data_agent.conversational_analytics_data_agent_create,
                                     conversational_analytics_data_agent.conversational_analytics_data_agent_delete
                                    
                                   ],
                             model="gemini-2.5-flash")

data_engineering_sub_agent = LlmAgent(name="DataEngineering", 
                             description="Provides the ability to create and update data engineering pipelines using natural language prompts.",
                             tools=[ data_engineering_agent.execute_data_engineering_task,
                                     data_engineering_agent.get_worflow_invocation_status
                                   ],
                             model="gemini-2.5-flash")


################################################################################
# Main Agent - Supervisor Pattern using Sub-Agents
################################################################################


coordinator_system_prmompt = """You are a helpful AI assistant that can utlize the below AI LLM Agents.

AI LLM Agents that you can use to answer questions:
- Search:
    - Assists with running searches on the Internet using Google for realtime information.
- BigQuery:
    - Assists with running SQL statements in BigQuery.
    - Assists with getting the schema of a table.
    - Assists with getting a list of all tables in a google cloud project.  This should be called when constructing a query.
- DataCatalog: 
    - Assists with searching the data catalog.
    - Assists with getting the data governance tags on a table (aspect types).
- DataScan:
    - Assists with data profile scans.
- DataInsight:
    - Assists with data insight scans.
- DataQuality:
    - Assists with data quality scans.
- DataDiscovery:
    - Assists with data discovery scans.
- ConversationalAnalyticsAPI:
    - This agent will manage conversational analyitics API requests for conversational analyitics infrastucture.
    - This agent DOES NOT ANSWER conversational analyitics queries by the user like "How much are my sales".  Do not direct requests to this agent.
DataEngineering
    - This agent will create and update pipelines or dataform tasks using natural language.
    - You can also check the execution status of a pipeline.

Rules:
- Do not call the same tool agent with the EXACT same parameters to prevent yourself from looping.
- You should use one of the agents to complete each task.  You may only do basic logic yourself.
- You should always call get_bigquery_table_list to get the correct table and dataset names. 
    - Do not trust the user to state the correct name.

Your name is: Data Beans Agent.
"""

# allow_transfer=True is often implicit with sub_agents in AutoFlow

# Main Agent using Gemini Pro as coordinator
root_agent = LlmAgent(
    name="Coordinator",
    model="gemini-2.5-pro",
    instruction=coordinator_system_prmompt,
    description="Main help desk router.",
    sub_agents=[search_agent, 
                bigquery_agent, 
                datacatalog_agent, 
                datascan_agent, 
                datainsight_agent, 
                dataquality_agent,
                datadiscovery_agent,
                conversational_analytics_agent,
                data_engineering_sub_agent],
        planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True))
)
