# source .venv/bin/activate 
# cd /Users/paternostro/adk-web/
# python -m data_analytics_agent.test_agent test-data-eng-agent

import argparse

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


if __name__ == "__main__":
    load_dotenv()

    print()
    print()
    print("===================================================================================================")
    print()
    print()

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Run tests for the data analytics agent.")
    parser.add_argument("test_name", type=str, help="The name of the test to run.")

    args = parser.parse_args()

    if args.test_name == "test-data-eng-agent":
        repository_name = "adam-agent-07"
        workspace_name = "default"
        prompt = """Make the files fields (borough,zone and service_zone) all uppercase in the dataset:data_eng_dataset table:location and saved to a new table in the same dataset named: main_test"""

        execute_data_engineering_task_result = data_engineering_agent.execute_data_engineering_task(repository_name, workspace_name, prompt)

        print()
        print()
        print(f"execute_data_engineering_task_result: {execute_data_engineering_task_result}")

    else:
        print(f"Error: Test '{args.test_name}' not found.")
    

    print()
    print()
    print("===================================================================================================")
    print()
    print()    