# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import uuid
import time
from typing import Any, Coroutine, List
from google.adk.agents import Agent
from google.adk.events import Event
from google.adk.runners import Runner
from google.adk.tools import LongRunningFunctionTool, FunctionTool
from google.adk.sessions import InMemorySessionService, Session
from google.genai import types
from dotenv import load_dotenv


# --- Tool Definitions (Unchanged) ---
def start_profile_scan_job(profile_name: str) -> dict[str, Any]:
    """Initiates a profile scan job that runs in the background."""
    print(f"\n[SYSTEM]: Starting profile scan for '{profile_name}'.")
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    print(f"[SYSTEM]: Job started with ID: {job_id}")
    return {'status': 'RUNNING', 'job_id': job_id, 'profile_name': profile_name}

def update_bigquery_metadata(job_id: str, final_status: str) -> str:
    """Updates BigQuery with the final metadata from a completed job."""
    print(f"\n[SYSTEM]: Job {job_id} completed with status '{final_status}'. Updating BigQuery...")
    time.sleep(2) # Simulate BQ update
    return f"BigQuery metadata updated successfully for job {job_id}."

long_running_scanner_tool = LongRunningFunctionTool(func=start_profile_scan_job)
bigquery_update_tool = FunctionTool(func=update_bigquery_metadata)


# --- Agent Definition (MODIFIED) ---
scanning_agent = Agent(
    model="gemini-2.5-flash",
    name='profile_scanning_agent',
    instruction="""
      You are a helpful and aware data operations agent.

      CONTEXT: Before the user's message, you may be given a 'System Context' that lists currently running background jobs.

      Your tasks:
      1.  **Starting Jobs:** If the user asks to start a scan, you MUST use the 'start_profile_scan_job' tool.
      2.  **Status Updates:** If the user asks about the status of a job and you see a job running in the System Context, inform them that it is still in progress.
      3.  **Completing Jobs:** You will be notified when a job is 'COMPLETED'. When this happens, you MUST use the 'update_bigquery_metadata' tool and then confirm to the user that the entire process is finished.
      4.  **General Chat:** If the user is just chatting, respond naturally.
    """,
    tools=[long_running_scanner_tool, bigquery_update_tool]
)


# --- Application Logic (MODIFIED) ---
APP_NAME = "profile_scanner_app"
USER_ID = "data_analyst_1"

async def setup_session_and_runner():
    session_service = InMemorySessionService()
    session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID)
    # Initialize our custom state for tracking jobs
    session.state["running_jobs"] = []
    runner = Runner(agent=scanning_agent, app_name=APP_NAME, session_service=session_service)
    return session, runner

# Helper functions to parse events (Unchanged)
def get_long_running_function_call(event: Event) -> types.FunctionCall | None:
    if not event.long_running_tool_ids or not event.content or not event.content.parts: return None
    for part in event.content.parts:
        if part.function_call and event.long_running_tool_ids and part.function_call.id in event.long_running_tool_ids:
            return part.function_call
    return None

def get_function_response(event: Event, function_call_id: str) -> types.FunctionResponse | None:
    if not event.content or not event.content.parts: return None
    for part in event.content.parts:
        if part.function_response and part.function_response.id == function_call_id:
            return part.function_response
    return None

async def poll_and_resume_agent(job_id: str, profile_name: str, initial_response: types.FunctionResponse, session: Session, runner: Runner):
    """
    This function runs in the background, waits for the job to complete,
    resumes the agent, and cleans up the session state.
    """
    print(f"\n[BACKGROUND TASK for {job_id}]: Polling for completion. This will take 10 seconds.")
    await asyncio.sleep(10)
    final_status = "COMPLETED"
    print(f"[BACKGROUND TASK for {job_id}]: Polling complete. Job has finished.")

    # Resume the agent to finalize the process
    print(f"\n[SYSTEM]: Notifying agent that job {job_id} is complete.")
    final_tool_response = initial_response.model_copy(deep=True)
    final_tool_response.response['status'] = final_status
    resume_content = types.Content(parts=[types.Part(function_response=final_tool_response)], role='user')

    async for event in runner.run_async(session_id=session.id, user_id=USER_ID, new_message=resume_content):
        if text := "".join(part.text or "" for part in event.content.parts if event.content):
            # The final confirmation message from the agent will appear here, in the main stream
            print(f"[{event.author}]: {text}")

    # Clean up the job from the session state after it's fully processed
    session.state["running_jobs"] = [job for job in session.state["running_jobs"] if job.get("id") != job_id]
    print(f"[SYSTEM]: Job {job_id} has been removed from the active job list.")


async def main():
    """Main function to run the interactive agent."""
    load_dotenv()
    print("Profile Scanning Agent")
    print("Type your request, e.g., 'scan customer_data', or 'hi' to check status.")
    print("Type 'end' to exit.")

    session, runner = await setup_session_and_runner()

    while True:
        user_input = input("\n> ")
        if user_input.lower() == 'end':
            break
        if not user_input:
            continue

        print("\n--- Agent Turn ---")

        # Add context about running jobs to the prompt for the agent
        prompt_with_context = ""
        running_jobs = session.state.get("running_jobs", [])
        if running_jobs:
            job_list_str = ", ".join([f"{job['profile_name']} (ID: {job['id']})" for job in running_jobs])
            prompt_with_context = f"System Context: The following jobs are currently running in the background: {job_list_str}\n\n"
        
        prompt_with_context += f"User: {user_input}"
        
        content = types.Content(role='user', parts=[types.Part(text=prompt_with_context)])
        
        events_async = runner.run_async(session_id=session.id, user_id=USER_ID, new_message=content)

        long_running_call, initial_response = None, None

        async for event in events_async:
            if not long_running_call: long_running_call = get_long_running_function_call(event)
            if long_running_call and not initial_response:
                initial_response = get_function_response(event, long_running_call.id)
            
            if text := "".join(part.text or "" for part in event.content.parts if event.content):
                print(f"[{event.author}]: {text}")

        # If a new job was started in this turn, store it and launch the background task
        if initial_response and initial_response.response.get("status") == "RUNNING":
            job_info = {
                "id": initial_response.response.get("job_id"),
                "profile_name": initial_response.response.get("profile_name")
            }
            if job_info["id"]:
                session.state["running_jobs"].append(job_info)
                print(f"[SYSTEM]: Job {job_info['id']} is now being tracked. Starting background poller.")
                asyncio.create_task(
                    poll_and_resume_agent(job_info["id"], job_info["profile_name"], initial_response, session, runner)
                )

    print("\nApplication finished.")
    await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())