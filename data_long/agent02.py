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
from typing import Any, Coroutine
from google.adk.agents import Agent
from google.adk.events import Event
from google.adk.runners import Runner
from google.adk.tools import LongRunningFunctionTool
from google.adk.sessions import InMemorySessionService
from google.genai import types
from dotenv import load_dotenv
import time

# 1. Define the long running function
def ask_for_approval(
    purpose: str, amount: float
) -> dict[str, Any]:
    """Ask for approval for the reimbursement."""
    # In a real application, this would create a ticket and notify the approver.
    print(f"\n[SYSTEM]: Approval required for {amount} USD for {purpose}. A ticket has been created.")
    return {'status': 'pending', 'approver': 'Sean Zhou', 'purpose' : purpose, 'amount': amount, 'ticket-id': 'approval-ticket-1'}

def reimburse(purpose: str, amount: float) -> str:
    """Reimburse the amount of money to the employee."""
    # In a real application, this would send the reimbursement request to a payment vendor.
    print(f"\n[SYSTEM]: Reimbursing {amount} USD for {purpose}.")
    return {'status': 'ok'}

# 2. Wrap the function with LongRunningFunctionTool
long_running_tool = LongRunningFunctionTool(func=ask_for_approval)


# 3. Use the tool in an Agent
file_processor_agent = Agent(
    # Use a model compatible with function calling
    model="gemini-2.5-flash",
    name='reimbursement_agent',
    instruction="""
      You are an agent whose job is to handle the reimbursement process for
      the employees. If the amount is less than $100, you will automatically
      approve the reimbursement by calling the reimburse() tool directly.

      If the amount is greater than or equal to $100, you will
      ask for approval from the manager by calling the ask_for_approval() tool.
      If the manager approves, you will then call reimburse() to reimburse the
      amount to the employee. If the manager rejects, you will inform the
e      mployee of the rejection.
    """,
    tools=[reimburse, long_running_tool]
)


APP_NAME = "human_in_the_loop"
USER_ID = "1234"
SESSION_ID = "session1234"

# Session and Runner
async def setup_session_and_runner():
    session_service = InMemorySessionService()
    # A new session is created for each call in this example.
    # For a continuous conversation, you might want to manage the session outside the call_agent_async function.
    session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID)
    runner = Runner(agent=file_processor_agent, app_name=APP_NAME, session_service=session_service)
    return session, runner


# Agent Interaction
async def call_agent_async(query: str, session, runner) -> None:

    def get_long_running_function_call(event: Event) -> types.FunctionCall | None:
        # Get the long running function call from the event
        if not event.long_running_tool_ids or not event.content or not event.content.parts:
            return None
        for part in event.content.parts:
            if (
                part.function_call
                and event.long_running_tool_ids
                and part.function_call.id in event.long_running_tool_ids
            ):
                return part.function_call
        return None

    def get_function_response(event: Event, function_call_id: str) -> types.FunctionResponse | None:
        # Get the function response for the function call with the specified id.
        if not event.content or not event.content.parts:
            return None
        for part in event.content.parts:
            if (
                part.function_response
                and part.function_response.id == function_call_id
            ):
                return part.function_response
        return None

    content = types.Content(role='user', parts=[types.Part(text=query)])

    print("\nRunning agent...")
    events_async = runner.run_async(
        session_id=session.id, user_id=USER_ID, new_message=content
    )

    long_running_function_call = None
    long_running_function_response = None

    async for event in events_async:
        if not long_running_function_call:
            long_running_function_call = get_long_running_function_call(event)
        if long_running_function_call:
             long_running_function_response = get_function_response(event, long_running_function_call.id)

        if event.content and event.content.parts:
            if text := ''.join(part.text or '' for part in event.content.parts):
                print(f'[{event.author}]: {text}')

    # This section simulates the human approval process.
    # If a long-running function was called (ask_for_approval), we now pretend it was approved.
    if long_running_function_response:
        print("\n[SYSTEM]: The manager has approved the request. Resuming agent.")
        # Create a new response with the status 'approved'
        updated_response = long_running_function_response.model_copy(deep=True)
        updated_response.response['status'] = 'approved'

        # Send this approval back to the agent to continue the process
        async for event in runner.run_async(
          session_id=session.id, user_id=USER_ID, new_message=types.Content(parts=[types.Part(function_response = updated_response)], role='user')
        ):
            if event.content and event.content.parts:
                if text := ''.join(part.text or '' for part in event.content.parts):
                    print(f'[{event.author}]: {text}')

async def main():
    """Main function to run the interactive agent loop."""
    load_dotenv()
    print("Google Agent Development Kit - Interactive Reimbursement")
    print("Enter your reimbursement request (e.g., 'Please reimburse 200$ for meals').")
    print("Type 'end' to exit the application.")

    # Setup session and runner once
    session, runner = await setup_session_and_runner()

    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() == 'end':
                print("Exiting agent...")
                break
            if not user_input:
                continue
            await call_agent_async(user_input, session, runner)

        except (KeyboardInterrupt, EOFError):
            print("\nExiting agent...")
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"An error occurred: {e}")