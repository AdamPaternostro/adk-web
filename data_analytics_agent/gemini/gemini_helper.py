import os
import json
import logging
import requests
import google.auth
import google.auth.transport.requests
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log, retry_if_exception

def retry_condition(error):
  error_string = str(error)
  print(error_string)

  retry_errors = [
      "RESOURCE_EXHAUSTED",
      "No content in candidate",
      # Add more error messages here as needed
  ]

  for retry_error in retry_errors:
    if retry_error in error_string:
      print("Retrying...")
      return True
  return False

@retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(10), retry=retry_if_exception(retry_condition), before_sleep=before_sleep_log(logging.getLogger(), logging.INFO))
def gemini_llm(prompt, model="gemini-2.5-flash", response_schema=None, temperature=1, topP=1, topK=32):
    """
    Calls the Gemini LLM with a given prompt and handles the response.
    """
    llm_response = None
    if temperature < 0:
        temperature = 0

    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    location = os.getenv("AGENT_ENV_VERTEX_AI_REGION")

    url = f"https://{location}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{location}/publishers/google/models/{model}:generateContent"

    generation_config = {
        "temperature": temperature,
        "topP": topP,
        "maxOutputTokens": 8192,
        "candidateCount": 1,
        "responseMimeType": "application/json",
    }

    if response_schema is not None:
        generation_config["responseSchema"] = response_schema
    if "flash" in model: # topK is supported by flash models
        generation_config["topK"] = topK

    request_body = {
        "contents": {"role": "user", "parts": {"text": prompt}},
        "generation_config": {**generation_config},
        "safety_settings": {
            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "threshold": "BLOCK_LOW_AND_ABOVE"
        }
    }

    creds, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + creds.token
    }

    response = requests.post(url, json=request_body, headers=headers)

    if response.status_code == 200:
        try:
            json_response = response.json()
            llm_response = json_response["candidates"][0]["content"]["parts"][0]["text"]
            # Clean up common markdown formatting if present
            return llm_response.strip().lstrip("```json").rstrip("```")
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Error parsing LLM response: {e}. Full response: {response.text}")
    else:
        raise RuntimeError(f"Error with prompt:'{prompt}'  Status:'{response.status_code}' Text:'{response.text}'")
    


def llm_as_a_judge(original_prompt: str, response_from_processing: str) -> bool:
    """
    Determines if the prompt has run correctly
    """
    response_schema = {
        "type": "object",
        "properties": {"processing_status": {"type": "boolean"}},
        "required": ["processing_status"]
    }

    prompt = f"""Respond back with True if you think the below process request executed successfully or False if it looks like it failed.

    Processing Request: {original_prompt}

    Response from Processing: {response_from_processing}
    """
    gemini_response = gemini_llm(prompt, response_schema=response_schema)
    gemini_response_json = json.loads(gemini_response)
    return bool(gemini_response_json["processing_status"])