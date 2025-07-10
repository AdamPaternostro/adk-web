def rest_api_helper(url: str, http_verb: str, request_body: str) -> dict:
  """Calls the Google Cloud REST API passing in the current users credentials"""

  import google.auth.transport.requests
  import requests
  import google.auth
  import json

  # Get an access token based upon the current user
  creds, project = google.auth.default()
  auth_req = google.auth.transport.requests.Request()
  creds.refresh(auth_req)
  access_token=creds.token

  headers = {
    "Content-Type" : "application/json",
    "Authorization" : "Bearer " + access_token
  }

  if http_verb == "GET":
    response = requests.get(url, headers=headers)
  elif http_verb == "POST":
    response = requests.post(url, json=request_body, headers=headers)
  elif http_verb == "PUT":
    response = requests.put(url, json=request_body, headers=headers)
  elif http_verb == "PATCH":
    response = requests.patch(url, json=request_body, headers=headers)
  elif http_verb == "DELETE":
    response = requests.delete(url, headers=headers)
  else:
    raise RuntimeError(f"Unknown HTTP verb: {http_verb}")

  if response.status_code == 200:
      return json.loads(response.content)
  else:
    error = f"Error rest_api_helper -> ' Status: '{response.status_code}' Text: '{response.text}'"
    raise RuntimeError(error)
  