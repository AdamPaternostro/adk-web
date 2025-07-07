import json
import requests
from requests.exceptions import HTTPError, Timeout

def google_search(search_query: str) -> dict:
    """
    Calls Google Search to search the internet.
    Use this for up to date information on news, weather, etc.
    You can also use this to supplement your knowledge and query help documents on subjects.

    Args:
        search_query (str): The search string

    Returns:
        dict: { "status": "success", "search-string": "search_query", "search-results": [ {"title": "N/A", "snippet": "N/A"}] }
    """
    import os
    GOOGLE_API_KEY = os.getenv("AGENT_GOOGLE_API_KEY")
    GOOGLE_CSE_ID = os.getenv("AGENT_GOOGLE_CSE_ID")

    # https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}&q={search_query}&num=10"

    final_tool_result = None

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
        response_dict = response.json()

        results = []
        if "items" in response_dict:
            for item in response_dict.get("items", []): # Ensure items is a list
                results.append({
                    "title": item.get("title", "N/A"),
                    "snippet": item.get("snippet", "N/A")
                })

        final_tool_result = { "status": "success", "search-string": search_query, "search-results": results }

    except Timeout:
        error_message = "Request to Google Search API timed out."
        final_tool_result = { "status": "failed", "message": error_message }
    except HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} (Status code: {http_err.response.status_code})"
        try:
            details = str(http_err.response.text)
        except Exception:
            details = "Could not retrieve error details from response."
        final_tool_result = { "status": "failed", "message": error_message, "details": details }
    except requests.exceptions.RequestException as req_err:
        error_message = f"Request error occurred: {req_err}"
        final_tool_result = { "status": "failed", "message": error_message }
    except Exception as e: # Catch any other unexpected error
        error_message = f"An unexpected error occurred in google_search: {str(e)}"
        final_tool_result = { "status": "failed", "message": error_message }

    return final_tool_result