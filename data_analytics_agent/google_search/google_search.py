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
        dict:
        {
            "status": "success",
            "tool_name": "search_query",
            "query": "The google search query used",
            "messages": ["List of messages during processing"]
            "results": [ {"title": "N/A", "snippet": "N/A"} ] 
        }
    """
    import os

    GOOGLE_API_KEY = os.getenv("AGENT_GOOGLE_API_KEY")
    GOOGLE_CSE_ID = os.getenv("AGENT_GOOGLE_CSE_ID")
    messages = []

    # https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}&q={search_query}&num=10"

    return_value = None

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

        messages.append(f"Call the url: {url}")
        return_value = { "status": "success", "tool_name": "google_search", "query": search_query, "messages": messages, "results": results }

    except Timeout:
        messages.append("Request to Google Search API timed out.")
        return_value = { "status": "failed", "tool_name": "google_search", "query": search_query, "messages": messages, "results": None }
    except HTTPError as http_err:
        messages.append(f"HTTP error occurred: {http_err} (Status code: {http_err.response.status_code})")
        try:
            details = str(http_err.response.text)
        except Exception:
            details = "Could not retrieve error details from response."
        return_value = { "status": "failed", "tool_name": "google_search", "query": search_query, "messages": messages, "results": None }
    except requests.exceptions.RequestException as req_err:
        messages.append(f"Request error occurred: {req_err}")
        return_value = { "status": "failed", "tool_name": "google_search", "query": search_query, "messages": messages, "results": None }
    except Exception as e: # Catch any other unexpected error
        messages.append(f"An unexpected error occurred in google_search: {str(e)}")
        return_value = { "status": "failed", "tool_name": "google_search", "query": search_query, "messages": messages, "results": None }

    return return_value