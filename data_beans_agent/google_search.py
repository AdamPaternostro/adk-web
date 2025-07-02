import json
import requests
from requests.exceptions import HTTPError, Timeout

def google_search(search_query: str) -> dict:
    """
    Calls Google Search to search the internet.
    Returns a dictionary containing progress updates and the final result.
    This version is NOT a generator.

    Args:
        search_query (str): The search string

    Returns:
        dict: {
                  "progress_updates": list[dict],
                  "final_tool_result": dict
              }
    """
    tool_name = "google_search"
    progress_updates = []

    # It's good practice to use a more secure way to handle API keys in production
    GOOGLE_API_KEY = "AIzaSyDkUQqAszJjL9OTJIqvUgyfBmRzp2NdWGc" # Dummy/public key (ensure this is intended)
    GOOGLE_CSE_ID = "b28bd031933814c1a"
    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}&q={search_query}"

    progress_updates.append({
        "type": "progress", # Retaining type for potential frontend consistency
        "tool_name": tool_name,
        "message": f"Starting Google Search for: '{search_query}'"
    })

    final_tool_result = None

    try:
        response = requests.get(url, timeout=10)
        progress_updates.append({
            "type": "progress",
            "tool_name": tool_name,
            "message": f"Request sent. HTTP Status: {response.status_code}"
        })
        response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
        response_dict = response.json()

        results = []
        if "items" in response_dict:
            for item in response_dict.get("items", []): # Ensure items is a list
                results.append({
                    "title": item.get("title", "N/A"),
                    "snippet": item.get("snippet", "N/A")
                })

        progress_updates.append({
            "type": "progress",
            "tool_name": tool_name,
            "message": f"Processing {len(results)} search results."
        })

        final_tool_result = { "status": "success", "search-string": search_query, "search-results": results }

    except Timeout:
        error_message = "Request to Google Search API timed out."
        progress_updates.append({"type": "progress", "tool_name": tool_name, "message": error_message})
        final_tool_result = { "status": "failed", "message": error_message }
    except HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} (Status code: {http_err.response.status_code})"
        progress_updates.append({"type": "progress", "tool_name": tool_name, "message": error_message})
        try:
            details = str(http_err.response.text)
        except Exception:
            details = "Could not retrieve error details from response."
        final_tool_result = { "status": "failed", "message": error_message, "details": details }
    except requests.exceptions.RequestException as req_err:
        error_message = f"Request error occurred: {req_err}"
        progress_updates.append({"type": "progress", "tool_name": tool_name, "message": error_message})
        final_tool_result = { "status": "failed", "message": error_message }
    except Exception as e: # Catch any other unexpected error
        error_message = f"An unexpected error occurred in google_search: {str(e)}"
        progress_updates.append({"type": "progress", "tool_name": tool_name, "message": error_message})
        final_tool_result = { "status": "failed", "message": error_message }


    return {
        "progress_updates": progress_updates,
        "final_tool_result": final_tool_result
    }
