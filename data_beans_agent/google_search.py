import json
import requests
from requests.exceptions import HTTPError

# This function will be wrapped by LongRunningFunctionTool
def google_search(search_query: str): # No return type hint, it's a generator
    """
    Calls Google Search to search the internet. Streams progress updates
    when wrapped with LongRunningFunctionTool.

    Args:
        search_query (str): The search string

    Yields:
        dict: Progress updates or the final search result.
              Progress: {"type": "progress", "tool_name": "google_search", "message": str}
              Result:   {"type": "result", "tool_name": "google_search", "data": dict}
    """
    tool_name = "google_search" # Define once

    # It's good practice to use a more secure way to handle API keys in production
    GOOGLE_API_KEY = "AIzaSyDkUQqAszJjL9OTJIqvUgyfBmRzp2NdWGc" # Dummy/public key
    GOOGLE_CSE_ID = "b28bd031933814c1a"

    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}&q={search_query}"

    yield {
        "type": "progress",
        "tool_name": tool_name,
        "message": f"Starting Google Search for: '{search_query}'"
    }

    try:
        response = requests.get(url, timeout=10) # Added timeout
        yield {
            "type": "progress",
            "tool_name": tool_name,
            "message": f"Request sent. HTTP Status: {response.status_code}"
        }
        response.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)
        response_dict = response.json() # Use response.json()

    except HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} (Status code: {http_err.response.status_code})"
        # It's important that the 'data' field contains what a normal error response would look like
        yield {
            "type": "result",
            "tool_name": tool_name,
            "data": { "status": "failed", "message": error_message, "details": str(http_err.response.text) }
        }
        return # Stop generation
    except requests.exceptions.Timeout:
        error_message = "Request timed out."
        yield {
            "type": "result",
            "tool_name": tool_name,
            "data": { "status": "failed", "message": error_message }
        }
        return # Stop generation
    except requests.exceptions.RequestException as req_err:
        error_message = f"Request error occurred: {req_err}"
        yield {
            "type": "result",
            "tool_name": tool_name,
            "data": { "status": "failed", "message": error_message }
        }
        return # Stop generation

    # Server-side logging (optional, can be removed if too verbose)
    # print(f"google_search raw response: {json.dumps(response_dict, indent=2)}")

    results = []
    if "items" in response_dict:
        for item in response_dict["items"]:
            results.append({
                "title": item.get("title", "N/A"),
                "snippet": item.get("snippet", "N/A")
            })

    yield {
        "type": "progress",
        "tool_name": tool_name,
        "message": f"Processing {len(results)} search results."
    }

    final_data = { "status": "success", "search-string": search_query, "search-results": results }
    # print(f"google_search final data: {json.dumps(final_data, indent=2)}")

    yield {
        "type": "result",
        "tool_name": tool_name,
        "data": final_data
    }
