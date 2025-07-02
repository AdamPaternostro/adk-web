import json
import requests
from requests.exceptions import HTTPError

def google_search(search_query: str): # Return type hint removed as it's now a generator
    """
    Calls Google Search to search the internet. Streams progress updates.
    Call this to get realtime information, news, events, etc.

    Args:
        search_query (str): The search string

    Yields:
        dict: Progress updates or the final search result.
              Progress: {"type": "tool_progress", "tool_name": "google_search", "message": str}
              Result:   {"type": "tool_result", "tool_name": "google_search", "data": dict}
    """

    GOOGLE_API_KEY = "AIzaSyDkUQqAszJjL9OTJIqvUgyfBmRzp2NdWGc" # This seems to be a dummy/public key
    GOOGLE_CSE_ID = "b28bd031933814c1a"

    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}&q={search_query}"

    yield {
        "type": "tool_progress",
        "tool_name": "google_search",
        "message": f"Starting Google Search for: {search_query}"
    }

    try:
        try:
            response = requests.get(url)
            yield {
                "type": "tool_progress",
                "tool_name": "google_search",
                "message": f"Request sent. Status: {response.status_code}"
            }
            response.raise_for_status()
            response_dict = json.loads(response.content)
        except HTTPError as http_err:
            error_message = f"Error when calling REST API. Status: {http_err.response.status_code}. Reason: {http_err.response.reason}. Details: {http_err}"
            yield {
                "type": "tool_progress", # Still emit as progress, then the error result
                "tool_name": "google_search",
                "message": error_message
            }
            # The ADK framework would typically expect a FunctionResponse that indicates an error.
            # This part is tricky: how does a generator tool signal a final error vs progress?
            # For now, we yield a tool_result that is an error.
            yield {
                "type": "tool_result",
                "tool_name": "google_search",
                "data": { "status": "failed", "message": error_message }
            }
            return
        except requests.exceptions.RequestException as req_err:
            error_message = f"Request error: {req_err}"
            yield {
                "type": "tool_progress",
                "tool_name": "google_search",
                "message": error_message
            }
            yield {
                "type": "tool_result",
                "tool_name": "google_search",
                "data": { "status": "failed", "message": error_message }
            }
            return

        # print(f"google_search -> response: {json.dumps(response_dict, indent=2)}") # Keep for server logs if needed

        results = []
        if "items" in response_dict:
            for item in response_dict["items"]:
                results.append({
                    "title": item["title"],
                    "snippet": item["snippet"]
                })

        yield {
            "type": "tool_progress",
            "tool_name": "google_search",
            "message": f"Processing {len(results)} search results."
        }

        final_data = { "status": "success", "search-string": search_query, "search-results": results }
        # print(f"google_search -> return_value: {json.dumps(final_data, indent=2)}") # Keep for server logs

        yield {
            "type": "tool_result",
            "tool_name": "google_search",
            "data": final_data
        }

    except Exception as e:
        error_message = f"Unexpected error in google_search: {e}"
        yield { # Try to yield progress even on unexpected error
            "type": "tool_progress",
            "tool_name": "google_search",
            "message": error_message
        }
        yield {
            "type": "tool_result",
            "tool_name": "google_search",
            "data": { "status": "failed", "message": error_message }
        }