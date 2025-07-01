import json
import requests
from requests.exceptions import HTTPError

def google_search(search_query: str) -> dict:
    """
    Calls Google Search to search the internet.  Call this to get realtime information, news, events, etc.

    Args:
        search_query (str): The search string

    Returns:
        dict: This will return: 
{
    "status": "success",
    "search-string": "google",
    "search-results": [
                    {
                        "title": "Google - Wikipedia",
                        "snippet": "Google was founded on September 4, 1998, by American computer scientists Larry Page and Sergey Brin. Together, they own about 14% of its publicly listed shares\u00a0...",
                    },
                    {
                        "title": "Create a Gmail account - Gmail Help",
                        "snippet": "Reserved by Google to prevent spam or abuse. Someone is impersonating me. If you believe someone has created a Gmail address to try to impersonate your identity\u00a0...",
                    }
                ]
            }  
    """

    GOOGLE_API_KEY = "AIzaSyDkUQqAszJjL9OTJIqvUgyfBmRzp2NdWGc"
    GOOGLE_CSE_ID = "b28bd031933814c1a"

    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}&q={search_query}"

    try:
        try:
            response = requests.get(url)
            response.raise_for_status()
            response_dict = json.loads(response.content)
        except HTTPError as http_err:
            return { "status": "failed", "message": "Error when calling rest api with status code of: {http_err.response.status_code}, HTTP error occurred: {http_err}, eason: {http_err.response.reason}" }      
            #if http_err.response.text:
            #    print(f"Server error message: {http_err.response.text}")
        except requests.exceptions.RequestException as req_err:
            return { "status": "failed", "message": "Error when calling rest api with Other request error occurred: {req_err}" }      
    
        print(f"google_search -> response: {json.dumps(response_dict, indent=2)}")

        # We only want a subset of information
        results = []
        if "items" in response_dict:
            for item in response_dict["items"]:
                results.append({
                    "title": item["title"],
                    "snippet": item["snippet"]
                })

        return_value =  { "status": "success", "search-string": search_query, "search-results": results }
        print(f"google_search -> return_value: {json.dumps(return_value, indent=2)}")

        return return_value            

    except Exception as e:
        return { "status": "failed", "message": "Error when calling google_search: {e}" }