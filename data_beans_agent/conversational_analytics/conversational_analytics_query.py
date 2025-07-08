def chart_tool() -> dict:
    """Only should be called when the user types 'show me a chart'.
    Show the user the "human_message" from the returned result.
    """
    ca_result =    {
        "systemMessage": {
            "chart": {
                "result": {
                    "vegaConfig": 
                   {
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Count per Borough by Year",
  "width": 500,
  "height": 300,
  "data": {
    "values": [
      { "borough": "Bronx", "year": 2024, "count": 126556 },
      { "borough": "Bronx", "year": 2020, "count": 147557 },
      { "borough": "Unknown", "year": 2024, "count": 146202 },
      { "borough": "EWR", "year": 2019, "count": 8654 },
      { "borough": "Manhattan", "year": 2025, "count": 6117195 },
      { "borough": "Bronx", "year": 2021, "count": 131030 },
      { "borough": "Staten Island", "year": 2021, "count": 3712 },
      { "borough": "Manhattan", "year": 2022, "count": 35281422 },
      { "borough": "Unknown", "year": 2021, "count": 370327 },
      { "borough": "Bronx", "year": 2025, "count": 34956 },
      { "borough": "Manhattan", "year": 2021, "count": 27926919 },
      { "borough": "Brooklyn", "year": 2022, "count": 291169 },
      { "borough": "Queens", "year": 2021, "count": 2105346 },
      { "borough": "Queens", "year": 2025, "count": 573771 },
      { "borough": "Brooklyn", "year": 2025, "count": 158358 },
      { "borough": "Queens", "year": 2022, "count": 3453254 },
      { "borough": "Staten Island", "year": 2024, "count": 1831 },
      { "borough": "Unknown", "year": 2019, "count": 834135 },
      { "borough": "Bronx", "year": 2023, "count": 65999 },
      { "borough": "Bronx", "year": 2019, "count": 177860 },
      { "borough": "Unknown", "year": 2022, "count": 564241 },
      { "borough": "Manhattan", "year": 2019, "count": 76606337 },
      { "borough": "Brooklyn", "year": 2021, "count": 363633 },
      { "borough": "Unknown", "year": 2023, "count": 387213 },
      { "borough": "Manhattan", "year": 2024, "count": 36402042 },
      { "borough": "EWR", "year": 2021, "count": 3015 },
      { "borough": "Unknown", "year": 2020, "count": 224579 },
      { "borough": "Unknown", "year": 2025, "count": 17798 },
      { "borough": "EWR", "year": 2024, "count": 5940 },
      { "borough": "Brooklyn", "year": 2019, "count": 1051888 },
      { "borough": "Brooklyn", "year": 2024, "count": 600018 },
      { "borough": "Brooklyn", "year": 2020, "count": 395930 },
      { "borough": "EWR", "year": 2023, "count": 5105 },
      { "borough": "Brooklyn", "year": 2023, "count": 281567 },
      { "borough": "Queens", "year": 2020, "count": 1411819 },
      { "borough": "Queens", "year": 2019, "count": 5914413 },
      { "borough": "Manhattan", "year": 2020, "count": 22463643 },
      { "borough": "EWR", "year": 2022, "count": 10125 },
      { "borough": "Manhattan", "year": 2023, "count": 33806178 },
      { "borough": "EWR", "year": 2025, "count": 676 },
      { "borough": "Staten Island", "year": 2025, "count": 545 },
      { "borough": "Staten Island", "year": 2022, "count": 3233 },
      { "borough": "Queens", "year": 2024, "count": 3887102 },
      { "borough": "Queens", "year": 2023, "count": 3761525 },
      { "borough": "Staten Island", "year": 2023, "count": 2551 },
      { "borough": "Staten Island", "year": 2019, "count": 3846 },
      { "borough": "Staten Island", "year": 2020, "count": 3614 },
      { "borough": "Bronx", "year": 2022, "count": 52178 },
      { "borough": "EWR", "year": 2020, "count": 2114 }
    ]
  },
  "mark": {
    "type": "bar",
    "tooltip": True
  },
  "encoding": {
    "x": {
      "field": "borough",
      "type": "nominal",
      "title": "Borough",
      "axis": { "labelAngle": -45 },
      "sort": { "op": "sum", "field": "count", "order": "descending" }
    },
    "y": {
      "field": "count",
      "type": "quantitative",
      "title": "Count"
    },
    "xOffset": {
      "field": "year",
      "type": "nominal",
      "sort": "ascending"
    },
    "color": {
      "field": "year",
      "type": "nominal",
      "title": "Year"
    }
  }
} 
                    }
                }
            }
        }
    

    import uuid

    return { "status": "success", "chartId" : f"{uuid.uuid4()}",  "ca_result": ca_result , "human_message" : "Here is a bar graph showing the number of bad people by borough and year."}