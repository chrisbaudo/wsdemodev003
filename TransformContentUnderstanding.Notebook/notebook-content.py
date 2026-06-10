# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4de3fc5a-80c5-4780-833e-37be3ccb4354",
# META       "default_lakehouse_name": "ExtractFormsBronze",
# META       "default_lakehouse_workspace_id": "d2a440f4-6d00-4354-9f77-a2fcae66e547",
# META       "known_lakehouses": [
# META         {
# META           "id": "4de3fc5a-80c5-4780-833e-37be3ccb4354"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Run Content Understanding Analyzer and Poll for Results


# CELL ********************

from notebookutils import credentials
import requests
import time
import pandas as pd

# Auth
token = credentials.getToken("https://cognitiveservices.azure.com")

# API details
endpoint = "https://admin-rg-2159-resource-6636.services.ai.azure.com/"
analyzer_id = "student_college_applications"
api_version = "2025-11-01"

url = f"{endpoint}/contentunderstanding/analyzers/{analyzer_id}:analyze?api-version={api_version}"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

payload = {
    "inputs": [
        {
            "url": "https://stcontentunderstanding.blob.core.windows.net/content/college_application_sample.pdf"
        }
    ],
    "config": {
        "omitContent": True, # Reduces payload to mainly fields
        "returnDetails": False
    }

}

# Submit job
response = requests.post(url, headers=headers, json=payload)

operation_url = response.headers["Operation-Location"]

# Poll
while True:
    poll = requests.get(operation_url, headers=headers)
    result = poll.json()

    status = result.get("status")
    print(status)

    if status in ["Succeeded", "Failed"]:
        break

    time.sleep(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Display Results in Pandas Data Frames

# CELL ********************

def extract_value(field_obj):
    """
    Pull the most useful scalar/list value out of a CU field object.
    """
    if not isinstance(field_obj, dict):
        return field_obj

    # Common CU value shapes
    for key in [
        "valueString",
        "valueNumber",
        "valueInteger",
        "valueBoolean",
        "valueDate",
        "valueDateTime",
        "valueTime",
        "valuePhoneNumber",
        "valueSelectionMark",
        "valueCurrency",
        "valueArray",
        "valueObject"
    ]:
        if key in field_obj:
            return field_obj[key]

    # Fallback: sometimes nested under generic keys
    if "value" in field_obj:
        return field_obj["value"]

    return None

def flatten_parent(result_json):
    contents = result_json.get("result", {}).get("contents", [])
    rows = []

    for i, content in enumerate(contents):
        row = {"content_index": i}

        fields = content.get("fields", {})
        for field_name, field_obj in fields.items():
            row[field_name] = extract_value(field_obj)

            # optional confidence
            if isinstance(field_obj, dict) and "confidence" in field_obj:
                row[f"{field_name}_confidence"] = field_obj["confidence"]

        rows.append(row)

    return pd.DataFrame(rows)

def flatten_array(result_json, field_name):
    contents = result_json.get("result", {}).get("contents", [])
    rows = []

    for i, content in enumerate(contents):
        fields = content.get("fields", {})
        arr = fields.get(field_name, {})
        items = arr.get("valueArray", [])

        for j, item in enumerate(items):
            row = {
                "content_index": i,
                "item_index": j
            }

            obj = item.get("valueObject", {}) if isinstance(item, dict) else {}
            for subfield, subval in obj.items():
                row[subfield] = extract_value(subval)

                if isinstance(subval, dict) and "confidence" in subval:
                    row[f"{subfield}_confidence"] = subval["confidence"]

            rows.append(row)

    return pd.DataFrame(rows)

# Usage
df = flatten_parent(result)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write Results To Default Lakehouse

# CELL ********************

# Parent table
spark.createDataFrame(df) \
    .write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("StudentCollegeApplications")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
