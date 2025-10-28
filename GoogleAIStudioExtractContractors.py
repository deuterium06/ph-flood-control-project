import pandas as pd
from openai import OpenAI
import re
from datetime import datetime
import os
import time
import json

import google.generativeai as genai

# Replace with your actual API key
genai.configure(api_key="INSERT YOUR API KEY HERE")

def get_contractor_owner(contractor_name):
    """Ask GPT to find public info about a contractor."""
    try:

        prompt = f"""
        You are a factual data assistant.

        For each construction company or contractor below, find the official owner, CEO, or company head (if publicly available in the Philippines).
        You may look into Department of Public Works and Highway source. If the information is not publicly verifiable, respond with "Not found".

        Return the results as a valid JSON array like this:
        [
        {{"Contractor": "...", "Owner": "..."}},
        ...
        ]

        Contractors:
        {json.dumps(contractor_name, indent=2)}
        """
        model = genai.GenerativeModel('gemini-2.5-pro')
        response = model.generate_content(prompt)

        # Parse output
        text_output = response.candidates[0].content.parts[0].text
        clean = re.sub(r"```(?:json)?|```", "", text_output).strip()
        return clean

    except Exception as e:
        print(f"Error fetching owner for {contractor_name}: {e}")
        return "Error"

# Example: process your contractor table
df = pd.read_csv("flood-control-data_2025-09-28_contractors.csv")

distinct_contractors = df[['Contractor']].drop_duplicates() #.head(len(df)//2)

all_dataframes = []
batch_size = 50
for i in range(0, len(distinct_contractors), batch_size):
    batch = distinct_contractors[i:i+batch_size].to_dict(orient="records")
    print("printing batch")
    
    try:
        owner_data = json.loads(get_contractor_owner(batch))
        print(owner_data)
    except json.JSONDecodeError:
        print("Output not in valid JSON format.")
        owner_data = []
    
    df = pd.DataFrame(owner_data)
    
    all_dataframes.append(df)

    time.sleep(3)

final_combined_df = pd.concat(all_dataframes, ignore_index=True)

final_combined_df.to_csv("projects_contractors_with_owners.csv", index=False)
print("Contractor owner lookup complete.")