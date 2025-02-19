import os
import json
import pandas as pd
import streamlit as st
from openai import OpenAI
from io import BytesIO
from utils.data_processor import DataProcessor
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize OpenAI client
client = OpenAI()
MODEL = 'o1-preview'

# Function to generate rules_config.json dynamically

def generate_rules_config(baseline_df, candidate_df):
    messages = [
        {
            "role": "user",
            "content": f"""
You are an AI trained to generate validation rules for trading data discrepancies. Based on the following baseline and candidate datasets, define a structured validation configuration:

BASELINE DATA SAMPLE:
{baseline_df.head().to_json(indent=2)}

CANDIDATE DATA SAMPLE:
{candidate_df.head().to_json(indent=2)}

- Identify key fields for validation (e.g., price, quantity, trade type, timestamps, order book ID).
- Define acceptable thresholds and tolerances for discrepancies.
- Specify rules for flagging missing or unexpected records.
- Ensure compliance with the Nasdaq Direct Drop protocol, including:
  - Order status transition validation
  - Bid-ask spread consistency
  - Settlement date verification
  - Circuit breaker & trading halt checks
  - Short-sell trade validation
  - Order execution sequence integrity

**Return a JSON object with a list of rules, where each rule contains:**
- "Rule Number": Unique ID for the rule.
- "type": Type of validation (e.g., "range_check", "missing_value", "status_transition").
- "columns": List of columns to validate.
- "threshold": Acceptable variation threshold.
- "severity": Severity level ("warning", "critical").
"""
        }
    ]
    
    response = client.chat.completions.create(
        model=MODEL,
        messages=messages
    )
    
    response_content = response.choices[0].message.content.replace('```json', '').replace('```', '').strip()
    
    try:
        rules_config = json.loads(response_content)
        with open('rules_config.json', 'w') as f:
            json.dump(rules_config, f, indent=4)
        return rules_config
    except json.JSONDecodeError as e:
        print(f"Failed to generate rules: {response_content}")
        return {"rules": []}

# Streamlit UI for uploading trade files
st.set_page_config(page_title="AI Discrepancy Finder", layout="wide")
st.title("Upload Trade Files for AI Discrepancy Detection")

uploaded_baseline = st.file_uploader("Upload Baseline Trade File", type=["txt", "csv", "log"], key="baseline")
uploaded_candidate = st.file_uploader("Upload Candidate Trade File", type=["txt", "csv", "log"], key="candidate")

if uploaded_baseline and uploaded_candidate:
    df_baseline = pd.read_csv(uploaded_baseline, delimiter="|")
    df_candidate = pd.read_csv(uploaded_candidate, delimiter="|")
    
    # Generate rules dynamically
    rules_config = generate_rules_config(df_baseline, df_candidate)
    
    # Function to validate discrepancies using AI based on generated rules
    def validate_discrepancy(row):
        messages = [
            {
                "role": "user",
                "content": f"""
You are an AI trained to identify discrepancies in financial trading data. Analyze the following trade entry and determine if there are any inconsistencies:

TRADE DATA:
{row}

Rules for Validation:
{json.dumps(rules_config, indent=2)}

- Validate order status transitions follow proper sequence.
- Ensure bid-ask spread consistency using `nationalBidPriceSnapshot` and `nationalOfferPriceSnapshot`.
- Verify that `settlementDate` aligns with exchange trading schedules.
- Check for trades executed during circuit breakers or trading halts.
- Validate short-sell trades are correctly classified (`shortSellQuantity`).
- Detect missing data in essential fields (`orderBookId`, `tradePrice`, `quantity`).

**Return a JSON response with:**
- "is_valid": Boolean (true if valid, false if a discrepancy is found)
- "issue": Explanation if a discrepancy is found; otherwise, null.
"""
            }
        ]
        
        response = client.chat.completions.create(
            model=MODEL,
            messages=messages
        )
        
        response_content = response.choices[0].message.content.replace('```json', '').replace('```', '').strip()
        
        try:
            return json.loads(response_content)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response: {response_content}")
            return {"is_valid": False, "issue": "Failed to process AI validation."}
    
    # AI-Based Discrepancy Detection with Multi-threading
    results = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(validate_discrepancy, row): row for row in df_candidate.to_dict(orient='records')}
        for future in as_completed(futures):
            results.append(future.result())
    
    results_df = pd.DataFrame(results)
    st.write("AI-Detected Discrepancies:")
    st.dataframe(results_df)
