from openai import OpenAI
import json
from IPython.display import display, HTML
from sklearn.metrics import precision_score, recall_score, f1_score
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import pandas as pd

client = OpenAI()
MODEL = 'o1-preview'

def validate_data(input_data):
    messages = [
        {
            "role": "user",
            "content": f"""
You are a helpful assistant designed to validate the quality of medical datasets. You will be given a single row of medical data, and your task is to determine whether the data is valid.

- Carefully analyze the data for any inconsistencies, contradictions, missing values, or implausible information.
- Consider the logical relationships between different fields (e.g., treatments should be appropriate for the diagnoses, medications should not conflict with allergies, lab results should be consistent with diagnoses, etc.).
- Use your general medical knowledge to assess the validity of the data.
- Focus solely on the information provided without making assumptions beyond the given data.

**Return only a JSON object** with the following two properties:

- `"is_valid"`: a boolean (`true` or `false`) indicating whether the data is valid.
- `"issue"`: if `"is_valid"` is `false`, provide a brief explanation of the issue; if `"is_valid"` is `true`, set `"issue"` to `null`.

Both JSON properties must always be present.

Do not include any additional text or explanations outside the JSON object.

MEDICAL DATA:
{input_data}
            """
        }
    ]

    response = client.chat.completions.create(
        model=MODEL,
        messages=messages
    )

    response_content = response.choices[0].message.content.replace('```json', '').replace('```', '').strip()
    
    try:
        if isinstance(response_content, dict):
            response_dict = response_content
        else:
            response_dict = json.loads(response_content)
        return response_dict
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON response: {response_content}")
        raise e
    
# Read the CSV file and exclude the last two columns
input_data = []
with open('../data/medicalData.csv', 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)
    for row in reader:
        input_data.append(row[:-2])  # Exclude "Is Valid" and "Issue" columns

# Initialize lists to store true labels
true_is_valid = []
true_issues = []

# Extract true labels from the CSV file
with open('../data/medicalData.csv', 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)
    for row in reader:
        true_is_valid.append(row[-2] == 'True')
        true_issues.append(row[-1])

# Function to validate a single row of data
def validate_row(row):
    input_str = ','.join(row)
    result_json = validate_data(input_str)
    return result_json

# Validate data rows and collect results
pred_is_valid = [False] * len(input_data)
pred_issues = [''] * len(input_data)

with ThreadPoolExecutor() as executor:
    futures = {executor.submit(validate_row, row): i for i, row in enumerate(input_data)}
    
    for future in as_completed(futures):
        i = futures[future]  # Get the index of the current row
        result_json = future.result()
        pred_is_valid[i] = result_json['is_valid']
        pred_issues[i] = result_json['issue']


# Convert predicted and true 'is_valid' labels to boolean if they aren't already
pred_is_valid_bool = [bool(val) if isinstance(val, bool) else val == 'True' for val in pred_is_valid]
true_is_valid_bool = [bool(val) if isinstance(val, bool) else val == 'True' for val in true_is_valid]

# Calculate precision, recall, and f1 score for the 'is_valid' prediction
precision = precision_score(true_is_valid_bool, pred_is_valid_bool, pos_label=True)
recall = recall_score(true_is_valid_bool, pred_is_valid_bool, pos_label=True)
f1 = f1_score(true_is_valid_bool, pred_is_valid_bool, pos_label=True)

# Initialize issue_matches_full with False
issue_matches_full = [False] * len(true_is_valid)

print(f"Precision: {precision:.2f}")
print(f"Recall: {recall:.2f}")
print(f"F1: {f1:.2f}")

def validate_issue(model_generated_answer, correct_answer):
    messages = [
        {
            "role": "user",
            "content": f"""
You are a medical expert assistant designed to validate the quality of an LLM-generated answer.

The model was asked to review a medical dataset row to determine if the data is valid. If the data is not valid, it should provide a justification explaining why.

Your task:

    •	Compare the model-generated justification with the correct reason provided.
    •	Determine if they address the same underlying medical issue or concern, even if phrased differently.
    •	Focus on the intent, medical concepts, and implications rather than exact wording.

Instructions:

    •	If the justifications have the same intent or address the same medical issue, return True.
    •	If they address different issues or concerns, return False.
    •	Only respond with a single word: True or False.

Examples:

    1.	Example 1:
    •	Model Generated Response: “The patient is allergic to penicillin”
    •	Correct Response: “The patient was prescribed penicillin despite being allergic”
    •	Answer: True
    2.	Example 2:
    •	Model Generated Response: “The date of birth of the patient is incorrect”
    •	Correct Response: “The patient was prescribed penicillin despite being allergic”
    •	Answer: False


Model Generated Response: {model_generated_answer}
Correct Response:  {correct_answer}
            """
        }
    ]

    response = client.chat.completions.create(
        model="o1-preview",
        messages=messages
    )

    result = response.choices[0].message.content

    return result

# Validate issues for rows where both true and predicted 'is_valid' are False
validation_results = []

with ThreadPoolExecutor() as executor:
    futures = {
        executor.submit(validate_issue, pred_issues[i], true_issues[i]): i
        for i in range(len(pred_is_valid_bool))
        if not pred_is_valid_bool[i] and not true_is_valid_bool[i]
    }
    
    for future in as_completed(futures):
        i = futures[future]  # Get the original index
        issue_match = future.result()
        issue_matches_full[i] = (issue_match == 'True')
        validation_results.append({
            "index": i,
            "predicted_issue": pred_issues[i],
            "true_issue": true_issues[i],
            "issue_match": issue_matches_full[i]
        })
    
    # Calculate issue accuracy
    issue_accuracy = sum([i['issue_match'] for i in validation_results]) / len(validation_results)
    
    # Store the results in the dictionary
    model_results = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "issue_accuracy": issue_accuracy
    }

# Create a DataFrame to store the results
df_results = pd.DataFrame([model_results])

# Create a DataFrame to store the validation results for each row
df_validation_results = pd.DataFrame(validation_results)

def display_formatted_dataframe(df):
    def format_text(text):
        return text.replace('\n', '<br>')

    df_formatted = df.copy()
    df_formatted['predicted_issue'] = df_formatted['predicted_issue'].apply(format_text)
    df_formatted['true_issue'] = df_formatted['true_issue'].apply(format_text)
    
    display(HTML(df_formatted.to_html(escape=False, justify='left')))
    
display_formatted_dataframe(pd.DataFrame(validation_results))