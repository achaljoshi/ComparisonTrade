import streamlit as st
import os
import pandas as pd
import plotly.express as px
import json
from io import BytesIO
from utils.data_processor import DataProcessor
import plotly.colors
import shutil
import tempfile
from pathlib import Path
import logging
import hashlib
import uuid
import re

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


# Get the absolute path of the current script's directory
base_dir = os.path.dirname(os.path.abspath(__file__))
cache_dir = os.path.join(base_dir, "utils", "__pycache__")

# Check if the directory exists before attempting to delete it
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)
    print(f"Deleted {cache_dir}")

# **✅ Ensure `st.set_page_config()` is first**
st.set_page_config(page_title="Discrepancy Dashboard", layout="wide")

# ✅ Clear Cache & Restart Button
if st.sidebar.button("🔄 Clear Cache & Restart", key="restart_button"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

# ✅ Ensure session state variables persist
if "screen" not in st.session_state:
    st.session_state["screen"] = "upload_config"
if "directory_config_path" not in st.session_state:
    st.session_state["directory_config_path"] = None
if "job_response_path" not in st.session_state:
    st.session_state["job_response_path"] = None
if "rules_config_path" not in st.session_state:
    st.session_state["rules_config_path"] = None
if "selected_filters" not in st.session_state:
    st.session_state["selected_filters"] = {}
if "filtered_results" not in st.session_state:
    st.session_state["filtered_results"] = pd.DataFrame()

# ✅ Define required config files before using them
required_files = {
    "directory_config.json": "directory_config_path",
    "job_creation_response.json": "job_response_path",
    "rules_config.json": "rules_config_path"
}

def extract_numeric(value):
    if isinstance(value, str):
        match = re.search(r"\b\d+\b", value)  # Extract first number
        return float(match.group()) if match else None
    return value


# ✅ Step 1: Upload Configuration Files
if st.session_state["screen"] == "upload_config":
    st.title("Upload Configuration Files")

    missing_files = {}
    if not st.session_state.get("directory_config_path"):
        missing_files["directory_config.json"] = "directory_config.json"
    if not st.session_state.get("job_response_path"):
        missing_files["job_creation_response.json"] = "job_creation_response.json"
    if not st.session_state.get("rules_config_path"):
        missing_files["rules_config.json"] = "rules_config.json"

    uploaded_files = {}

    temp_dir = tempfile.mkdtemp()
    def file_upload():
        global f, e
        for file_name, key in missing_files.items():
            uploaded_file = st.file_uploader(f"Upload `{file_name}`", type=["json"], key=key)

            if uploaded_file is not None:
                save_path = os.path.join(temp_dir, file_name)  # ✅ Save in a unique temp directory

                try:
                    # ✅ Ensure safe file writing (avoids permission errors)
                    with open(save_path, "wb") as f:
                        shutil.copyfileobj(uploaded_file, f)  # ✅ Efficient way to copy file content

                    uploaded_files[key] = save_path  # ✅ Store path in dictionary
                    st.success(f"`{file_name}` uploaded successfully!")

                except Exception as e:
                    st.error(f"❌ Error saving `{file_name}`: {e}")

    file_upload()

    # ✅ Debugging: Display uploaded file paths (for verification)
    # st.write("Uploaded file paths:", uploaded_files)

    # ✅ Update session state only if all files are uploaded
    if len(uploaded_files) == len(missing_files):
        st.session_state["directory_config_path"] = uploaded_files.get("directory_config.json", st.session_state.get("directory_config_path"))
        st.session_state["job_response_path"] = uploaded_files.get("job_creation_response.json", st.session_state.get("job_response_path"))
        st.session_state["rules_config_path"] = uploaded_files.get("rules_config.json", st.session_state.get("rules_config_path"))

        st.success("✅ Configuration files uploaded successfully! Click 'Next' to proceed.")
        
        if st.button("Next"):
            st.session_state["screen"] = "file_type_selection"
            st.rerun()  # ✅ Refresh UI to move to the next step

    st.stop()

# ✅ Cleanup function: Delete temp files & folder
if st.button("🗑️ Cleanup Temporary Files"):
    def cleanup_temp_files():
        try:
            for file_path in uploaded_files.values():
                if os.path.exists(file_path):
                    os.remove(file_path)  # ✅ Delete individual temp files
            
            shutil.rmtree(temp_dir)  # ✅ Remove the entire temp directory
            st.success("Temporary files cleaned up successfully!")

        except Exception as e:
            st.error(f"❌ Error during cleanup: {e}")
    cleanup_temp_files()

# ✅ Step 2: Select File Type
if st.session_state["screen"] == "file_type_selection":
    st.title("Select File Type for Comparison")

    # File type selection from UI
    file_type = st.radio("Choose the file type:", ["DD (.txt)", "Excel (.xlsx)",  "Flat Files (.txt, .csv, .json, .log)"])

    # Mapping UI file type selection to simplified values
    file_type_mapping = {
        "Excel (.xlsx)": "Excel",
        "DD (.txt)": "DD",
        "Flat Files (.txt, .csv, .json, .log)": "Text"
    }

    if st.button("Next", key="next_button_file_type"):
        # Store the mapped file type for consistent internal processing
        st.session_state["file_type"] = file_type_mapping[file_type]
        st.session_state["screen"] = "file_selection"
        st.rerun()

    st.stop()

# ✅ Step 3: Upload Files for Comparison
if st.session_state["screen"] == "file_selection":
    st.title("Upload Files for Comparison")

    uploaded_file_baseline = st.file_uploader(
        "Upload Baseline File", type=["xlsx", "txt", "csv", "json", "log"], key="baseline_file"
    )
    uploaded_file_candidate = st.file_uploader(
        "Upload Candidate File", type=["xlsx", "txt", "csv", "json", "log"], key="candidate_file"
    )

    if st.button("Run Comparison", key="run_comparison_button") and (uploaded_file_baseline and uploaded_file_candidate):
        try:
            # ✅ Initialize Data Processor
            processor = DataProcessor(
                st.session_state["directory_config_path"],
                st.session_state["job_response_path"],
                st.session_state["rules_config_path"]
            )
    
            file_type = st.session_state["file_type"]
            def detect_file_type(file):
                filename = file.name.lower()
                if filename.endswith(".xlsx") or filename.endswith(".xls") and file_type == "Excel":
                    return "Excel"
                elif filename.endswith(".txt") and file_type == "DD":
                    return "DD"
                elif filename.endswith(".txt") and file_type == "Text":
                    return "TEXT"
                elif filename.endswith(".csv") and file_type == "Text":
                    return "CSV"
                elif filename.endswith(".json") and file_type == "Text":
                    return "JSON"
                elif filename.endswith(".log") and file_type == "Text":
                    return "LOG"
                else:
                    raise ValueError("Unsupported file format.")
    
            file_type = detect_file_type(uploaded_file_baseline)
            st.session_state["file_type"] = file_type  # ✅ Ensure file type is set in session
    
            # ✅ Convert uploaded files to `BytesIO`
            baseline_bytes = BytesIO(uploaded_file_baseline.getvalue())
            candidate_bytes = BytesIO(uploaded_file_candidate.getvalue())
    
            # ✅ Reset file pointer before reading (important for Streamlit uploads)
            baseline_bytes.seek(0)
            candidate_bytes.seek(0)
    
            # ✅ Read files using `read_file()` from DataProcessor
            df_baseline = processor.read_file(baseline_bytes, file_type)
            df_candidate = processor.read_file(candidate_bytes, file_type)
    
            # ✅ Debugging: Display sample data
            st.write("✅ Uploaded files successfully converted to DataFrames.")
            print("✅ Baseline DataFrame:\n", df_baseline.head())
            print("✅ Candidate DataFrame:\n", df_candidate.head())
    
            # ✅ Ensure files are not empty
            if df_baseline.empty or df_candidate.empty:
                st.error("One of the uploaded files is empty. Please check your data.")
                st.stop()
    
            # ✅ Run Comparison
            results = processor.compare_files(df_baseline, df_candidate, file_type)
            st.success("✅Apply Comparison Completed! Discrepancy report generated.")
    
            # ✅ Store results in session state
            st.session_state["results"] = results
            st.session_state["filtered_results"] = results
            st.session_state["uploaded_file_baseline"] = uploaded_file_baseline
            st.session_state["uploaded_file_candidate"] = uploaded_file_candidate
    
        except Exception as e:
            st.error(f"Error processing files: {str(e)}")

# ✅ Load `rules_config.json`
rules_config_path = st.session_state.get("rules_config_path", None)

if rules_config_path and os.path.exists(rules_config_path):
    with open(rules_config_path, "r") as f:
        rules_config = json.load(f)
        st.session_state["rules_config"] = rules_config  # ✅ Store in session state
        print("\n✅ [DEBUG] Reloaded `rules_config.json` from disk\n")
else:
    print("🚨 [ERROR] `rules_config.json` file not found or path is incorrect!")

# ✅ Sidebar: Dynamic Filters
st.sidebar.header("🔍 Filter Rules")
selected_filters = st.session_state.get("selected_filters", {})

for category, rules in rules_config.get("rules", {}).items():
    for rule in rules:
        rule_number = rule.get("rulenumber", "Unknown Rule")
        rule_type = rule.get("type", "Unknown Type")
        rule_columns = ", ".join(rule.get("columns", []))
        rule_description = rule.get("description", "No Description")
        constraints = rule.get("constraints", {})
        sub_columns = rule.get("sub_columns", [])

        rule_key_prefix = f"{category}_{rule_number}"
        st.sidebar.subheader(f"⚖️ {rule_number} ({rule_type})")
        st.sidebar.write(f"📝 Columns: {rule_columns}")
        st.sidebar.write(f"📌 Description: {rule_description}")

        selected_filters.setdefault(rule_key_prefix, {})

        for key, value in rule.items():
            if key == "constraints" and isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    input_key = f"{rule_key_prefix}_{key}_{sub_key}"

                    # ✅ Load previous value from session state if available
                    prev_value = st.session_state.get(input_key, sub_value)

                    # ✅ Number input for dynamic constraints update
                    new_value = st.sidebar.number_input(
                        f"{sub_key.capitalize()} for {rule_number}", value=prev_value, key=input_key
                    )

                    # ✅ Update session state immediately
                    # ✅ Update only if the value is different
                    if input_key not in st.session_state:
                        st.session_state[input_key] = new_value
                    elif st.session_state[input_key] != new_value:
                        st.session_state[input_key] = new_value

                    # ✅ Ensure the updated values are stored in `rules_config`
                    rule["constraints"][sub_key] = new_value

            elif isinstance(value, (int, float)):
                input_key = f"{rule_key_prefix}_{key}"
                prev_value = selected_filters[rule_key_prefix].get(input_key, value)
                new_value = st.sidebar.number_input(
                    f"{key.capitalize()} for {rule_number}", value=prev_value, key=input_key
                )
                selected_filters[rule_key_prefix][input_key] = new_value

# Store selected filters in session state
st.session_state["selected_filters"] = selected_filters


# ✅ Buttons
apply_filter_clicked = st.sidebar.button("📌 Apply Filter", key="apply_filter_button")
reset_filter_clicked = st.sidebar.button("♻️ Reset Filters", key="reset_filter_button")

# ✅ Reset Filters
if reset_filter_clicked:
    st.session_state["selected_filters"] = {}
    st.session_state["filtered_results"] = st.session_state.get("results", pd.DataFrame())
    st.rerun()

# ✅ Apply Filters
if apply_filter_clicked and "results" in st.session_state:

    print("\n🔍 [DEBUG] Apply Filter Clicked")

    # ✅ Get existing discrepancy data
    df = st.session_state.get("results", pd.DataFrame()).copy()

    if not df.empty:
        print(f"🔹 [DEBUG] Initial DataFrame Rows: {len(df)}")

        # ✅ Ensure numeric conversion for comparison
        def extract_numeric(value):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        df["Baseline Field Value Numeric"] = df["Baseline Field Value"].apply(extract_numeric)
        df["Candidate Field Value Numeric"] = df["Candidate Field Value"].apply(extract_numeric)

        # ✅ Compute absolute difference
        df["Difference"] = abs(df["Baseline Field Value Numeric"] - df["Candidate Field Value Numeric"])
        print("\n🔹 [DEBUG] Computed Difference Column Added\n",
              df[["Baseline Field Value Numeric", "Candidate Field Value Numeric", "Difference"]].head())

        # ✅ Load `rules_config` from session state
        rules_config = st.session_state.get("rules_config", {})

        # ✅ Ensure `rules_config` has the required structure
        if "rules" in rules_config:
            for category, rules in rules_config["rules"].items():
                for rule in rules:
                    rule_number = rule.get("rulenumber", "Unknown Rule")
                    constraints = rule.get("constraints", {})
                    rule_columns = rule.get("columns", [])
                    format_type = rule.get("format_type", "Simple")

                    min_constraint = constraints.get("min", None)
                    max_constraint = constraints.get("max", None)

                    print(f"\n🔍 [DEBUG] Processing Rule: {rule_number} ({format_type})")
                    print(f"🔹 Columns: {rule_columns}")
                    print(f"🔹 Old Min: {constraints.get('min', None)}, New Min: {min_constraint}")
                    print(f"🔹 Old Max: {constraints.get('max', None)}, New Max: {max_constraint}")

                    # ✅ Apply filtering based on format type
                    for col in rule_columns:
                        if format_type == "Array":
                            # ✅ Ensure column name pattern matching for arrays like `tickSizes[0].lowerLimit`
                            df = df[~((df["Column Name"].str.contains(col, regex=False)) & (df["Difference"] < min_constraint))]
                            df = df[~((df["Column Name"].str.contains(col, regex=False)) & (df["Difference"] > max_constraint))]
                        else:
                            if min_constraint is not None:
                                df = df[~((df["Column Name"] == col) & (df["Difference"] < min_constraint))]
                            if max_constraint is not None:
                                df = df[~((df["Column Name"] == col) & (df["Difference"] > max_constraint))]

        # ✅ Store updated filtered results
        st.session_state["filtered_results"] = df

        print("\n✅ [DEBUG] Updated Filtered Data:")
        print(df.head())
        print(f"\n✅ [DEBUG] Final Filtered Row Count: {len(df)}")

        # ✅ Refresh UI
        st.rerun()


# ✅ Display Filtered Results



# **📤 Export Button**
filtered_results = st.session_state.get("filtered_results", pd.DataFrame())

if not filtered_results.empty:
    st.header("📊 Updated Discrepancy Analysis")
    st.dataframe(filtered_results)
job_response_path = st.session_state["job_response_path"]
export_format = "CSV"  # Default file format

if job_response_path and os.path.exists(job_response_path):
    try:
        with open(job_response_path, "r") as f:
            job_response = json.load(f)
            export_format = job_response.get("report", {}).get("format", "CSV").upper()
    except Exception as e:
        st.error(f"Error loading job response file: {str(e)}")
        job_response = None
else:
    job_response = None

if not filtered_results.empty and job_response:
    # ✅ Generate Filename Using Job Response Data
    baseline_env = job_response["baseline"]["env"]
    candidate_env = job_response["candidate"]["env"]
    baseline_label = job_response["baseline"]["label"]
    candidate_label = job_response["candidate"]["label"]

    filename = f"discrepancy_report_{baseline_env}_{candidate_env}_{baseline_label}_{candidate_label}"

    # ✅ Export Data Based on Format
    export_data = None
    mime_type = "text/plain"

    if export_format == "CSV":
        export_data = filtered_results.to_csv(index=False).encode("utf-8")
        filename += ".csv"
        mime_type = "text/csv"

    elif export_format == "EXCEL":
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            filtered_results.to_excel(writer, index=False, sheet_name="Discrepancies")
        export_data = output.getvalue()
        filename += ".xlsx"
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    elif export_format == "JSON":
        export_data = filtered_results.to_json(orient="records", indent=4).encode("utf-8")
        filename += ".json"
        mime_type = "application/json"

    elif export_format == "TEXT":
        export_data = filtered_results.to_string(index=False).encode("utf-8")
        filename += ".txt"
        mime_type = "text/plain"

    if export_data:
        st.download_button(
            label="📥 Download Report",
            data=export_data,
            file_name=filename,
            mime=mime_type
        )
    else:
        st.warning("Unsupported export format. Defaulting to CSV.")

# ✅ Display Results
filtered_results = st.session_state.get("filtered_results", pd.DataFrame())
if not filtered_results.empty:
    st.header("📊 Key Performance Indicators")

    # ✅ Ensure consistent capitalization in Category column
    filtered_results["Classification"] = filtered_results["Classification"].str.upper()

    # ✅ Count each unique category dynamically
    category_counts = filtered_results["Classification"].value_counts().to_dict()


    # ✅ Identify Missing Rows
    missing_baseline_count = (filtered_results["Rule Type"] == "Missing in Baseline").sum()
    missing_candidate_count = (filtered_results["Rule Type"] == "Missing in Candidate").sum()

    # ✅ Total metrics to display
    total_metrics = len(category_counts) + 3  # Dynamic categories + threshold + missing rows

    # ✅ Create correct number of columns
    kpi_columns = st.columns(min(total_metrics, 4))  # Limit to 4 columns for layout readability

    # ✅ Display each category dynamically
    i = 0
    kpi_columns[i % len(kpi_columns)].metric("🔍 Total Discrepancies", len(filtered_results))
    i += 1
    for category, count in category_counts.items():
        kpi_columns[i % len(kpi_columns)].metric(f"{category}", count)
        i += 1

    kpi_columns[i % len(kpi_columns)].metric("Missing Rows in Baseline", missing_baseline_count)
    i += 1
    kpi_columns[i % len(kpi_columns)].metric("Missing Rows in Candidate", missing_candidate_count)

    # ✅ Extract unique categories dynamically
    unique_categories = filtered_results["Classification"].unique()
    # ✅ Generate distinct colors dynamically using Plotly's color palette
    color_palette = plotly.colors.qualitative.Set1  # Choose a color set
    color_map = {category: color_palette[i % len(color_palette)] for i, category in enumerate(unique_categories)}
    # ✅ Count discrepancies per column and category
    discrepancy_counts = filtered_results.groupby(["Column Name", "Classification"]).size().reset_index(name="Count")
    # ✅ Bar Chart: Count of Discrepancies by Column
    st.header("📊 Discrepancy Analysis")
    fig = px.bar(
        discrepancy_counts,
        x="Column Name",
        y="Count",
        color="Classification",
        title="Discrepancies by Column",
        barmode="group",
        color_discrete_map=color_map  # ✅ Now dynamically generated
    )
    st.plotly_chart(fig, use_container_width=True)


    # ✅ **Pie Chart: Category Distribution**
    st.header("Discrepancy Distribution")
    pie_chart = px.pie(filtered_results, names="Classification", title="Proportion of Discrepancy Types", hole=0.4)
    st.plotly_chart(pie_chart, use_container_width=True)

    # ✅ **Filtered Data Table Based on Selected Column**
    st.header("Discrepancy Details")
    st.dataframe(filtered_results)