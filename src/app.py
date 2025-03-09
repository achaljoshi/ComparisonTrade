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
# ✅ Sidebar: Dynamic Filters
st.sidebar.header("🔍 Filter Rules")

# ✅ Ensure `rules_config` exists in session
rules_config = st.session_state.get("rules_config", {})

# ✅ Retrieve existing selected_filters or initialize a new structure
selected_filters = st.session_state.get("selected_filters", {})

# ✅ Process Normal Rules
if "rules" in rules_config:
    for column_name, rules in rules_config["rules"].items():
        for index, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue

            rule_number = rule.get("rulenumber", "Unknown Rule")
            rule_type = rule.get("type", "Unknown Type")
            category = rule.get("category", "General")
            constraints = rule.get("constraints", {})
            format_type = rule.get("format_type", "Simple")

            # ✅ Ensure the column_name entry exists in selected_filters
            if column_name not in selected_filters:
                selected_filters[column_name] = {}

            # ✅ Ensure the rule_number entry exists in selected_filters
            if rule_number not in selected_filters[column_name]:
                selected_filters[column_name][rule_number] = {}

            # ✅ Get previous values if they exist, otherwise use defaults
            min_constraint = constraints.get("min", 0.0)
            max_constraint = constraints.get("max", 100.0)

            default_min = selected_filters[column_name][rule_number].get("min", min_constraint)
            default_max = selected_filters[column_name][rule_number].get("max", max_constraint)

            min_key = f"{column_name}_{rule_number}_{index}_min"
            max_key = f"{column_name}_{rule_number}_{index}_max"

            # ✅ Sidebar UI for constraints (min/max)
            st.sidebar.subheader(f"{column_name} - {category} ({rule_type})")

            new_min_value = st.sidebar.number_input(
                f"Min Value for {column_name} ({rule_number})", value=default_min, key=min_key
            )
            new_max_value = st.sidebar.number_input(
                f"Max Value for {column_name} ({rule_number})", value=default_max, key=max_key
            )

            # ✅ Store updated values in selected_filters
            selected_filters[column_name][rule_number] = {
                "min": new_min_value,
                "max": new_max_value,
                "format_type": format_type,
                "rule_type": rule_type,
            }

# ✅ Process Tick Sizes Separately
if "tickSizes" in rules_config:
    if "tick_sizes" not in selected_filters:
        selected_filters["tick_sizes"] = {}

    for rule in rules_config["tickSizes"]:
        rule_number = rule.get("rulenumber", "Unknown Rule")
        rule_type = rule.get("type", "Unknown Type")
        rule_columns = rule.get("columns", [])
        constraints = rule.get("constraints", {})
        format_type = rule.get("format_type", "Array")
        tick_size = constraints.get("tickSize", None)

        # ✅ Use default value if tick_size exists
        default_tick_size = selected_filters["tick_sizes"].get(rule_number, {}).get("tickSize", tick_size)

        tick_size_key = f"tick_size_{rule_number}"

        # ✅ Sidebar UI for Tick Size
        st.sidebar.subheader(f"📏 Tick Size ({rule_number})")
        new_tick_size = st.sidebar.number_input(f"Tick Size ({rule_number})", value=default_tick_size, key=tick_size_key)

        # ✅ Store updated Tick Size constraints
        selected_filters["tick_sizes"][rule_number] = {
            "tickSize": new_tick_size,
            "format_type": format_type,
            "rule_type": rule_type,
            "columns": rule_columns
        }

# ✅ Store constraints and tick sizes in session state
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
    processor = DataProcessor(
        st.session_state["directory_config_path"],
        st.session_state["job_response_path"],
        st.session_state["rules_config_path"]
    )

    # ✅ Get file type
    file_type = st.session_state["file_type"]
    uploaded_file_baseline = st.session_state["uploaded_file_baseline"]
    uploaded_file_candidate = st.session_state["uploaded_file_candidate"]

    # ✅ Convert uploaded files to `BytesIO`
    baseline_bytes = BytesIO(uploaded_file_baseline.getvalue())
    candidate_bytes = BytesIO(uploaded_file_candidate.getvalue())

    # ✅ Reset file pointer before reading (important for Streamlit uploads)
    baseline_bytes.seek(0)
    candidate_bytes.seek(0)

    # ✅ Read files using `read_file()` from DataProcessor
    df_baseline = processor.read_file(baseline_bytes, file_type)
    df_candidate = processor.read_file(candidate_bytes, file_type)
    # ✅ Apply comparison with filters
    updated_results = processor.compare_files(
        df_baseline, df_candidate, file_type, st.session_state.get("selected_filters", {})
    )

    # ✅ Store results in session state
    st.session_state["results"] = updated_results
    st.session_state["filtered_results"] = updated_results

    # ✅ Refresh UI
    st.rerun()

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