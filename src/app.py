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

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Get the absolute path of the current script's directory
base_dir = os.path.dirname(os.path.abspath(__file__))
cache_dir = os.path.join(base_dir, "utils", "__pycache__")

# Delete cache directory if exists
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)
    print(f"Deleted {cache_dir}")

# **✅ Ensure `st.set_page_config()` is first**
st.set_page_config(page_title="Discrepancy Dashboard", layout="wide")
st.markdown(
    """
    <style>
    [data-testid="stSidebarNav"] {
        background-image: url(https://www.mabl.com/hs-fs/hubfs/logo-coforge.png?width=900&name=logo-coforge.png);
        background-repeat: no-repeat;
        padding-top: 120px;
        background-position: 20px 20px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

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
if "results" not in st.session_state:
    st.session_state["results"] = pd.DataFrame()

# ✅ Ensure session state variables persist
if "df_baseline" not in st.session_state:
    st.session_state["df_baseline"] = pd.DataFrame()  # Initialize as empty DataFrame
if "df_candidate" not in st.session_state:
    st.session_state["df_candidate"] = pd.DataFrame()  # Initialize as empty DataFrame


# ✅ Define required config files before using them
required_files = {
    "directory_config.json": "directory_config_path",
    "job_creation_response.json": "job_response_path",
    "rules_config.json": "rules_config_path"
}

def apply_filter():
    """Re-runs comparison with updated filters and updates UI components dynamically."""
    if "df_baseline" not in st.session_state or "df_candidate" not in st.session_state:
        st.warning("Baseline or candidate dataset is missing. Please upload files first.")
        return

    if st.session_state["df_baseline"].empty or st.session_state["df_candidate"].empty:
        st.warning("Baseline or candidate dataset is empty. Please upload valid files.")
        return

    try:
        df_baseline = st.session_state["df_baseline"].copy()
        df_candidate = st.session_state["df_candidate"].copy()
        selected_filters = st.session_state.get("selected_filters", {})

        # ✅ Re-run comparison using updated filters
        processor = DataProcessor(
            st.session_state["directory_config_path"],
            st.session_state["job_response_path"],
            st.session_state["rules_config_path"]
        )

        results = processor.compare_files(df_baseline, df_candidate, st.session_state["file_type"], selected_filters)
        st.success("✅ Filters Applied Successfully! Report Updated.")

        st.session_state["results"] = results
        st.session_state["filtered_results"] = results

        # ✅ Update UI Components
        get_key_performance()
        st.rerun()
    except Exception as e:
        st.error(f"❌ Error applying filters: {str(e)}")

def get_key_performance():
    """Updates KPI Metrics, Bar Chart, Pie Chart dynamically after applying filters."""

    # ✅ Pull filtered results from session state
    filtered_results = st.session_state.get("filtered_results", pd.DataFrame())

    # ✅ Handle cases where filtering removes all discrepancies
    if filtered_results.empty:
        st.warning("⚠️ No discrepancies found after applying filters.")
        return

    st.header("📊 Key Performance Indicators")

    # ✅ Ensure consistent category formatting
    filtered_results["category"] = filtered_results["category"].astype(str).str.upper()

    # ✅ Count each unique category dynamically
    category_counts = filtered_results["category"].value_counts().to_dict()

    # ✅ Identify Missing Rows
    missing_baseline_count = (filtered_results["Rule Type"] == "Missing in Baseline").sum()
    missing_candidate_count = (filtered_results["Rule Type"] == "Missing in Candidate").sum()

    # ✅ Total metrics to display
    total_metrics = len(category_counts) + 3  # Dynamic categories + threshold + missing rows

    # ✅ Create correct number of columns dynamically
    kpi_columns = st.columns(min(total_metrics, 4))  # Limit to 4 columns for layout readability

    # ✅ Display each KPI dynamically
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
    unique_categories = filtered_results["category"].unique()

    # ✅ Dynamically generate colors for categories
    color_palette = plotly.colors.qualitative.Set1
    color_map = {category: color_palette[i % len(color_palette)] for i, category in enumerate(unique_categories)}

    # ✅ Count discrepancies per column and category
    discrepancy_counts = filtered_results.groupby(["Column Name", "category"]).size().reset_index(name="Count")

    # ✅ Bar Chart: Count of Discrepancies by Column
    st.header("📊 Discrepancy Analysis")
    if discrepancy_counts.empty:
        st.warning("📉 No discrepancies available to plot.")
    else:
        fig = px.bar(
            discrepancy_counts,
            x="Column Name",
            y="Count",
            color="category",
            title="Discrepancies by Column",
            barmode="group",
            color_discrete_map=color_map
        )
        st.plotly_chart(fig, use_container_width=True)

    # ✅ Pie Chart: Category Distribution
    st.header("Discrepancy Distribution")
    pie_chart = px.pie(filtered_results, names="category", title="Proportion of Discrepancy Types", hole=0.4)
    st.plotly_chart(pie_chart, use_container_width=True)

    # ✅ Filtered Data Table Based on Selected Column
    st.header("Discrepancy Details")
    st.dataframe(filtered_results)
    st.rerun()


def file_upload():
    global f, e
    for file_name, key in missing_files.items():
        uploaded_file = st.file_uploader(f"Upload `{file_name}`", type=["json"], key=key)
        if uploaded_file is not None:
            save_path = os.path.join(temp_dir, file_name)
            try:
                with open(save_path, "wb") as f:
                    shutil.copyfileobj(uploaded_file, f)
                uploaded_files[key] = save_path
                st.success(f"`{file_name}` uploaded successfully!")
            except Exception as e:
                st.error(f"❌ Error saving `{file_name}`: {e}")

# ✅ Helper function to load rules configuration at runtime
def load_rules_config():
    rules_config_path = st.session_state.get("rules_config_path")
    if rules_config_path and os.path.exists(rules_config_path):
        try:
            with open(rules_config_path, "r") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading rules configuration: {e}")
    return {"rules": {}}

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
    file_upload()

    if len(uploaded_files) == len(missing_files):
        st.session_state["directory_config_path"] = uploaded_files.get("directory_config.json", st.session_state.get("directory_config_path"))
        st.session_state["job_response_path"] = uploaded_files.get("job_creation_response.json", st.session_state.get("job_response_path"))
        st.session_state["rules_config_path"] = uploaded_files.get("rules_config.json", st.session_state.get("rules_config_path"))
        st.success("✅ Configuration files uploaded successfully! Click 'Next' to proceed.")
        if st.button("Next"):
            st.session_state["screen"] = "file_type_selection"
            st.rerun()
    st.stop()

# ✅ Cleanup function: Delete temp files & folder
def cleanup_temp_files():
    try:
        for file_path in uploaded_files.values():
            if os.path.exists(file_path):
                os.remove(file_path)
        shutil.rmtree(temp_dir)
        st.success("Temporary files cleaned up successfully!")
    except Exception as e:
        st.error(f"❌ Error during cleanup: {e}")

if st.button("🗑️ Cleanup Temporary Files"):
    cleanup_temp_files()

# ✅ Step 2: Select File Type
if st.session_state["screen"] == "file_type_selection":
    st.title("Select File Type for Comparison")
    file_type = st.radio("Choose the file type:", ["DD (.txt)", "Excel (.xlsx)", "Flat Files (.txt, .csv, .json, .log)"])
    file_type_mapping = {
        "Excel (.xlsx)": "Excel",
        "DD (.txt)": "DD",
        "Flat Files (.txt, .csv, .json, .log)": "Text"
    }
    if st.button("Next", key="next_button_file_type"):
        st.session_state["file_type"] = file_type_mapping[file_type]
        st.session_state["screen"] = "file_selection"
        st.rerun()
    st.stop()

# ✅ Step 3: Upload Files for Comparison
if st.session_state["screen"] == "file_selection":
    st.title("Upload Files for Comparison")
    uploaded_file_baseline = st.file_uploader("Upload Baseline File", type=["xlsx", "txt", "csv", "json", "log"], key="baseline_file")
    uploaded_file_candidate = st.file_uploader("Upload Candidate File", type=["xlsx", "txt", "csv", "json", "log"], key="candidate_file")

    if st.button("Run Comparison", key="run_comparison_button") and (uploaded_file_baseline and uploaded_file_candidate):
        try:
            processor = DataProcessor(
                st.session_state["directory_config_path"],
                st.session_state["job_response_path"],
                st.session_state["rules_config_path"]
            )
            file_type = st.session_state["file_type"]

            def detect_file_type(file):
                filename = file.name.lower()
                if (filename.endswith(".xlsx") or filename.endswith(".xls")) and file_type == "Excel":
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
            st.session_state["file_type"] = file_type

            baseline_bytes = BytesIO(uploaded_file_baseline.getvalue())
            candidate_bytes = BytesIO(uploaded_file_candidate.getvalue())
            baseline_bytes.seek(0)
            candidate_bytes.seek(0)

            df_baseline = processor.read_file(baseline_bytes, file_type)
            df_candidate = processor.read_file(candidate_bytes, file_type)
            st.write("✅ Uploaded files successfully converted to DataFrames.")
            if df_baseline.empty or df_candidate.empty:
                st.error("One of the uploaded files is empty. Please check your data.")
                st.stop()

            results = processor.compare_files(df_baseline, df_candidate, file_type)
            st.success("✅ Comparison Completed! Discrepancy report generated.")
            st.session_state["results"] = results
            st.session_state["filtered_results"] = results

        except Exception as e:
            st.error(f"Error processing files: {str(e)}")

# Reload the rules configuration each time the sidebar is rendered
rules_config = load_rules_config()

# ✅ Sidebar: Dynamic Filters
st.sidebar.header("🔍 Filter Rules")
selected_filters = st.session_state.get("selected_filters", {})

def generate_unique_key(column_name, rule_number, rule_type, rule_id, index):
    hash_string = f"{column_name}_{rule_number}_{rule_type}_{rule_id}_{index}_{uuid.uuid4().hex}"
    return hashlib.md5(hash_string.encode()).hexdigest()[:8] + f"_{rule_id}_{index}"

def convert_to_numeric(value):
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str) and value.replace(".", "").isdigit():
        return float(value) if "." in value else int(value)
    return None

def get_rules_sidebar():
    """Generates a Streamlit sidebar with dynamically populated rule filters for constraints."""
    selected_filters = st.session_state.get("selected_filters", {})

    for column_name, rules in rules_config.get("rules", {}).items():
        for index, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue

            rule_number = rule.get("Rule Number", "Unknown Rule")
            rule_type = rule.get("type", "Unknown Type")
            category = rule.get("category", "General")

            st.sidebar.subheader(f"{column_name} - {category} ({rule_type})")

            if column_name not in selected_filters:
                selected_filters[column_name] = {}
            if rule_number not in selected_filters[column_name]:
                selected_filters[column_name][rule_number] = {}

            min_constraint = rule.get("constraints", {}).get("min", None)
            max_constraint = rule.get("constraints", {}).get("max", None)

            default_min = selected_filters[column_name][rule_number].get("min", min_constraint)
            default_max = selected_filters[column_name][rule_number].get("max", max_constraint)

            min_key = f"{column_name}_{rule_number}_{index}_min"
            max_key = f"{column_name}_{rule_number}_{index}_max"

            new_min_value = st.sidebar.number_input(
                f"Min Value for {column_name}", value=default_min, key=min_key
            )
            new_max_value = st.sidebar.number_input(
                f"Max Value for {column_name}", value=default_max, key=max_key
            )

            selected_filters[column_name][rule_number]["min"] = new_min_value
            selected_filters[column_name][rule_number]["max"] = new_max_value

    st.session_state["selected_filters"] = selected_filters


get_rules_sidebar()
st.session_state["selected_filters"] = selected_filters


# ✅ Buttons
apply_filter_clicked = st.sidebar.button("📌 Apply Filter", key="apply_filter_button")
if reset_filter_clicked := st.sidebar.button(
    "♻️ Reset Filters", key="reset_filter_button"
):
    st.session_state["selected_filters"] = {}
    st.session_state["filtered_results"] = st.session_state.get("results", pd.DataFrame())
    get_key_performance()
    st.rerun()


# ✅ Apply Filters
if apply_filter_clicked and "results" in st.session_state:
    try:
        apply_filter()

    except Exception as e:
        st.error(f"Error applying filters: {str(e)}")


# **📤 Export Button**
filtered_results = st.session_state.get("filtered_results", pd.DataFrame())
job_response_path = st.session_state["job_response_path"]
export_format = "CSV"

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

def get_file_name():
    baseline_env = job_response["baseline"]["env"]
    candidate_env = job_response["candidate"]["env"]
    baseline_label = job_response["baseline"]["label"]
    candidate_label = job_response["candidate"]["label"]
    filename = f"discrepancy_report_{baseline_env}_{candidate_env}_{baseline_label}_{candidate_label}"
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

if not filtered_results.empty and job_response:
    get_file_name()

# ✅ Display Results and Key Performance Indicators
filtered_results = st.session_state.get("filtered_results", pd.DataFrame())


def get_key_performance():
    global count
    st.header("📊 Key Performance Indicators")
    # ✅ Ensure consistent capitalization in category column
    filtered_results["category"] = filtered_results["category"].str.upper()
    # ✅ Count each unique category dynamically
    category_counts = filtered_results["category"].value_counts().to_dict()
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
    unique_categories = filtered_results["category"].unique()
    # ✅ Generate distinct colors dynamically using Plotly's color palette
    color_palette = plotly.colors.qualitative.Set1  # Choose a color set
    color_map = {category: color_palette[i % len(color_palette)] for i, category in enumerate(unique_categories)}
    # ✅ Count discrepancies per column and category
    discrepancy_counts = filtered_results.groupby(["Column Name", "category"]).size().reset_index(name="Count")
    # ✅ Bar Chart: Count of Discrepancies by Column
    st.header("📊 Discrepancy Analysis")
    fig = px.bar(
        discrepancy_counts,
        x="Column Name",
        y="Count",
        color="category",
        title="Discrepancies by Column",
        barmode="group",
        color_discrete_map=color_map  # ✅ Now dynamically generated
    )
    st.plotly_chart(fig, use_container_width=True)
    # ✅ **Pie Chart: category Distribution**
    st.header("Discrepancy Distribution")
    pie_chart = px.pie(filtered_results, names="category", title="Proportion of Discrepancy Types", hole=0.4)
    st.plotly_chart(pie_chart, use_container_width=True)
    # ✅ **Filtered Data Table Based on Selected Column**
    st.header("Discrepancy Details")
    st.dataframe(filtered_results)


if not filtered_results.empty:
    get_key_performance()