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
import logging

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
def cleanup_temp_files():
    try:
        for file_path in uploaded_files.values():
            if os.path.exists(file_path):
                os.remove(file_path)  # ✅ Delete individual temp files
        
        shutil.rmtree(temp_dir)  # ✅ Remove the entire temp directory
        st.success("Temporary files cleaned up successfully!")

    except Exception as e:
        st.error(f"❌ Error during cleanup: {e}")

if st.button("🗑️ Cleanup Temporary Files"):
    cleanup_temp_files()

# ✅ Step 2: Select File Type
if st.session_state["screen"] == "file_type_selection":
    st.title("Select File Type for Comparison")

    # File type selection from UI
    file_type = st.radio("Choose the file type:", ["Excel (.xlsx)", "DD (.log, .csv)", "Text Files (.txt)"])

    # Mapping UI file type selection to simplified values
    file_type_mapping = {
        "Excel (.xlsx)": "Excel",
        "DD (.log, .csv)": "DD",
        "Text Files (.txt)": "Text"
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

    uploaded_file_baseline = st.file_uploader("Upload Baseline File", type=["xlsx", "log", "txt", "csv"], key="baseline_file")
    uploaded_file_candidate = st.file_uploader("Upload Candidate File", type=["xlsx", "log", "txt", "csv"], key="candidate_file")

    if st.button("Run Comparison", key="run_comparison_button"):
        if uploaded_file_baseline and uploaded_file_candidate:
            try:
                processor = DataProcessor(
                    st.session_state["directory_config_path"],
                    st.session_state["job_response_path"],
                    st.session_state["rules_config_path"]
                )
                file_type = st.session_state["file_type"]
                if file_type == "Excel":
                    df_baseline = pd.read_excel(uploaded_file_baseline, engine="openpyxl")
                    df_candidate = pd.read_excel(uploaded_file_candidate, engine="openpyxl")
                elif file_type in ["Text", "DD"]:
                    delimiter = processor.rules_config.get("text_file_delimiter", ",")
                    header = 0 if processor.rules_config.get("text_file_contains_header", "yes").lower() == "yes" else None
                    df_baseline = pd.read_csv(uploaded_file_baseline, delimiter=delimiter, header=header)
                    df_candidate = pd.read_csv(uploaded_file_candidate, delimiter=delimiter, header=header)
                else:
                    st.error("Unsupported file type selected.")
                    st.stop()

                # ✅ Run Comparison
                st.write("✅ Uploaded files are successfully read as DataFrames.")
                if df_baseline.empty or df_candidate.empty:
                    st.error("One of the uploaded files is empty. Please check your data.")
                    st.stop()
                results = processor.compare_files(df_baseline, df_candidate, st.session_state["file_type"])
                st.success("Comparison Completed! Discrepancy report generated.")

                # ✅ Store results in session state
                st.session_state["results"] = results
                st.session_state["filtered_results"] = results
                st.session_state["uploaded_file_baseline"] = uploaded_file_baseline
                st.session_state["uploaded_file_candidate"] = uploaded_file_candidate

            except Exception as e:
                st.error(f"Error processing files: {str(e)}")

# ✅ Load `rules_config.json`
rules_config_path = st.session_state.get("rules_config_path")

if rules_config_path and os.path.exists(rules_config_path):
    with open(rules_config_path, "r") as f:
        rules_config = json.load(f)
else:
    rules_config = {"rules": []}

# ✅ Sidebar: Dynamic Filters
st.sidebar.header("🔍 Filter Rules")
selected_filters = st.session_state["selected_filters"]

for rule in rules_config.get("rules", []):
    if not isinstance(rule, dict):
        continue  # Skip invalid rules

    rule_number = rule.get("Rule Number", "Unknown Rule")
    rule_type = rule.get("type", "Unknown Type")
    rule_columns = ", ".join(rule.get("columns", [])) if rule.get("columns") else "N/A"

    st.sidebar.subheader(f"⚖️ {rule_number} ({rule_type})")
    st.sidebar.write(f"📝 Columns: {rule_columns}")
    
    selected_filters.setdefault(rule_number, {})

    for key, value in rule.items():
        if isinstance(value, (int, float, dict)):
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    input_key = f"{key}_{sub_key}"
                    prev_value = selected_filters[rule_number].get(input_key, sub_value)
                    new_value = st.sidebar.number_input(
                        f"{key} ({sub_key}) for {rule_number}", value=prev_value, key=f"{rule_number}_{input_key}"
                    )
                    selected_filters[rule_number][input_key] = new_value
            else:
                prev_value = selected_filters[rule_number].get(key, value)
                new_value = st.sidebar.number_input(
                    f"{key} for {rule_number}", value=prev_value, key=f"{rule_number}_{key}"
                )
                selected_filters[rule_number][key] = new_value

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
    file_type = st.session_state["file_type"]
    if file_type == "Excel":
        df_baseline = pd.read_excel(uploaded_file_baseline, engine="openpyxl")
        df_candidate = pd.read_excel(uploaded_file_candidate, engine="openpyxl")
    elif file_type in ["Text", "DD"]:
        delimiter = processor.rules_config.get("text_file_delimiter", ",")
        header = 0 if processor.rules_config.get("text_file_contains_header", "yes").lower() == "yes" else None
        df_baseline = pd.read_csv(uploaded_file_baseline, delimiter=delimiter, header=header)
        df_candidate = pd.read_csv(uploaded_file_candidate, delimiter=delimiter, header=header)
    else:
        st.error("Unsupported file type selected.")
        st.stop()

    updated_results = processor.compare_files(
        df_baseline, df_candidate, st.session_state["file_type"], st.session_state["selected_filters"]
    )

    st.session_state["results"] = updated_results
    st.session_state["filtered_results"] = updated_results
    st.rerun()

# **📤 Export Button**
filtered_results = st.session_state.get("filtered_results", pd.DataFrame())
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

