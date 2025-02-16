import streamlit as st
import os
import pandas as pd
import plotly.express as px
import json
from io import BytesIO
from utils.data_processor import DataProcessor
import plotly.colors
import shutil



# Get the absolute path of the current script's directory
base_dir = os.path.dirname(os.path.abspath(__file__))
cache_dir = os.path.join(base_dir, "utils", "__pycache__")

# Check if the directory exists before attempting to delete it
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)
    print(f"Deleted {cache_dir}")

# **✅ Ensure `st.set_page_config()` is first**
st.set_page_config(page_title="Discrepancy Dashboard", layout="wide")

# **🔄 Clear Cache & Restart Button**
if st.sidebar.button("🔄 Clear Cache & Restart"):
    st.cache_data.clear()  # Clears cached data
    st.session_state.clear()  # Resets all session state variables
    st.rerun()  # ✅ Restart the app

# **🛠️ Ensure session state variables persist**
if "screen" not in st.session_state:
    st.session_state["screen"] = "upload_config"

if "directory_config_path" not in st.session_state:
    st.session_state["directory_config_path"] = None
if "job_response_path" not in st.session_state:
    st.session_state["job_response_path"] = None
if "rules_config_path" not in st.session_state:
    st.session_state["rules_config_path"] = None

# **Save directory for uploaded config files**
save_directory = os.path.expanduser("~/config/")
if not os.path.exists(save_directory):
    os.makedirs(save_directory, exist_ok=True)

# **Step 1: Upload Configuration Files**
missing_files = {}
if not st.session_state["directory_config_path"]:
    missing_files["directory_config.json"] = os.path.join(save_directory, "directory_config.json")
if not st.session_state["job_response_path"]:
    missing_files["job_creation_response.json"] = os.path.join(save_directory, "job_creation_response.json")
if not st.session_state["rules_config_path"]:
    missing_files["rules_config.json"] = os.path.join(save_directory, "rules_config.json")
if "selected_filters" not in st.session_state:
    st.session_state["selected_filters"] = {}  # ✅ Store dynamically created filters
if "filtered_results" not in st.session_state:
    st.session_state["filtered_results"] = pd.DataFrame()


if st.session_state["screen"] == "upload_config":
    st.title("Upload Configuration Files")

    if missing_files:
        st.error(f"The following configuration files are missing: {', '.join(missing_files.keys())}. Please upload them.")

        uploaded_files = {}
        for file_name, save_path in missing_files.items():
            uploaded_file = st.file_uploader(f"Upload `{file_name}`", type=["json"], key=file_name)
            if uploaded_file:
                with open(save_path, "wb") as f:
                    f.write(uploaded_file.read())
                uploaded_files[file_name] = save_path
                st.success(f"`{file_name}` uploaded successfully!")

        # **Store uploaded file paths in session state**
        if len(uploaded_files) == len(missing_files):
            st.session_state["directory_config_path"] = uploaded_files.get("directory_config.json", st.session_state["directory_config_path"])
            st.session_state["job_response_path"] = uploaded_files.get("job_creation_response.json", st.session_state["job_response_path"])
            st.session_state["rules_config_path"] = uploaded_files.get("rules_config.json", st.session_state["rules_config_path"])

            st.success("Configuration files uploaded successfully! Click 'Next' to proceed.")

            if st.button("Next"):
                st.session_state["screen"] = "file_type_selection"
                st.rerun()  # ✅ Refresh screen to move to the next step

    else:
        st.success("All configuration files are already uploaded. Click 'Next' to proceed.")
        if st.button("Next"):
            st.session_state["screen"] = "file_type_selection"
            st.rerun()

    st.stop()  # ✅ Prevent the app from moving forward until all files are uploaded

# **Debugging: Print session state to check stored values**
# st.sidebar.write("📌 Debug Info:", st.session_state)

# **Step 2: Select File Type**
if st.session_state["screen"] == "file_type_selection":
    st.title("Select File Type for Comparison")

    # Ensure `rules_config.json` exists before proceeding
    if not st.session_state["rules_config_path"] or not os.path.exists(st.session_state["rules_config_path"]):
        st.error("Rules configuration file not found! Please go back and upload it.")
        if st.button("Go Back"):
            st.session_state["screen"] = "upload_config"
            st.rerun()
        st.stop()

    file_type = st.radio(
        "Choose the type of files you want to compare:",
        ["Excel (.xlsx)", "Datadog Logs (.log)", "Text Files (.txt)"]
    )

    if st.button("Next"):
        st.session_state["file_type"] = file_type
        st.session_state["screen"] = "file_selection"
        st.rerun()
    st.stop()

# **Step 3: Upload Files for Comparison (Only Visible If on This Step)**
elif st.session_state.get("screen") == "file_selection":
    st.title("Upload Files for Comparison")

    # ✅ Load the rules_config file
    rules_config_path = st.session_state["rules_config_path"]
    if rules_config_path and os.path.exists(rules_config_path):
        rules_config = pd.read_json(rules_config_path)
    else:
        rules_config = {"rules": []}

    # **Load Job Response Config**
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

    # ✅ Sidebar: Dynamic Filters for Each Rule
    st.sidebar.header("🔍 Filter Rules")
    selected_filters = st.session_state["selected_filters"]

    for rule in rules_config.get("rules", []):
        rule_number = rule.get("Rule Number", "Unknown Rule")
        rule_type = rule.get("type", "Unknown Type")
        rule_columns = ", ".join(rule.get("columns", []))

        st.sidebar.subheader(f"⚖️ {rule_number} ({rule_type})")
        st.sidebar.write(f"📝 Columns: {rule_columns}")

        # ✅ Create input fields dynamically for all numeric parameters
        for key, value in rule.items():
            if isinstance(value, (int, float)):  # Detect numeric values dynamically
                selected_filters.setdefault(rule_number, {})  # Initialize if not exists
                selected_filters[rule_number][key] = st.sidebar.number_input(
                    f"{key} for {rule_number}", value=value
                )

    # ✅ Save filters in session state
    st.session_state["selected_filters"] = selected_filters

    # ✅ File Upload Section
    uploaded_file_baseline = st.file_uploader("Upload Baseline File", type=["xlsx", "log", "txt"])
    uploaded_file_candidate = st.file_uploader("Upload Candidate File", type=["xlsx", "log", "txt"])

    if st.button("Run Comparison"):
        if uploaded_file_baseline and uploaded_file_candidate:
            try:
                processor = DataProcessor(
                    st.session_state["directory_config_path"],
                    st.session_state["job_response_path"],
                    st.session_state["rules_config_path"]
                )
                df_baseline = pd.read_excel(uploaded_file_baseline, engine="openpyxl")
                df_candidate = pd.read_excel(uploaded_file_candidate, engine="openpyxl")

                # ✅ Run Comparison
                st.write("✅ Uploaded files are successfully read as DataFrames.")
                results = processor.compare_files(df_baseline, df_candidate, st.session_state["file_type"])
                st.success("Comparison Completed! Discrepancy report generated.")

                # ✅ Store results in session state
                st.session_state["results"] = results
                st.session_state["filtered_results"] = results  # ✅ Initial filtered results

            except Exception as e:
                st.error(f"Error processing files: {str(e)}")

# ✅ Get results (if available)
results = st.session_state.get("results", pd.DataFrame())
filtered_results = st.session_state.get("filtered_results", pd.DataFrame())  # ✅ Ensure not None

# **📤 Export Button**
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

# ✅ Display KPIs
if not filtered_results.empty:

    st.header("📊 Key Performance Indicators")

    # ✅ Ensure consistent capitalization in Category column
    filtered_results["Category"] = filtered_results["Category"].str.upper()

    # ✅ Count each unique category dynamically
    category_counts = filtered_results["Category"].value_counts().to_dict()


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
    unique_categories = filtered_results["Category"].unique()
    # ✅ Generate distinct colors dynamically using Plotly's color palette
    color_palette = plotly.colors.qualitative.Set1  # Choose a color set
    color_map = {category: color_palette[i % len(color_palette)] for i, category in enumerate(unique_categories)}
    # ✅ Count discrepancies per column and category
    discrepancy_counts = filtered_results.groupby(["Column Name", "Category"]).size().reset_index(name="Count")
    # ✅ Bar Chart: Count of Discrepancies by Column
    st.header("📊 Discrepancy Analysis")
    fig = px.bar(
        discrepancy_counts,
        x="Column Name",
        y="Count",
        color="Category",
        title="Discrepancies by Column",
        barmode="group",
        color_discrete_map=color_map  # ✅ Now dynamically generated
    )
    st.plotly_chart(fig, use_container_width=True)


    # ✅ **Pie Chart: Category Distribution**
    st.header("Discrepancy Distribution")
    pie_chart = px.pie(filtered_results, names="Category", title="Proportion of Discrepancy Types", hole=0.4)
    st.plotly_chart(pie_chart, use_container_width=True)

    # ✅ **Filtered Data Table Based on Selected Column**
    st.header("Discrepancy Details")
    st.dataframe(filtered_results)

