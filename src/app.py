import streamlit as st
import os
import pandas as pd
import plotly.express as px
from utils.data_processor import DataProcessor

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
    st.session_state["selected_filters"] = {}

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

    # ✅ Load Rules Config
    rules_config_path = st.session_state["rules_config_path"]
    if rules_config_path and os.path.exists(rules_config_path):
        rules_config = pd.read_json(rules_config_path)
    else:
        st.error("Rules configuration file not found!")
        st.stop()

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

            except Exception as e:
                st.error(f"Error processing files: {str(e)}")

# ✅ Get results (if available)
results = st.session_state.get("results", pd.DataFrame())

# **🎯 Display KPIs**
if not results.empty:
    st.header("Key Performance Indicators")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Discrepancies", len(results))

    warning_count = (results["Category"] == "Warning").sum()
    fatal_count = (results["Category"] == "FATAL").sum()

    col2.metric("Warning Threshold", warning_count)
    col3.metric("Fatal Discrepancies", fatal_count)


    # ✅ **Dynamic Bar Chart Using Selected Column**
    st.header(f"Discrepancy Analysis")

    fig = px.bar(
        results,
        x="Column Name",
        y="Category",
        color="Category",
        barmode="group",
        title="Discrepancies by Column"
    )
    st.plotly_chart(fig, use_container_width=True)

    # ✅ **Pie Chart: Category Distribution**
    st.header("Discrepancy Distribution")
    pie_chart = px.pie(results, names="Category", title="Proportion of Discrepancy Types", hole=0.4)
    st.plotly_chart(pie_chart, use_container_width=True)

    # ✅ **Filtered Data Table Based on Selected Column**
    st.header("Discrepancy Details")
    st.dataframe(results)

