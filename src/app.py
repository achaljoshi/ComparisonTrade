import shutil
import streamlit as st
import pandas as pd
import os
from utils.data_processor import DataProcessor

# Use system environment variables if available, otherwise set to None
directory_config_path = os.getenv("DIRECTORY_CONFIG_PATH")
job_response_path = os.getenv("JOB_RESPONSE_PATH")
rules_config_path = os.getenv("RULES_CONFIG_PATH")

# Change save directory to a writable location
save_directory = os.path.expanduser("~/config/")  # Use home directory for write access

# Get the absolute path of the current script's directory
base_dir = os.path.dirname(os.path.abspath(__file__))
cache_dir = os.path.join(base_dir, "utils", "__pycache__")

# Check if the directory exists before attempting to delete it
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)
    print(f"Deleted {cache_dir}")
else:
    print(f"No cache directory found at {cache_dir}")
    
# Ensure the directory exists
if not os.path.exists(save_directory):
    os.makedirs(save_directory, exist_ok=True)

# Check which configuration files are missing
missing_files = {}

if not directory_config_path:
    missing_files["directory_config.json"] = os.path.join(save_directory, "directory_config.json")
if not job_response_path:
    missing_files["job_creation_response.json"] = os.path.join(save_directory, "job_creation_response.json")
if not rules_config_path:
    missing_files["rules_config.json"] = os.path.join(save_directory, "rules_config.json")

# **Step 1: Upload Config Files if Missing**
if "screen" not in st.session_state:
    st.cache_data.clear()
    st.session_state["screen"] = "upload_config"

if st.session_state["screen"] == "upload_config":
    st.title("Upload Configuration Files")

    if missing_files:
        st.error(f"The following configuration files are missing: {', '.join(missing_files.keys())}. Please upload them.")

        uploaded_files = {}
        for file_name, save_path in missing_files.items():
            uploaded_file = st.file_uploader(f"Upload `{file_name}`", type=["json"])
            if uploaded_file:
                with open(save_path, "wb") as f:
                    f.write(uploaded_file.read())
                uploaded_files[file_name] = save_path  # Store uploaded file paths

        # **Set paths after upload**
        if len(uploaded_files) == len(missing_files):
            directory_config_path = uploaded_files.get("directory_config.json", directory_config_path)
            job_response_path = uploaded_files.get("job_creation_response.json", job_response_path)
            rules_config_path = uploaded_files.get("rules_config.json", rules_config_path)

            # Store paths in session state to prevent loss on rerun
            st.session_state["directory_config_path"] = directory_config_path
            st.session_state["job_response_path"] = job_response_path
            st.session_state["rules_config_path"] = rules_config_path

            st.success("Configuration files uploaded successfully! Click 'Next' to proceed.")
            if st.button("Next"):
                st.session_state["screen"] = "file_type_selection"
                st.rerun()

# **Step 2: Select File Type**
elif st.session_state["screen"] == "file_type_selection":
    st.title("Select File Type for Comparison")

    file_type = st.radio(
        "Choose the type of files you want to compare:",
        ["Excel (.xlsx)", "Datadog Logs (.log)", "Text Files (.txt)"]
    )

    if st.button("Next"):
        st.session_state["file_type"] = file_type
        st.session_state["screen"] = "file_selection"
        st.rerun()

# **Step 3: Show File Upload Based on Selected File Type**
elif st.session_state["screen"] == "file_selection":
    st.title(f"Upload {st.session_state['file_type']} Files for Comparison")

    # **Retrieve config paths from session state**
    directory_config_path = st.session_state.get("directory_config_path")
    job_response_path = st.session_state.get("job_response_path")
    rules_config_path = st.session_state.get("rules_config_path")

    if not directory_config_path or not job_response_path or not rules_config_path:
        st.error("Configuration paths are missing! Please restart and upload the required files.")
        st.stop()

    # **Initialize DataProcessor**
    processor = DataProcessor(directory_config_path, job_response_path, rules_config_path)

    # File upload options based on selected file type
    file_type = st.session_state["file_type"]
    
    uploaded_file_baseline = None
    uploaded_file_candidate = None
    df_baseline = None
    df_candidate = None

    if file_type == "Excel (.xlsx)":
        uploaded_file_baseline = st.file_uploader("Upload Baseline File (.xlsx)", type=["xlsx"])
        uploaded_file_candidate = st.file_uploader("Upload Candidate File (.xlsx)", type=["xlsx"])
        if uploaded_file_baseline and uploaded_file_candidate:
            df_baseline = pd.read_excel(uploaded_file_baseline, engine="openpyxl")
            df_candidate = pd.read_excel(uploaded_file_candidate, engine="openpyxl")

    elif file_type == "Datadog Logs (.log)":
        uploaded_file_baseline = st.file_uploader("Upload Baseline Log File (.log)", type=["log"])
        uploaded_file_candidate = st.file_uploader("Upload Candidate Log File (.log)", type=["log"])
        if uploaded_file_baseline and uploaded_file_candidate:
            df_baseline = pd.read_csv(uploaded_file_baseline, delimiter="\n", header=None, names=["log_text"])
            df_candidate = pd.read_csv(uploaded_file_candidate, delimiter="\n", header=None, names=["log_text"])

    elif file_type == "Text Files (.txt)":
        uploaded_file_baseline = st.file_uploader("Upload Baseline Text File (.txt)", type=["txt"])
        uploaded_file_candidate = st.file_uploader("Upload Candidate Text File (.txt)", type=["txt"])
        delimiter = processor.rules_config.get("text_file_delimiter", ",")  # Get delimiter from rules_config
        if uploaded_file_baseline and uploaded_file_candidate:
            df_baseline = pd.read_csv(uploaded_file_baseline, delimiter=delimiter)
            df_candidate = pd.read_csv(uploaded_file_candidate, delimiter=delimiter)

    if st.button("Run Comparison"):
        if df_baseline is not None and df_candidate is not None:
            st.write("✅ Uploaded files are successfully read as DataFrames.")
            results = processor.compare_files(df_baseline, df_candidate, file_type)
            st.success("Comparison Completed! Discrepancy report generated.")
            st.dataframe(results)
        else:
            st.error("❌ Please upload both baseline and candidate files before running the comparison.")
