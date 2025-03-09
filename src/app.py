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
from utils.ui_helpers import UIHelper
from utils.file_handler import FileHandler

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
if "config_files" not in st.session_state:
    st.session_state["config_files"] = {
        "directory_config": None,
        "job_response": None,
        "rules_config": None,
        "uploaded_configs": {}  # Store uploaded file contents
    }
if "selected_filters" not in st.session_state:
    st.session_state["selected_filters"] = {}
if "filtered_results" not in st.session_state:
    st.session_state["filtered_results"] = pd.DataFrame()

# ✅ Define required config files before using them
required_files = {
    "directory_config.json": "directory_config",
    "job_creation_response.json": "job_response",
    "rules_config.json": "rules_config"
}

# File type mapping
FILE_TYPE_MAPPING = {
    "Excel (.xlsx)": "Excel",
    "DD (.txt)": "DD",
    "Flat Files (.txt, .csv, .json, .log)": "Text"
}

def extract_numeric(value):
    if isinstance(value, str):
        match = re.search(r"\b\d+\b", value)  # Extract first number
        return float(match.group()) if match else None
    return value

def main():
    """Main application entry point."""
    UIHelper.initialize_session_state()
    
    # Initialize file handler
    file_handler = FileHandler()
    
    # Handle different screens
    if st.session_state["screen"] == "upload_config":
        handle_config_upload(file_handler)
    elif st.session_state["screen"] == "file_type_selection":
        handle_file_type_selection()
    elif st.session_state["screen"] == "file_selection":
        handle_file_selection(file_handler)
        # Only show filters and results if we have processed results
        if "results" in st.session_state and not st.session_state["filtered_results"].empty:
            handle_filters_and_results()
    
    # Cleanup button
    if st.button("🗑️ Cleanup Temporary Files"):
        success, message = file_handler.cleanup()
        if success:
            st.success(message)
        else:
            st.error(message)

def handle_config_upload(file_handler: FileHandler):
    """Handle configuration file upload screen."""
    st.title("Upload Configuration Files")
    
    # Identify missing files
    missing_files = {
        filename: config_key
        for filename, config_key in required_files.items()
        if not st.session_state["config_files"]["uploaded_configs"].get(config_key)
    }
    
    if not missing_files:
        if st.button("Next"):
            st.session_state["screen"] = "file_type_selection"
            st.rerun()
        return
    
    # Handle file uploads
    for filename, config_key in missing_files.items():
        st.write(f"Please upload {filename}")
        uploaded_file = st.file_uploader(
            f"Upload {filename}",
            type=["json"],
            key=f"upload_{config_key}"
        )
        
        if uploaded_file:
            try:
                # Read the uploaded file content
                content = json.load(uploaded_file)
                # Store the content in session state
                st.session_state["config_files"]["uploaded_configs"][config_key] = content
                st.success(f"✅ {filename} uploaded successfully!")
            except Exception as e:
                st.error(f"Error processing {filename}: {str(e)}")
                logging.error(f"Error processing {filename}: {str(e)}")
    
    # Check if all files are uploaded
    all_uploaded = all(
        st.session_state["config_files"]["uploaded_configs"].get(config_key) is not None
        for config_key in [v for k, v in required_files.items()]
    )
    
    if all_uploaded:
        st.success("✅ All configuration files uploaded successfully! Click 'Next' to proceed.")
        
        if st.button("Next"):
            st.session_state["screen"] = "file_type_selection"
            st.rerun()

def handle_file_type_selection():
    """Handle file type selection screen."""
    st.title("Select File Type for Comparison")
    
    file_type = st.radio(
        "Choose the file type:",
        list(FILE_TYPE_MAPPING.keys())
    )
    
    if st.button("Next", key="next_button_file_type"):
        st.session_state["file_type"] = FILE_TYPE_MAPPING[file_type]
        st.session_state["screen"] = "file_selection"
        st.rerun()

def handle_file_selection(file_handler: FileHandler):
    """Handle file selection and comparison screen."""
    st.title("Upload Files for Comparison")
    
    # Show the currently selected file type
    st.info(f"Selected file type: {st.session_state['file_type']}")
    
    # Show allowed extensions based on file type
    allowed_extensions = {
        "Excel": ".xlsx, .xls",
        "DD": ".txt, .dat",
        "Text": ".txt, .csv, .json, .log"
    }
    st.write(f"Allowed file extensions: {allowed_extensions[st.session_state['file_type']]}")
    
    uploaded_file_baseline = st.file_uploader(
        "Upload Baseline File",
        type=["xlsx", "xls", "txt", "csv", "json", "log", "dat"],
        key="baseline_file"
    )
    uploaded_file_candidate = st.file_uploader(
        "Upload Candidate File",
        type=["xlsx", "xls", "txt", "csv", "json", "log", "dat"],
        key="candidate_file"
    )
    
    if st.button("Run Comparison", key="run_comparison_button") and (uploaded_file_baseline and uploaded_file_candidate):
        try:
            # Initialize processor with uploaded configs
            configs = st.session_state["config_files"]["uploaded_configs"]
            processor = DataProcessor(
                configs["directory_config"],
                configs["job_response"],
                configs["rules_config"]
            )
            
            # Store processor in session state
            st.session_state["processor"] = processor
            
            # Detect file type and prepare files
            try:
                file_type = file_handler.detect_file_type(uploaded_file_baseline, st.session_state["file_type"])
                # Verify both files are of the same type
                candidate_type = file_handler.detect_file_type(uploaded_file_candidate, st.session_state["file_type"])
                if file_type != candidate_type:
                    st.error(f"File type mismatch: Baseline is {file_type}, Candidate is {candidate_type}")
                    return
            except ValueError as e:
                st.error(f"File type error: {str(e)}")
                return
            
            baseline_bytes = file_handler.prepare_file_for_processing(uploaded_file_baseline)
            candidate_bytes = file_handler.prepare_file_for_processing(uploaded_file_candidate)
            
            # Process files
            df_baseline = processor.read_file(baseline_bytes, file_type)
            df_candidate = processor.read_file(candidate_bytes, file_type)
            
            if df_baseline.empty or df_candidate.empty:
                st.error("One of the uploaded files is empty. Please check your data.")
                return
            
            # Run comparison
            results = processor.compare_files(df_baseline, df_candidate, file_type)
            
            if results is None or results.empty:
                st.error("No comparison results generated. Please check your input files and configuration.")
                return
            
            # Store results and files in session state
            st.session_state.update({
                "results": results,
                "filtered_results": results,
                "uploaded_file_baseline": uploaded_file_baseline,
                "uploaded_file_candidate": uploaded_file_candidate,
                "file_type": file_type
            })
            
            st.success("✅ Comparison completed! Discrepancy report generated.")
            st.rerun()  # Rerun to show filters and results
            
        except Exception as e:
            st.error(f"Error processing files: {str(e)}")
            logging.error(f"Error processing files: {str(e)}", exc_info=True)
            # Clear processor if initialization failed
            if "processor" in st.session_state:
                del st.session_state["processor"]

def handle_filters_and_results():
    """Handle filter UI and results display."""
    if "processor" not in st.session_state:
        return
        
    try:
        # Get filter metadata and render filter UI
        filter_metadata = st.session_state["processor"].get_filter_metadata()
        if not filter_metadata:
            st.warning("No filter metadata available. Please check your rules configuration.")
            return
            
        selected_filters = UIHelper.render_filter_ui(filter_metadata)
        
        # Store updated filters
        st.session_state["selected_filters"] = selected_filters
        
        # Handle filter buttons
        apply_filter_clicked = st.sidebar.button("📌 Apply Filter", key="apply_filter_button")
        reset_filter_clicked = st.sidebar.button("♻️ Reset Filters", key="reset_filter_button")
        
        if reset_filter_clicked:
            st.session_state["selected_filters"] = {}
            st.session_state["filtered_results"] = st.session_state.get("results", pd.DataFrame())
            st.rerun()
        
        if apply_filter_clicked and "results" in st.session_state:
            apply_filters()
        
        # Display results
        UIHelper.render_results(st.session_state.get("filtered_results", pd.DataFrame()))
        
    except Exception as e:
        st.error(f"Error handling filters and results: {str(e)}")
        logging.error(f"Error handling filters and results: {str(e)}", exc_info=True)

def apply_filters():
    """Apply filters to the results."""
    try:
        processor = st.session_state["processor"]
        
        # Process the selected filters
        processed_filters = processor.process_filters(st.session_state["selected_filters"])
        
        # Log the processed filters for debugging
        logging.debug(f"Processed filters: {processed_filters}")
        
        # Get files and process
        file_type = st.session_state["file_type"]
        baseline_file = st.session_state["uploaded_file_baseline"]
        candidate_file = st.session_state["uploaded_file_candidate"]
        
        if not all([baseline_file, candidate_file]):
            st.error("Missing comparison files. Please upload both baseline and candidate files.")
            return
        
        # Process files
        file_handler = FileHandler()
        baseline_bytes = file_handler.prepare_file_for_processing(baseline_file)
        candidate_bytes = file_handler.prepare_file_for_processing(candidate_file)
        
        df_baseline = processor.read_file(baseline_bytes, file_type)
        df_candidate = processor.read_file(candidate_bytes, file_type)
        
        if df_baseline.empty or df_candidate.empty:
            st.error("One of the files is empty after processing. Please check your data.")
            return
        
        # Apply comparison with processed filters
        updated_results = processor.compare_files(
            df_baseline,
            df_candidate,
            file_type,
            processed_filters
        )
        
        if updated_results is None or updated_results.empty:
            st.error("No results after applying filters. Please check your filter settings.")
            return
            
        # Update results
        st.session_state["filtered_results"] = updated_results
        
        # Show success message
        st.sidebar.success("Filters applied successfully!")
        
        # Rerun to update the UI
        st.rerun()
        
    except Exception as e:
        st.error(f"Error applying filters: {str(e)}")
        logging.error(f"Error applying filters: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()