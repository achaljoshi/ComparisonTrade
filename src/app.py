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

# ✅ Retrieve existing results DataFrame from session state
df = st.session_state.get("results", pd.DataFrame()).copy()

if df.empty:
    print("⚠️ Warning: No discrepancy results found in session state.")

# ✅ Debug: Print first few rows of Column Name
if not df.empty:
    print("✅ Debug: First few rows of `df['Column Name']`")
    print(df["Column Name"].head())

# ✅ Store dynamically selected constraints
selected_constraints = {}

# ✅ Process Normal Rules (groupType)
if "rules" in rules_config:
    for category, rules in rules_config["rules"].items():
        for rule in rules:
            rule_number = rule.get("rulenumber", "Unknown Rule")
            rule_type = rule.get("type", "Unknown Type")
            rule_columns = rule.get("columns", [])
            constraints = rule.get("constraints", {})
            format_type = rule.get("format_type", "Simple")

            print(f"🔍 Processing Rule: {rule_number}, Type: {rule_type}, Format: {format_type}, Columns: {rule_columns}")

            # ✅ Sidebar UI for constraints (min/max)
            st.sidebar.subheader(f"⚖️ {rule_number} ({rule_type})")
            min_constraint = st.sidebar.number_input(f"Min ({rule_number})", value=constraints.get("min", 0.0))
            max_constraint = st.sidebar.number_input(f"Max ({rule_number})", value=constraints.get("max", 100.0))

            # ✅ Store constraints dynamically
            selected_constraints[rule_number] = {
                "min": min_constraint,
                "max": max_constraint,
                "format_type": format_type,
                "rule_type": rule_type,
                "columns": rule_columns
            }

# ✅ Process Tick Sizes Separately
tick_size_constraints = {}
if "rules" in rules_config and "tickSizes" in rules_config["rules"]:
    print("✅ Debug: Processing tickSizes rules...")

    for rule in rules_config["rules"]["tickSizes"]:
        rule_number = rule.get("rulenumber", "Unknown Rule")
        rule_type = rule.get("type", "Unknown Type")
        rule_columns = rule.get("columns", [])
        constraints = rule.get("constraints", {})
        format_type = rule.get("format_type", "Array")

        tick_size_min = constraints.get("min", None)
        tick_size_max = constraints.get("max", None)

        print(f"🔍 TickSizes Rule: {rule_number}, Type: {rule_type}, Format: {format_type}, Min: {tick_size_min}, Max: {tick_size_max}")

        # ✅ Sidebar UI for Tick Size (only for Array format_type)
        if format_type == "Array":
            st.sidebar.subheader(f"📏 Tick Size ({rule_number})")
            if tick_size_min is not None and tick_size_max is not None:
                min_constraint = st.sidebar.number_input(f"Min Tick Size ({rule_number})", value=tick_size_min)
                max_constraint = st.sidebar.number_input(f"Max Tick Size ({rule_number})", value=tick_size_max)

        # ✅ Store Tick Size Constraints
        tick_size_constraints[rule_number] = {
            "min": tick_size_min,
            "max": tick_size_max,
            "format_type": format_type,
            "rule_type": rule_type,
            "columns": rule_columns
        }
print("🔍 Debugging: Checking DataFrame Before Processing")

if "results" in st.session_state:
    df = st.session_state["results"]
    print(f"✅ Debug: Found `results` in session_state. DataFrame shape: {df.shape}")
else:
    print("❌ Error: `results` is missing in `st.session_state`.")
# ✅ Apply Filters Automatically Based on Rule Type
if not df.empty:
    # ✅ Ensure numeric conversion
    def extract_numeric(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    df["Baseline Field Value Numeric"] = df["Baseline Field Value"].apply(extract_numeric)
    df["Candidate Field Value Numeric"] = df["Candidate Field Value"].apply(extract_numeric)

    # ✅ Compute absolute difference
    df["Difference"] = abs(df["Baseline Field Value Numeric"] - df["Candidate Field Value Numeric"])

    # ✅ Apply groupType Classification (Fixing Previous Issue)
    for rule_number, rule_data in selected_constraints.items():
        min_constraint = rule_data.get("min", None)
        max_constraint = rule_data.get("max", None)
        rule_type = rule_data.get("rule_type", "Unknown Type")
        columns = rule_data.get("columns", [])

        for col in columns:
            classification = rules_config["rules"].get(col, [{}])[0].get("classification", {})

            print(f"✅ Applying Classification for {col}")

            df.loc[(df["Column Name"] == col) &
                   (df["Rule Type"] == rule_type) &
                   (df["Difference"] <= min_constraint), "Classification"] = classification.get("min", "classification1")

            df.loc[(df["Column Name"] == col) &
                   (df["Rule Type"] == rule_type) &
                   (df["Difference"] >= max_constraint), "Classification"] = classification.get("max", "classification2")

            df.loc[(df["Column Name"] == col) &
                   (df["Rule Type"] == rule_type) &
                   (df["Difference"] > min_constraint) &
                   (df["Difference"] < max_constraint), "Classification"] = classification.get("min_max", "classification3")

            df.loc[df["Classification"].isna(), "Classification"] = classification.get("other", "classification4")

    # ✅ Apply Tick Size Classification
    for rule_number, rule_data in tick_size_constraints.items():
        tick_size_min = rule_data.get("min", None)
        tick_size_max = rule_data.get("max", None)
        rule_type = rule_data.get("rule_type", "Unknown Type")
        format_type = rule_data.get("format_type", "Array")

        print(f"✅ Applying Tick Size Rule: {rule_number}, Min: {tick_size_min}, Max: {tick_size_max}, Format: {format_type}")

        if format_type == "Array" and tick_size_min is not None and tick_size_max is not None:
            for nested_col in ["lowerLimit", "upperLimit", "tickSize"]:
                # ✅ Improve Filtering: Check if Column Name starts with "tickSizes" to include nested values
                affected_rows = df[df["Column Name"].str.startswith("tickSizes") &
                                   df["Column Name"].str.contains(nested_col, na=False, regex=True) &
                                   (df["Rule Type"] == rule_type)]

                print(f"📊 Debug: {nested_col} affected rows before classification: {len(affected_rows)}")

                if not affected_rows.empty:
                    print(f"✅ Applying Tick Size Classification for {nested_col}...")

                    # ✅ Fix: Preserve existing classifications while applying "Tick Size Out of Range"
                    df.loc[(df["Column Name"].str.startswith("tickSizes")) &
                           (df["Column Name"].str.contains(nested_col, na=False, regex=True)) &
                           (df["Rule Type"] == rule_type) &
                           (df["Difference"].notna()) &
                           ((df["Difference"] < tick_size_min) | (
                                       df["Difference"] > tick_size_max)), "Classification"] = df[ "Classification"].fillna(
                        "") + " | Tick Size Out of Range"

                print(
                    f"📊 Debug: {nested_col} affected rows after classification: {df[df['Classification'].str.contains('Tick Size Out of Range', na=False)].shape[0]}")

# ✅ Store updated classified results dynamically
st.session_state["filtered_results"] = df


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
                    rule_type = rule.get("type", "Unknown Type")  # ✅ Get Rule Type
                    constraints = rule.get("constraints", {})
                    rule_columns = rule.get("columns", [])
                    format_type = rule.get("format_type", "Simple")

                    min_constraint = constraints.get("min", None)
                    max_constraint = constraints.get("max", None)

                    print(f"\n🔍 [DEBUG] Processing Rule: {rule_number} ({format_type})")
                    print(f"🔹 Rule Type: {rule_type}")
                    print(f"🔹 Columns: {rule_columns}")
                    print(f"🔹 Old Min: {constraints.get('min', None)}, New Min: {min_constraint}")
                    print(f"🔹 Old Max: {constraints.get('max', None)}, New Max: {max_constraint}")

                    # ✅ Apply filtering based on format type + Rule Type
                    for col in rule_columns:
                        if format_type == "Array":
                            df = df[~((df["Column Name"].str.contains(col, regex=False)) &
                                      (df["Rule Type"] == rule_type) &  # ✅ Now filtering with Rule Type
                                      (df["Difference"] < min_constraint))]
                            df = df[~((df["Column Name"].str.contains(col, regex=False)) &
                                      (df["Rule Type"] == rule_type) &  # ✅ Now filtering with Rule Type
                                      (df["Difference"] > max_constraint))]
                        else:
                            if min_constraint is not None:
                                df = df[~((df["Column Name"] == col) &
                                          (df["Rule Type"] == rule_type) &  # ✅ Now filtering with Rule Type
                                          (df["Difference"] < min_constraint))]
                            if max_constraint is not None:
                                df = df[~((df["Column Name"] == col) &
                                          (df["Rule Type"] == rule_type) &  # ✅ Now filtering with Rule Type
                                          (df["Difference"] > max_constraint))]

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