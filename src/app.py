import streamlit as st
from typing import Dict, Optional, Union
from dataclasses import dataclass
import pandas as pd
import plotly.express as px
import plotly.colors
from datetime import datetime
from utils.data_processor import DataProcessor, ComparisonResult
import dask.dataframe as dd
from concurrent.futures import ThreadPoolExecutor
import json
from io import BytesIO

CHUNK_SIZE = 100000
MAX_WORKERS = 4

@dataclass
class AppState:
    screen: str = "upload_config"
    directory_config: Optional[Dict] = None
    job_response: Optional[Dict] = None
    rules_config: Optional[Dict] = None
    selected_filters: Dict = None
    comparison_results: Optional[ComparisonResult] = None
    file_type: Optional[str] = None
    baseline_df: Optional[dd.DataFrame] = None
    candidate_df: Optional[dd.DataFrame] = None

class DiscrepancyDashboard:
    def __init__(self):
        self.initialize_app()
        self.color_palette = plotly.colors.qualitative.Set1

    def initialize_app(self) -> None:
        st.set_page_config(page_title="Discrepancy Dashboard", layout="wide")
        if "state" not in st.session_state:
            st.session_state.state = AppState()
        self.setup_sidebar()

    def setup_sidebar(self) -> None:
        if st.sidebar.button("🔄 Clear Cache & Restart"):
            st.cache_data.clear()
            st.session_state.clear()
            st.rerun()

    def run(self) -> None:
        current_screen = st.session_state.state.screen
        if current_screen == "upload_config":
            self.handle_config_upload()
        elif current_screen == "file_type_selection":
            self.handle_file_type_selection()
        elif current_screen == "file_selection":
            self.handle_file_comparison()

    def handle_config_upload(self) -> None:
        st.title("Upload Configuration Files")
        uploaded_files = {}
        
        for config_name, state_key in {
            "Directory Config": "directory_config",
            "Job Response": "job_response",
            "Rules Config": "rules_config"
        }.items():
            uploaded_file = st.file_uploader(f"Upload {config_name}", type=["json"])
            if uploaded_file:
                try:
                    config_data = json.load(uploaded_file)
                    setattr(st.session_state.state, state_key, config_data)
                    uploaded_files[state_key] = config_data
                    st.success(f"{config_name} uploaded successfully!")
                except Exception as e:
                    st.error(f"Error loading {config_name}: {str(e)}")

        if len(uploaded_files) == 3:
            if st.button("Next"):
                st.session_state.state.screen = "file_type_selection"
                st.rerun()

    def handle_file_type_selection(self) -> None:
        st.title("Select File Type for Comparison")
        file_type = st.radio(
            "Choose file type:",
            ["Excel (.xlsx)", "DD (.log, .csv)", "Text Files (.txt)"]
        )

        if st.button("Next"):
            file_type_mapping = {
                "Excel (.xlsx)": "Excel",
                "DD (.log, .csv)": "DD",
                "Text Files (.txt)": "Text"
            }
            st.session_state.state.file_type = file_type_mapping[file_type]
            st.session_state.state.screen = "file_selection"
            st.rerun()

    def handle_file_comparison(self) -> None:
        st.title("Upload Files for Comparison")
        
        baseline_file = st.file_uploader("Upload Baseline File", type=self._get_allowed_extensions())
        candidate_file = st.file_uploader("Upload Candidate File", type=self._get_allowed_extensions())

        if baseline_file and candidate_file and st.button("Run Comparison"):
            with st.spinner("Processing comparison..."):
                try:
                    processor = DataProcessor(
                        st.session_state.state.directory_config,
                        st.session_state.state.job_response,
                        st.session_state.state.rules_config
                    )
                    
                    # Process files in chunks using Dask
                    df_baseline = self._read_file_in_chunks(baseline_file)
                    df_candidate = self._read_file_in_chunks(candidate_file)
                    
                    results = processor.compare_files(
                        df_baseline,
                        df_candidate,
                        st.session_state.state.file_type,
                        st.session_state.state.selected_filters
                    )
                    
                    st.session_state.state.comparison_results = results
                    self.display_results(results)
                    
                except Exception as e:
                    st.error(f"Error during comparison: {str(e)}")

    def _get_allowed_extensions(self) -> list:
        file_type = st.session_state.state.file_type
        extensions = {
            "Excel": ["xlsx", "xls"],
            "Text": ["txt", "csv"],
            "DD": ["log", "csv"]
        }
        return extensions.get(file_type, [])

    def _read_file_in_chunks(self, file) -> dd.DataFrame:
        file_type = st.session_state.state.file_type
        if file_type == "Excel":
            return dd.from_pandas(pd.read_excel(file), npartitions=CHUNK_SIZE)
        return dd.read_csv(file, blocksize=CHUNK_SIZE)

    def display_results(self, results: ComparisonResult) -> None:
        st.header("📊 Key Performance Indicators")

        # Process results in chunks for large datasets
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            category_counts = executor.submit(
                lambda: results.discrepancies["Category"].value_counts().to_dict()
            ).result()
            missing_counts = executor.submit(
                self._calculate_missing_counts, results.discrepancies
            ).result()

        self._display_kpis(category_counts, missing_counts)
        self._display_visualizations(results.discrepancies)
        self._setup_filtering_sidebar()
        self._display_data_table(results.discrepancies)
        self._setup_export(results)

    def _calculate_missing_counts(self, df: pd.DataFrame) -> Dict:
        return {
            "baseline": (df["Rule Type"] == "Missing in Baseline").sum(),
            "candidate": (df["Rule Type"] == "Missing in Candidate").sum()
        }

    def _display_kpis(self, category_counts: Dict, missing_counts: Dict) -> None:
        total_metrics = len(category_counts) + 3
        kpi_columns = st.columns(min(total_metrics, 4))

        for idx, (category, count) in enumerate(category_counts.items()):
            kpi_columns[idx % 4].metric(f"{category}", count)

        kpi_columns[-2].metric("Missing in Baseline", missing_counts["baseline"])
        kpi_columns[-1].metric("Missing in Candidate", missing_counts["candidate"])

    def _display_visualizations(self, df: pd.DataFrame) -> None:
        st.header("📊 Discrepancy Analysis")
        
        # Optimize groupby operation for large datasets
        discrepancy_counts = df.groupby(["Column Name", "Category"], observed=True).size().reset_index(name="Count")
        
        # Generate color map for consistent visualization
        unique_categories = df["Category"].unique()
        color_map = {category: self.color_palette[i % len(self.color_palette)] 
                    for i, category in enumerate(unique_categories)}

        fig_bar = px.bar(
            discrepancy_counts,
            x="Column Name",
            y="Count",
            color="Category",
            title="Discrepancies by Column",
            barmode="group",
            color_discrete_map=color_map
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        fig_pie = px.pie(
            df,
            names="Category",
            title="Proportion of Discrepancy Types",
            hole=0.4,
            color="Category",
            color_discrete_map=color_map
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    def _setup_filtering_sidebar(self) -> None:
        st.sidebar.header("🔍 Filter Rules")
        
        rules_config = st.session_state.state.rules_config
        selected_filters = st.session_state.state.selected_filters or {}

        for rule in rules_config.get("rules", []):
            rule_number = rule.get("Rule Number", "Unknown Rule")
            rule_type = rule.get("type", "Unknown Type")
            rule_columns = ", ".join(rule.get("columns", []))

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
                                f"{key} ({sub_key}) for {rule_number}",
                                value=prev_value,
                                key=f"{rule_number}_{input_key}"
                            )
                            selected_filters[rule_number][input_key] = new_value
                    else:
                        prev_value = selected_filters[rule_number].get(key, value)
                        new_value = st.sidebar.number_input(
                            f"{key} for {rule_number}",
                            value=prev_value,
                            key=f"{rule_number}_{key}"
                        )
                        selected_filters[rule_number][key] = new_value

        if st.sidebar.button("📌 Apply Filters"):
            st.session_state.state.selected_filters = selected_filters
            self.rerun_comparison()

        if st.sidebar.button("♻️ Reset Filters"):
            st.session_state.state.selected_filters = None
            self.rerun_comparison()

    def _display_data_table(self, df: pd.DataFrame) -> None:
        st.header("Discrepancy Details")
        st.dataframe(df)

    def _setup_export(self, results: ComparisonResult) -> None:
        if not results.discrepancies.empty:
            export_format = self._get_export_format()
            filename = self._generate_filename(export_format)
            export_data = self._prepare_export_data(results.discrepancies, export_format)
            
            st.download_button(
                label="📥 Download Report",
                data=export_data,
                file_name=filename,
                mime=self._get_mime_type(export_format)
            )

    def _get_export_format(self) -> str:
        job_response = st.session_state.state.job_response
        return job_response.get("report", {}).get("format", "CSV").upper()

    def _generate_filename(self, format: str) -> str:
        job_response = st.session_state.state.job_response
        baseline_env = job_response["baseline"]["env"]
        candidate_env = job_response["candidate"]["env"]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return f"discrepancy_report_{baseline_env}_{candidate_env}_{timestamp}.{format.lower()}"

    def _prepare_export_data(self, df: pd.DataFrame, format: str) -> bytes:
        if format == "CSV":
            return df.to_csv(index=False).encode("utf-8")
        elif format == "EXCEL":
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            return output.getvalue()
        elif format == "JSON":
            return df.to_json(orient="records", indent=4).encode("utf-8")
        return df.to_csv(index=False).encode("utf-8")

    def _get_mime_type(self, format: str) -> str:
        mime_types = {
            "CSV": "text/csv",
            "EXCEL": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "JSON": "application/json"
        }
        return mime_types.get(format, "text/csv")

    def rerun_comparison(self) -> None:
        if hasattr(st.session_state.state, 'comparison_results'):
            st.rerun()

if __name__ == "__main__":
    dashboard = DiscrepancyDashboard()
    dashboard.run()