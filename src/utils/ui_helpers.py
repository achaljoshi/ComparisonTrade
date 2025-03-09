import streamlit as st
import plotly.express as px
import plotly.colors
import pandas as pd
from typing import Dict, List, Any, Tuple

class UIHelper:
    """Helper class to manage Streamlit UI components and visualizations."""
    
    @staticmethod
    def setup_page_config():
        """Configure the Streamlit page settings."""
        # Page config is now handled in app.py
        pass
    
    @staticmethod
    def initialize_session_state():
        """Initialize all required session state variables."""
        defaults = {
            "screen": "upload_config",
            "config_files": {
                "directory_config": None,
                "job_response": None,
                "rules_config": None
            },
            "selected_filters": {},
            "filtered_results": pd.DataFrame(),
            "file_type": None,
            "uploaded_file_baseline": None,
            "uploaded_file_candidate": None,
            "results": None,
            "processor": None
        }
        
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    @staticmethod
    def render_filter_ui(filter_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Render filter UI components in the sidebar.
        
        Args:
            filter_metadata: Filter metadata from DataProcessor
            
        Returns:
            Dict containing updated filter values
        """
        st.sidebar.header("🔍 Filter Rules")
        selected_filters = st.session_state.get("selected_filters", {})
        
        # Initialize filter structure if needed
        if "columns" not in selected_filters:
            selected_filters["columns"] = {}
        if "tick_sizes" not in selected_filters:
            selected_filters["tick_sizes"] = {}
        if "array_fields" not in selected_filters:
            selected_filters["array_fields"] = {}
        
        # Track fields that have been processed to avoid duplicates
        processed_fields = set()
        
        # Process normal column rules
        for column_name, rules in filter_metadata.get("columns", {}).items():
            if column_name not in selected_filters["columns"]:
                selected_filters["columns"][column_name] = {}
            
            st.sidebar.subheader(f"📊 {column_name}")
            for rule in rules:
                selected_filters = UIHelper._render_column_rule_filter(
                    column_name, rule, selected_filters
                )
            
            # Mark this field as processed
            processed_fields.add(column_name)
        
        # Process array fields if present
        for field_name, field_data in filter_metadata.get("array_fields", {}).items():
            # Always process array fields - they take precedence over other types
            st.sidebar.subheader(f"📑 {field_name} (Array)")
            selected_filters = UIHelper._render_array_field_filter(
                field_name, field_data, selected_filters
            )
        
        return selected_filters

    @staticmethod
    def _render_column_rule_filter(column_name: str, rule: Dict[str, Any], selected_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Render UI components for a column rule filter."""
        # Make all fields optional with appropriate fallbacks
        rule_number = rule.get("rule_number", rule.get("rulenumber", "N/A"))
        rule_type = rule.get("rule_type", rule.get("type", "Unknown"))
        category = rule.get("category", rule_type)
        constraints = rule.get("constraints", {})
        valid_values = rule.get("valid_values", [])
        
        if rule_number not in selected_filters["columns"][column_name]:
            selected_filters["columns"][column_name][rule_number] = {}
        
        current_values = selected_filters["columns"][column_name][rule_number]
        
        st.sidebar.markdown(f"**Rule {rule_number}** - {category}")
        
        if valid_values:
            # Handle predefined values
            selected_value = st.sidebar.selectbox(
                f"Select value for {column_name}",
                options=valid_values,
                key=f"{column_name}_{rule_number}_value"
            )
            selected_filters["columns"][column_name][rule_number]["value"] = selected_value
        else:
            # Handle numeric constraints
            min_constraint = constraints.get("min")
            max_constraint = constraints.get("max")
            
            if min_constraint is not None:
                new_min = st.sidebar.number_input(
                    f"Min Value ({rule_number})",
                    value=float(current_values.get("min", min_constraint)),
                    key=f"{column_name}_{rule_number}_min"
                )
                selected_filters["columns"][column_name][rule_number]["min"] = new_min
                
            if max_constraint is not None:
                new_max = st.sidebar.number_input(
                    f"Max Value ({rule_number})",
                    value=float(current_values.get("max", max_constraint)),
                    key=f"{column_name}_{rule_number}_max"
                )
                selected_filters["columns"][column_name][rule_number]["max"] = new_max
        
        selected_filters["columns"][column_name][rule_number].update({
            "format_type": rule.get("format_type", ""),
            "rule_type": rule_type
        })
        
        return selected_filters

    @staticmethod
    def _render_tick_size_filter(rule: Dict[str, Any], selected_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Render UI components for a tick size filter."""
        # Make rule_number optional with appropriate fallback
        rule_number = rule.get("rule_number", rule.get("rulenumber", "N/A"))
        constraints = rule.get("constraints", {})
        nested_identifier = rule.get("nested_identifier", [])
        
        if rule_number not in selected_filters["tick_sizes"]:
            selected_filters["tick_sizes"][rule_number] = {}
        
        current_values = selected_filters["tick_sizes"][rule_number]
        
        st.sidebar.markdown(f"**Tick Size Rule {rule_number}**")
        
        # Handle nested structure for tick sizes
        for idx, identifier in enumerate(nested_identifier):
            current_value = current_values.get(identifier, constraints.get(identifier, 0))
            # Create a unique key using the index
            unique_key = f"tick_size_{rule_number}_{identifier}_{idx}"
            
            new_value = st.sidebar.number_input(
                f"{identifier} ({rule_number})",
                value=float(current_value),
                key=unique_key
            )
            selected_filters["tick_sizes"][rule_number][identifier] = new_value
        
        selected_filters["tick_sizes"][rule_number].update({
            "format_type": rule.get("format_type", ""),
            "rule_type": rule.get("rule_type", ""),
            "columns": rule.get("columns", [])
        })
        
        return selected_filters

    @staticmethod
    def _render_array_field_filter(field_name: str, field_data: Dict[str, Any], selected_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Render UI components for an array field filter."""
        # Initialize array field structure if needed
        if field_name not in selected_filters["array_fields"]:
            selected_filters["array_fields"][field_name] = {
                "nested_identifier": field_data.get("nested_identifier", []),
                "rules": []
            }
        
        # Handle case where rules might be missing
        rules = field_data.get("rules", [])
        if not isinstance(rules, list):
            rules = []
        
        for rule_idx, rule in enumerate(rules):
            # Make all fields optional with appropriate fallbacks
            rule_number = rule.get("rule_number", rule.get("rulenumber", "N/A"))
            rule_type = rule.get("rule_type", rule.get("type", "Unknown"))
            constraints = rule.get("constraints", {})
            
            st.sidebar.markdown(f"**Array Rule {rule_number}**")
            
            rule_data = {
                "rule_number": rule_number,
                "rule_type": rule_type
            }
            
            # Handle constraints
            for constraint_idx, (constraint_key, constraint_value) in enumerate(constraints.items()):
                # Find current value with proper error handling
                try:
                    current_value = next(
                        (r.get(constraint_key, constraint_value) 
                         for r in selected_filters["array_fields"][field_name]["rules"]
                         if r.get("rule_number") == rule_number),
                        constraint_value
                    )
                except (TypeError, AttributeError):
                    current_value = constraint_value
                
                # Convert to float with error handling
                try:
                    current_value = float(current_value)
                except (ValueError, TypeError):
                    current_value = 0.0
                
                # Create a unique key using all relevant identifiers
                unique_key = f"{field_name}_{rule_number}_{constraint_key}_{rule_idx}_{constraint_idx}"
                
                new_value = st.sidebar.number_input(
                    f"{constraint_key} ({rule_number})",
                    value=current_value,
                    key=unique_key
                )
                rule_data[constraint_key] = new_value
            
            # Update or append rule with proper error handling
            try:
                rule_index = next(
                    (i for i, r in enumerate(selected_filters["array_fields"][field_name]["rules"])
                     if r.get("rule_number") == rule_number),
                    None
                )
                
                if rule_index is not None:
                    selected_filters["array_fields"][field_name]["rules"][rule_index].update(rule_data)
                else:
                    selected_filters["array_fields"][field_name]["rules"].append(rule_data)
            except Exception as e:
                st.warning(f"Error updating rule {rule_number}: {str(e)}")
        
        return selected_filters

    @staticmethod
    def render_results(filtered_results: pd.DataFrame):
        """Render visualization components for the results."""
        if filtered_results.empty:
            return
            
        st.header("📊 Key Performance Indicators")
        
        # Process results data
        filtered_results["Classification"] = filtered_results["Classification"].str.upper()
        category_counts = filtered_results["Classification"].value_counts().to_dict()
        missing_counts = UIHelper._get_missing_counts(filtered_results)
        
        # Render KPI metrics
        UIHelper._render_kpi_metrics(filtered_results, category_counts, missing_counts)
        
        # Render charts
        UIHelper._render_discrepancy_charts(filtered_results)
        
        # Render details table
        st.header("Discrepancy Details")
        st.dataframe(filtered_results)

    @staticmethod
    def _get_missing_counts(filtered_results: pd.DataFrame) -> Tuple[int, int]:
        """Calculate missing row counts."""
        missing_baseline = (filtered_results["Rule Type"] == "Missing in Baseline").sum()
        missing_candidate = (filtered_results["Rule Type"] == "Missing in Candidate").sum()
        return missing_baseline, missing_candidate

    @staticmethod
    def _render_kpi_metrics(
        filtered_results: pd.DataFrame,
        category_counts: Dict[str, int],
        missing_counts: Tuple[int, int]
    ):
        """Render KPI metrics in columns."""
        total_metrics = len(category_counts) + 3
        kpi_columns = st.columns(min(total_metrics, 4))
        
        i = 0
        kpi_columns[i % len(kpi_columns)].metric("🔍 Total Discrepancies", len(filtered_results))
        i += 1
        
        for category, count in category_counts.items():
            kpi_columns[i % len(kpi_columns)].metric(f"{category}", count)
            i += 1
        
        missing_baseline, missing_candidate = missing_counts
        kpi_columns[i % len(kpi_columns)].metric("Missing Rows in Baseline", missing_baseline)
        i += 1
        kpi_columns[i % len(kpi_columns)].metric("Missing Rows in Candidate", missing_candidate)

    @staticmethod
    def _render_discrepancy_charts(filtered_results: pd.DataFrame):
        """Render bar chart and pie chart for discrepancies."""
        # Generate color map
        unique_categories = filtered_results["Classification"].unique()
        color_palette = plotly.colors.qualitative.Set1
        color_map = {
            category: color_palette[i % len(color_palette)] 
            for i, category in enumerate(unique_categories)
        }
        
        # Bar chart
        st.header("📊 Discrepancy Analysis")
        discrepancy_counts = filtered_results.groupby(
            ["Column Name", "Classification"]
        ).size().reset_index(name="Count")
        
        bar_chart = px.bar(
            discrepancy_counts,
            x="Column Name",
            y="Count",
            color="Classification",
            title="Discrepancies by Column",
            barmode="group",
            color_discrete_map=color_map
        )
        st.plotly_chart(bar_chart, use_container_width=True)
        
        # Pie chart
        st.header("Discrepancy Distribution")
        pie_chart = px.pie(
            filtered_results,
            names="Classification",
            title="Proportion of Discrepancy Types",
            hole=0.4
        )
        st.plotly_chart(pie_chart, use_container_width=True) 