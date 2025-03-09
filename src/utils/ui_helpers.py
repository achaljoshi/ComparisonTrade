import streamlit as st
import plotly.express as px
import plotly.colors
import pandas as pd
from typing import Dict, List, Any, Tuple
from io import BytesIO

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
            "processor": None,
            "search_query": ""  # Initialize search_query with empty string
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
        if "array_fields" not in selected_filters:
            selected_filters["array_fields"] = {}
            
        # Initialize expanded state in session state if not present
        if "expanded_sections" not in st.session_state:
            st.session_state.expanded_sections = {}
            
        # Add search functionality with clear button
        search_query = st.sidebar.text_input(
            "Search by column or rule",
            value=st.session_state.search_query,
            key="filter_search",
            placeholder="Search..."
        )
        
        # Clear button below search field
        if st.sidebar.button(
            "Clear Search",
            key="clear_search",
            help="Clear search field",
            type="secondary",
            use_container_width=True
        ):
            search_query = ""
            st.session_state.search_query = ""
            
        # Update session state
        st.session_state.search_query = search_query
        search_query = search_query.lower()
        
        # First, collect all array field names to avoid duplicates
        array_field_names = set(filter_metadata.get("array_fields", {}).keys())
        
        # Group rules by column name
        column_rules = {}
        for column_name, rules in filter_metadata.get("columns", {}).items():
            # Skip if this column is already handled as an array field
            if column_name in array_field_names:
                continue
                
            # Filter out rules with format_type "Array" or "Object"
            non_array_rules = [rule for rule in rules if rule.get("format_type") not in ["Array", "Object"]]
            
            # Only include columns that match the search query
            if search_query and search_query not in column_name.lower() and not any(
                search_query in rule.get("rulenumber", "").lower() for rule in non_array_rules
            ):
                continue
                
            column_rules[column_name] = non_array_rules
        
        # Add a separator
        st.sidebar.markdown("---")
        
        # Process normal column rules
        for column_name, rules in column_rules.items():
            if column_name not in selected_filters["columns"]:
                selected_filters["columns"][column_name] = {}
                
            # Initialize expanded state for this column if not present
            section_key = f"column_{column_name}"
            if section_key not in st.session_state.expanded_sections:
                st.session_state.expanded_sections[section_key] = False
                
            # Create a collapsible section using expander
            with st.sidebar.expander(f"📊 {column_name}", expanded=st.session_state.expanded_sections[section_key]):
                # Set expanded state
                st.session_state.expanded_sections[section_key] = True
                
                # Process each rule
                for i, rule in enumerate(rules):
                    # Skip rules that don't match the search query
                    rule_number = rule.get("rulenumber", "N/A")
                    if search_query and search_query not in column_name.lower() and search_query not in rule_number.lower():
                        continue
                    
                    # Add a separator between rules (except for the first rule)
                    if i > 0:
                        st.sidebar.markdown("<hr style='margin: 10px 0; border: none; border-top: 1px solid #e6e9ef;'>", unsafe_allow_html=True)
                        
                    # Render the rule
                    selected_filters = UIHelper._render_unified_rule_filter(
                        column_name, rule, selected_filters
                    )
        
        # Process array fields if present
        array_fields = {}
        for field_name, field_data in filter_metadata.get("array_fields", {}).items():
            # Only include array fields that match the search query
            if search_query and search_query not in field_name.lower() and not any(
                search_query in rule.get("rule_number", "").lower() for rule in field_data.get("rules", [])
            ):
                continue
                
            array_fields[field_name] = field_data
            
        for field_name, field_data in array_fields.items():
            # Initialize expanded state for this array field if not present
            section_key = f"array_{field_name}"
            if section_key not in st.session_state.expanded_sections:
                st.session_state.expanded_sections[section_key] = False
                
            # Create a collapsible section using expander
            with st.sidebar.expander(f"📑 {field_name} (Array)", expanded=st.session_state.expanded_sections[section_key]):
                # Set expanded state
                st.session_state.expanded_sections[section_key] = True
                
                # Render array field rules
                selected_filters = UIHelper._render_array_field_filter(
                    field_name, field_data, selected_filters
                )
        
        return selected_filters

    @staticmethod
    def _render_unified_rule_filter(column_name: str, rule: Dict[str, Any], selected_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Unified method to render UI components for column rules."""
        # Make all fields optional with appropriate fallbacks
        rule_number = rule.get("rule_number", rule.get("rulenumber", "N/A"))
        rule_type = rule.get("rule_type", rule.get("type", "Unknown"))
        category = rule.get("category", rule_type)
        constraints = rule.get("constraints", {})
        valid_values = rule.get("valid_values", [])
        format_type = rule.get("format_type", "")
        columns = rule.get("columns", [])
        description = rule.get("description", "")
        
        # Initialize the rule in selected_filters if it doesn't exist
        if rule_number not in selected_filters["columns"][column_name]:
            selected_filters["columns"][column_name][rule_number] = {}
        
        current_values = selected_filters["columns"][column_name][rule_number]
        
        # Create a better rule header with column name, rule number and description
        st.sidebar.markdown(f"**{column_name} - Rule {rule_number}**")
        
        # Display description if available
        if description:
            st.sidebar.markdown(f"<small><i>{description}</i></small>", unsafe_allow_html=True)
        
        # Handle different rule types
        if valid_values:
            # Handle predefined values
            st.sidebar.markdown("**Valid Values:**")
            selected_value = st.sidebar.selectbox(
                f"Select value",
                options=valid_values,
                key=f"{column_name}_{rule_number}_value"
            )
            selected_filters["columns"][column_name][rule_number]["value"] = selected_value
        else:
            # Handle numeric constraints
            min_constraint = constraints.get("min")
            max_constraint = constraints.get("max")
            
            if min_constraint is not None or max_constraint is not None:
                st.sidebar.markdown("**Constraints:**")
                
            if min_constraint is not None:
                new_min = st.sidebar.number_input(
                    f"Min Value",
                    value=float(current_values.get("min", min_constraint)),
                    key=f"{column_name}_{rule_number}_min"
                )
                selected_filters["columns"][column_name][rule_number]["min"] = new_min
                
            if max_constraint is not None:
                new_max = st.sidebar.number_input(
                    f"Max Value",
                    value=float(current_values.get("max", max_constraint)),
                    key=f"{column_name}_{rule_number}_max"
                )
                selected_filters["columns"][column_name][rule_number]["max"] = new_max
                
            # Handle ignorecase constraint for string fields
            if constraints.get("ignorecase") is not None:
                # Get the original constraint value
                original_ignorecase = constraints.get("ignorecase", "no").lower() == "yes"
                # Get the current value from selected filters, defaulting to original constraint value
                current_ignorecase = current_values.get("ignorecase", original_ignorecase)
                # Convert to boolean if it's a string
                if isinstance(current_ignorecase, str):
                    current_ignorecase = current_ignorecase.lower() == "yes"
                
                ignore_case = st.sidebar.checkbox(
                    f"Ignore case",
                    value=current_ignorecase,
                    key=f"{column_name}_{rule_number}_ignorecase"
                )
                selected_filters["columns"][column_name][rule_number]["ignorecase"] = "yes" if ignore_case else "no"
        
        # Store additional metadata
        selected_filters["columns"][column_name][rule_number].update({
            "format_type": format_type,
            "rule_type": rule_type
        })
        
        return selected_filters

    @staticmethod
    def _render_array_field_filter(field_name: str, field_data: Dict[str, Any], selected_filters: Dict[str, Any]) -> Dict[str, Any]:
        """Render UI components for an array field filter."""
        # Initialize array field structure if needed
        if field_name not in selected_filters["array_fields"]:
            selected_filters["array_fields"][field_name] = {
                "columns": field_data.get("columns", []),
                "rules": [],
                "format_type": field_data.get("format_type", "")
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
            description = rule.get("description", "")
            
            # Create a better rule header with field name, rule number and description
            st.sidebar.markdown(f"**{field_name} - Rule {rule_number}**")
            
            # Display description if available
            if description:
                st.sidebar.markdown(f"<small><i>{description}</i></small>", unsafe_allow_html=True)
            
            rule_data = {
                "rule_number": rule_number,
                "rule_type": rule_type
            }
            
            # Handle constraints
            if constraints:
                st.sidebar.markdown("**Constraints:**")
                
            for constraint_key, constraint_value in constraints.items():
                # Skip non-standard constraints
                if constraint_key not in ["min", "max", "ignorecase"]:
                    continue
                    
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
                
                # Handle different constraint types
                if constraint_key in ["min", "max"]:
                    # Convert to float with error handling
                    try:
                        current_value = float(current_value)
                    except (ValueError, TypeError):
                        current_value = 0.0 if constraint_key == "min" else float("inf")
                    
                    # Create a unique key
                    unique_key = f"{field_name}_{rule_number}_{constraint_key}_{rule_idx}"
                    
                    new_value = st.sidebar.number_input(
                        f"{constraint_key.capitalize()} Value",
                        value=current_value,
                        key=unique_key
                    )
                    rule_data[constraint_key] = new_value
                elif constraint_key == "ignorecase":
                    # Handle ignorecase as a checkbox
                    unique_key = f"{field_name}_{rule_number}_{constraint_key}_{rule_idx}"
                    
                    ignore_case = st.sidebar.checkbox(
                        f"Ignore case",
                        value=current_value.lower() == "yes" if isinstance(current_value, str) else bool(current_value),
                        key=unique_key
                    )
                    rule_data[constraint_key] = "yes" if ignore_case else "no"
            
            # Add a small separator between rules
            st.sidebar.markdown("<hr style='margin: 5px 0; border: none; border-top: 1px solid #e6e9ef;'>", unsafe_allow_html=True)
            
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
        
        # Render details table and download button
        st.header("Discrepancy Details")
        
        # Get report format from job_response
        report_format = st.session_state.get("config_files", {}).get("uploaded_configs", {}).get("job_response", {}).get("report", {}).get("format", "CSV")
        
        # Add download button with proper format
        if report_format.upper() == "JSON":
            json_data = filtered_results.to_json(orient="records", indent=4)
            st.download_button(
                label="📥 Download Discrepancy Report (JSON)",
                data=json_data,
                file_name="discrepancy_report.json",
                mime="application/json",
                help="Download the discrepancy report in JSON format"
            )
        elif report_format.upper() == "CSV":
            csv_data = filtered_results.to_csv(index=False)
            st.download_button(
                label="📥 Download Discrepancy Report (CSV)",
                data=csv_data,
                file_name="discrepancy_report.csv",
                mime="text/csv",
                help="Download the discrepancy report in CSV format"
            )
        elif report_format.upper() == "EXCEL":
            buffer = BytesIO()
            filtered_results.to_excel(buffer, index=False)
            st.download_button(
                label="📥 Download Discrepancy Report (Excel)",
                data=buffer.getvalue(),
                file_name="discrepancy_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Download the discrepancy report in Excel format"
            )
            
        # Display the dataframe
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