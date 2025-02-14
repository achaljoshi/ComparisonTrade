import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import base64
from io import BytesIO
import json
from plotly.utils import PlotlyJSONEncoder
from .color_manager import ColorSchemeManager

class Visualizer:
    def __init__(self, color_scheme: Optional[str] = None):
        self.color_manager = ColorSchemeManager()
        self.current_scheme = color_scheme or 'Default'
    
    def set_color_scheme(self, scheme_name: str) -> None:
        """Set the current color scheme"""
        self.current_scheme = scheme_name
    
    def create_summary_chart(self, discrepancies: Dict) -> go.Figure:
        """Create summary visualization of discrepancies with custom colors"""
        summary = discrepancies['summary']
        scheme = self.color_manager.get_scheme(self.current_scheme)
        
        categories = ['Value Mismatches', 'Missing in File 1', 'Missing in File 2']
        values = [
            summary['value_mismatches'],
            summary['missing_in_df1'],
            summary['missing_in_df2']
        ]
        
        fig = go.Figure(data=[
            go.Bar(
                x=categories,
                y=values,
                marker_color=scheme['primary'][:3],
                text=values,
                textposition='auto',
            )
        ])
        
        fig.update_layout(
            title='Discrepancy Summary',
            xaxis_title='Discrepancy Type',
            yaxis_title='Count',
            template='plotly_white',
            height=400,
            plot_bgcolor=scheme['background'],
            paper_bgcolor=scheme['background'],
            font={'color': scheme['text']},
            xaxis={'gridcolor': scheme['grid']},
            yaxis={'gridcolor': scheme['grid']}
        )
        
        return fig
    
    @staticmethod
    def create_mismatch_heatmap(discrepancies: Dict) -> go.Figure:
        """Create heatmap of value mismatches by column"""
        if not discrepancies['value_mismatches']:
            return None
            
        # Convert mismatches to DataFrame
        df_mismatches = pd.DataFrame(discrepancies['value_mismatches'])
        mismatch_counts = df_mismatches['column'].value_counts()
        
        fig = go.Figure(data=[
            go.Bar(
                x=mismatch_counts.index,
                y=mismatch_counts.values,
                marker_color='#FF9999',
                text=mismatch_counts.values,
                textposition='auto',
            )
        ])
        
        fig.update_layout(
            title='Mismatches by Column',
            xaxis_title='Column Name',
            yaxis_title='Number of Mismatches',
            template='plotly_white',
            height=400
        )
        
        return fig
    
    @staticmethod
    def create_comparison_table(df1: pd.DataFrame, df2: pd.DataFrame, discrepancies: Dict) -> pd.DataFrame:
        """Create an interactive comparison table highlighting differences"""
        if not discrepancies['value_mismatches']:
            return pd.DataFrame()
            
        # Create comparison DataFrame
        comparison_rows = []
        
        for mismatch in discrepancies['value_mismatches']:
            comparison_rows.append({
                'Key': str(mismatch['key']),
                'Column': mismatch['column'],
                'Value in File 1': mismatch['value_df1'],
                'Value in File 2': mismatch['value_df2'],
                'Difference': str(mismatch['value_df1']) != str(mismatch['value_df2'])
            })
        
        comparison_df = pd.DataFrame(comparison_rows)
        return comparison_df
    
    @staticmethod
    def create_sankey_diagram(discrepancies: Dict) -> go.Figure:
        """Create Sankey diagram showing data flow and discrepancies"""
        total_rows_df1 = discrepancies['summary']['total_rows_df1']
        total_rows_df2 = discrepancies['summary']['total_rows_df2']
        missing_in_df2 = discrepancies['summary']['missing_in_df2']
        missing_in_df1 = discrepancies['summary']['missing_in_df1']
        value_mismatches = discrepancies['summary']['value_mismatches']
        
        # Calculate matching rows
        matching_rows = total_rows_df1 - missing_in_df2 - value_mismatches
        
        # Define nodes and links
        nodes = ['File 1', 'Matching', 'Mismatches', 'Missing', 'File 2']
        
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=nodes,
                color=["#66B2FF", "#99FF99", "#FF9999", "#FFCC99", "#66B2FF"]
            ),
            link=dict(
                source=[0, 0, 0, 4],  # indices correspond to nodes
                target=[1, 2, 3, 1],
                value=[matching_rows, value_mismatches, missing_in_df2, missing_in_df1],
                color=["#99FF99", "#FF9999", "#FFCC99", "#99FF99"]
            )
        )])
        
        fig.update_layout(
            title='Data Flow Visualization',
            font_size=12,
            height=500
        )
        
        return fig 

    @staticmethod
    def get_column_statistics(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """Generate column-wise statistics comparing both dataframes"""
        stats = []
        for col in df1.columns:
            if col in df2.columns:
                stats.append({
                    'Column': col,
                    'Unique Values (File 1)': df1[col].nunique(),
                    'Unique Values (File 2)': df2[col].nunique(),
                    'Null Count (File 1)': df1[col].isnull().sum(),
                    'Null Count (File 2)': df2[col].isnull().sum(),
                    'Type Match': df1[col].dtype == df2[col].dtype,
                    'Type (File 1)': str(df1[col].dtype),
                    'Type (File 2)': str(df2[col].dtype)
                })
        return pd.DataFrame(stats) 

    def export_figure_as_json(self, fig: go.Figure) -> str:
        """Export Plotly figure as JSON for later reconstruction"""
        return json.dumps(fig, cls=PlotlyJSONEncoder)
    
    def export_figure_as_html(self, fig: go.Figure) -> str:
        """Export Plotly figure as standalone HTML"""
        return fig.to_html(include_plotlyjs=True, full_html=True)
    
    def export_figure_as_image(self, fig: go.Figure, format: str = 'png') -> bytes:
        """Export Plotly figure as image bytes"""
        img_bytes = fig.to_image(format=format, scale=2.0)
        return img_bytes
    
    def create_pie_chart(self, discrepancies: Dict) -> go.Figure:
        """Create pie chart visualization of discrepancies"""
        summary = discrepancies['summary']
        scheme = self.color_manager.get_scheme(self.current_scheme)
        
        values = [
            summary['value_mismatches'],
            summary['missing_in_df1'],
            summary['missing_in_df2'],
            summary['total_rows_df1'] - summary['value_mismatches'] - summary['missing_in_df2']
        ]
        
        labels = [
            'Value Mismatches',
            'Missing in File 1',
            'Missing in File 2',
            'Matching Records'
        ]
        
        fig = go.Figure(data=[
            go.Pie(
                labels=labels,
                values=values,
                marker_colors=scheme['primary'],
                textinfo='label+percent',
                hole=0.3
            )
        ])
        
        fig.update_layout(
            title='Discrepancy Distribution',
            template='plotly_white',
            height=500,
            plot_bgcolor=scheme['background'],
            paper_bgcolor=scheme['background'],
            font={'color': scheme['text']}
        )
        
        return fig
    
    def create_treemap(self, discrepancies: Dict) -> go.Figure:
        """Create treemap visualization of discrepancies by column"""
        if not discrepancies['value_mismatches']:
            return None
            
        df_mismatches = pd.DataFrame(discrepancies['value_mismatches'])
        column_counts = df_mismatches['column'].value_counts().reset_index()
        column_counts.columns = ['Column', 'Count']
        
        # Add total rows for context
        total_rows = discrepancies['summary']['total_rows_df1']
        column_counts['Total'] = total_rows
        column_counts['Match Rate'] = ((total_rows - column_counts['Count']) / total_rows * 100).round(2)
        
        scheme = self.color_manager.get_scheme(self.current_scheme)
        
        fig = px.treemap(
            column_counts,
            path=['Column'],
            values='Count',
            color='Match Rate',
            color_continuous_scale=[scheme['primary'][0], scheme['primary'][2]],
            custom_data=['Match Rate']
        )
        
        fig.update_traces(
            textinfo='label+value',
            hovertemplate=(
                'Column: %{label}<br>'
                'Mismatches: %{value}<br>'
                'Match Rate: %{customdata[0]:.1f}%<br>'
                '<extra></extra>'
            )
        )
        
        fig.update_layout(
            title='Discrepancy Distribution by Column (Treemap)',
            template='plotly_white',
            height=600,
            plot_bgcolor=scheme['background'],
            paper_bgcolor=scheme['background'],
            font={'color': scheme['text']}
        )
        
        return fig
    
    def create_sunburst(self, discrepancies: Dict) -> go.Figure:
        """Create sunburst visualization of discrepancies"""
        if not discrepancies['value_mismatches']:
            return None
            
        df_mismatches = pd.DataFrame(discrepancies['value_mismatches'])
        
        # Prepare data for sunburst
        data = []
        for col in df_mismatches['column'].unique():
            col_mismatches = df_mismatches[df_mismatches['column'] == col]
            data.extend([
                {'Column': col, 'Type': 'Mismatches', 'Count': len(col_mismatches)},
                {'Column': col, 'Type': 'Matches', 'Count': discrepancies['summary']['total_rows_df1'] - len(col_mismatches)}
            ])
        
        df_sunburst = pd.DataFrame(data)
        scheme = self.color_manager.get_scheme(self.current_scheme)
        
        fig = px.sunburst(
            df_sunburst,
            path=['Column', 'Type'],
            values='Count',
            color='Type',
            color_discrete_map={
                'Matches': scheme['primary'][2],
                'Mismatches': scheme['primary'][0]
            }
        )
        
        fig.update_layout(
            title='Hierarchical View of Discrepancies',
            template='plotly_white',
            height=600,
            plot_bgcolor=scheme['background'],
            paper_bgcolor=scheme['background'],
            font={'color': scheme['text']}
        )
        
        return fig
    
    def get_all_visualizations(self, discrepancies: Dict) -> Dict[str, go.Figure]:
        """Get all available visualizations for the dataset"""
        visualizations = {
            'summary_chart': self.create_summary_chart(discrepancies),
            'mismatch_heatmap': self.create_mismatch_heatmap(discrepancies),
            'sankey_diagram': self.create_sankey_diagram(discrepancies),
            'pie_chart': self.create_pie_chart(discrepancies),
            'treemap': self.create_treemap(discrepancies),
            'sunburst': self.create_sunburst(discrepancies)
        }
        
        # Remove None values (charts that couldn't be created)
        return {k: v for k, v in visualizations.items() if v is not None}
    
    def export_all_visualizations(self, discrepancies: Dict, format: str = 'html') -> Dict[str, bytes]:
        """Export all visualizations in specified format"""
        visualizations = self.get_all_visualizations(discrepancies)
        exports = {}
        
        for name, fig in visualizations.items():
            if format == 'html':
                exports[name] = self.export_figure_as_html(fig).encode('utf-8')
            elif format == 'json':
                exports[name] = self.export_figure_as_json(fig).encode('utf-8')
            else:  # image formats
                exports[name] = self.export_figure_as_image(fig, format)
        
        return exports
    
    def create_visualization_report(self, discrepancies: Dict, df1: pd.DataFrame, df2: pd.DataFrame) -> str:
        """Create a complete HTML report with all visualizations"""
        visualizations = self.get_all_visualizations(discrepancies)
        comparison_table = self.create_comparison_table(df1, df2, discrepancies)
        
        # Get color scheme
        scheme = self.color_manager.get_scheme(self.current_scheme)
        
        html_content = f"""
        <html>
        <head>
            <title>Discrepancy Analysis Report</title>
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    margin: 20px;
                    background-color: {scheme['background']};
                    color: {scheme['text']};
                }}
                .section {{ margin-bottom: 30px; }}
                .chart {{ margin: 20px 0; }}
                table {{ 
                    border-collapse: collapse; 
                    width: 100%;
                    background-color: {scheme['background']};
                }}
                th, td {{ 
                    border: 1px solid {scheme['grid']}; 
                    padding: 8px; 
                    text-align: left; 
                }}
                th {{ background-color: {scheme['grid']}; }}
                .mismatch {{ background-color: {scheme['primary'][0]}; }}
            </style>
        </head>
        <body>
            <h1>Discrepancy Analysis Report</h1>
            
            <div class="section">
                <h2>Summary Statistics</h2>
                <p>Total Rows in File 1: {discrepancies['summary']['total_rows_df1']}</p>
                <p>Total Rows in File 2: {discrepancies['summary']['total_rows_df2']}</p>
                <p>Total Mismatches: {discrepancies['summary']['value_mismatches']}</p>
            </div>
            
            <div class="section">
                <h2>Visualizations</h2>
                <div class="chart">
                    <h3>Summary Chart</h3>
                    {visualizations['summary_chart'].to_html(full_html=False) if 'summary_chart' in visualizations else ''}
                </div>
                
                <div class="chart">
                    <h3>Mismatch Distribution</h3>
                    {visualizations['mismatch_heatmap'].to_html(full_html=False) if 'mismatch_heatmap' in visualizations else '<p>No mismatches to display</p>'}
                </div>
                
                <div class="chart">
                    <h3>Data Flow</h3>
                    {visualizations['sankey_diagram'].to_html(full_html=False) if 'sankey_diagram' in visualizations else ''}
                </div>
                
                <div class="chart">
                    <h3>Distribution Analysis</h3>
                    {visualizations['pie_chart'].to_html(full_html=False) if 'pie_chart' in visualizations else ''}
                </div>
            </div>
            
            <div class="section">
                <h2>Detailed Comparison</h2>
                {comparison_table.to_html(index=False)}
            </div>
        </body>
        </html>
        """
        return html_content 