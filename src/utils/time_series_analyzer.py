from typing import Dict, List, Optional, Union
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

class TimeSeriesAnalyzer:
    def __init__(self):
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
            r'\d{2}-\d{2}-\d{4}'   # DD-MM-YYYY
        ]
        self.date_columns = []
        self.time_based_discrepancies = {}
        
    def identify_date_columns(self, df: pd.DataFrame) -> List[str]:
        """Identify columns that might contain dates"""
        date_columns = []
        
        for col in df.columns:
            # Check if column is already datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                date_columns.append(col)
                continue
            
            # Check if string column contains date patterns
            if df[col].dtype == 'object':
                sample = df[col].dropna().head(10).astype(str)
                for pattern in self.date_patterns:
                    if any(sample.str.match(pattern)):
                        date_columns.append(col)
                        break
        
        return date_columns
    
    def analyze_time_patterns(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                            date_column: str, discrepancies: Dict) -> Dict:
        """Analyze time-based patterns in discrepancies"""
        try:
            # Convert dates to datetime if not already
            df1[date_column] = pd.to_datetime(df1[date_column])
            df2[date_column] = pd.to_datetime(df2[date_column])
            
            # Create DataFrame of discrepancies with dates
            discrepancy_dates = []
            for mismatch in discrepancies['value_mismatches']:
                key = mismatch['key']
                if isinstance(key, tuple):
                    key = key[0]  # Take first element if composite key
                date = df1[df1[date_column].notna()][date_column].iloc[0]
                discrepancy_dates.append(date)
            
            discrepancy_df = pd.DataFrame({
                'date': discrepancy_dates
            })
            
            # Find peak periods
            peak_period = {
                'daily': discrepancy_df.groupby(discrepancy_df['date'].dt.date)['date'].count().idxmax().strftime('%Y-%m-%d'),
                'weekly': discrepancy_df.groupby(discrepancy_df['date'].dt.isocalendar().week)['date'].count().idxmax(),
                'monthly': discrepancy_df.groupby(discrepancy_df['date'].dt.month)['date'].count().idxmax(),
                'yearly': discrepancy_df.groupby(discrepancy_df['date'].dt.year)['date'].count().idxmax()
            }
            
            # Calculate trends
            trends = {}
            for period in ['daily', 'weekly', 'monthly']:
                if period == 'daily':
                    grouped = discrepancy_df.groupby(discrepancy_df['date'].dt.date)['date'].count()
                elif period == 'weekly':
                    grouped = discrepancy_df.groupby(discrepancy_df['date'].dt.isocalendar().week)['date'].count()
                else:
                    grouped = discrepancy_df.groupby(discrepancy_df['date'].dt.month)['date'].count()
                
                trend = np.polyfit(range(len(grouped)), grouped.values, 1)
                trends[period] = {
                    'direction': 'increasing' if trend[0] > 0 else 'decreasing',
                    'magnitude': abs(trend[0])
                }
            
            return {
                'peak_period': peak_period,
                'trends': trends,
                'time_series': discrepancy_df
            }
            
        except Exception as e:
            print(f"Error in time series analysis: {str(e)}")
            return None
    
    def create_time_series_plots(self, time_analysis: Dict) -> Dict[str, go.Figure]:
        """Create time series visualizations"""
        if not time_analysis:
            return {}
            
        plots = {}
        discrepancy_df = time_analysis['time_series']
        
        # Daily trend
        daily_counts = discrepancy_df.groupby(discrepancy_df['date'].dt.date)['date'].count()
        plots['daily_trend'] = go.Figure(data=[
            go.Scatter(x=daily_counts.index, y=daily_counts.values, mode='lines+markers')
        ])
        plots['daily_trend'].update_layout(
            title='Daily Discrepancy Trend',
            xaxis_title='Date',
            yaxis_title='Number of Discrepancies'
        )
        
        # Weekly trend
        weekly_counts = discrepancy_df.groupby(discrepancy_df['date'].dt.isocalendar().week)['date'].count()
        plots['weekly_trend'] = go.Figure(data=[
            go.Bar(x=weekly_counts.index, y=weekly_counts.values)
        ])
        plots['weekly_trend'].update_layout(
            title='Weekly Discrepancy Distribution',
            xaxis_title='Week Number',
            yaxis_title='Number of Discrepancies'
        )
        
        # Monthly trend
        monthly_counts = discrepancy_df.groupby(discrepancy_df['date'].dt.month)['date'].count()
        plots['monthly_trend'] = go.Figure(data=[
            go.Bar(x=monthly_counts.index, y=monthly_counts.values)
        ])
        plots['monthly_trend'].update_layout(
            title='Monthly Discrepancy Distribution',
            xaxis_title='Month',
            yaxis_title='Number of Discrepancies'
        )
        
        return plots 