from typing import Dict, List, Optional
import json
from pathlib import Path
import plotly.colors as pc
import streamlit as st

class ColorSchemeManager:
    DEFAULT_SCHEMES = {
        'Default': {
            'primary': ['#FF9999', '#66B2FF', '#99FF99'],
            'secondary': ['#FFCC99', '#99CCFF', '#FFFF99'],
            'background': '#FFFFFF',
            'text': '#000000',
            'grid': '#EEEEEE'
        },
        'Dark': {
            'primary': ['#FF6B6B', '#4ECDC4', '#45B7D1'],
            'secondary': ['#96CEB4', '#FFEEAD', '#D4A5A5'],
            'background': '#2B2B2B',
            'text': '#FFFFFF',
            'grid': '#3D3D3D'
        },
        'Pastel': {
            'primary': ['#FFB3BA', '#BAFFC9', '#BAE1FF'],
            'secondary': ['#FFFFBA', '#FFB3F7', '#B3FFF7'],
            'background': '#FFFFFF',
            'text': '#666666',
            'grid': '#F5F5F5'
        }
    }
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("config/color_schemes.json")
        self.schemes = self.DEFAULT_SCHEMES.copy()
        self.load_schemes()
    
    def load_schemes(self) -> None:
        """Load custom color schemes from config file"""
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                custom_schemes = json.load(f)
                self.schemes.update(custom_schemes)
    
    def save_schemes(self) -> None:
        """Save custom color schemes to config file"""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w') as f:
            json.dump(self.schemes, f, indent=2)
    
    def add_scheme(self, name: str, colors: Dict[str, List[str]]) -> None:
        """Add a new color scheme"""
        self.schemes[name] = colors
        self.save_schemes()
    
    def remove_scheme(self, name: str) -> None:
        """Remove a color scheme"""
        if name in self.schemes and name not in self.DEFAULT_SCHEMES:
            del self.schemes[name]
            self.save_schemes()
    
    def get_scheme(self, name: str) -> Dict[str, List[str]]:
        """Get a color scheme by name"""
        return self.schemes.get(name, self.DEFAULT_SCHEMES['Default'])
    
    def get_scheme_names(self) -> List[str]:
        """Get list of available color scheme names"""
        return list(self.schemes.keys())
    
    @staticmethod
    def generate_color_sequence(n: int, scheme: Dict[str, List[str]]) -> List[str]:
        """Generate a sequence of n colors from a scheme"""
        primary = scheme['primary']
        secondary = scheme['secondary']
        all_colors = primary + secondary
        
        if n <= len(all_colors):
            return all_colors[:n]
        
        # If more colors needed, generate additional ones
        base = all_colors[0]
        return pc.n_colors(base, all_colors[-1], n, colortype='rgb') 