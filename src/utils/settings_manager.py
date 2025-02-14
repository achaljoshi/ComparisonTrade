from typing import Dict, Optional
import json
from pathlib import Path
import datetime

class ComparisonSettings:
    def __init__(self):
        self.name: str = ""
        self.key_columns: list = []
        self.selected_columns: list = []
        self.validation_rules: Dict = {}
        self.color_scheme: str = "Default"
        self.created_at: str = ""
        self.last_modified: str = ""
        
    def to_dict(self) -> Dict:
        """Convert settings to dictionary"""
        return {
            'name': self.name,
            'key_columns': self.key_columns,
            'selected_columns': self.selected_columns,
            'validation_rules': self.validation_rules,
            'color_scheme': self.color_scheme,
            'created_at': self.created_at,
            'last_modified': self.last_modified
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ComparisonSettings':
        """Create settings from dictionary"""
        settings = cls()
        settings.name = data.get('name', '')
        settings.key_columns = data.get('key_columns', [])
        settings.selected_columns = data.get('selected_columns', [])
        settings.validation_rules = data.get('validation_rules', {})
        settings.color_scheme = data.get('color_scheme', 'Default')
        settings.created_at = data.get('created_at', '')
        settings.last_modified = data.get('last_modified', '')
        return settings

class SettingsManager:
    def __init__(self, config_dir: Optional[Path] = None):
        self.config_dir = config_dir or Path("config")
        self.settings_file = self.config_dir / "comparison_settings.json"
        self.settings: Dict[str, ComparisonSettings] = {}
        self.load_settings()
    
    def load_settings(self) -> None:
        """Load saved settings from file"""
        if self.settings_file.exists():
            try:
                with open(self.settings_file, 'r') as f:
                    data = json.load(f)
                    self.settings = {
                        name: ComparisonSettings.from_dict(settings_data)
                        for name, settings_data in data.items()
                    }
            except Exception as e:
                print(f"Error loading settings: {str(e)}")
    
    def save_settings(self) -> None:
        """Save settings to file"""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with open(self.settings_file, 'w') as f:
            data = {
                name: settings.to_dict()
                for name, settings in self.settings.items()
            }
            json.dump(data, f, indent=2)
    
    def add_settings(self, settings: ComparisonSettings) -> None:
        """Add new settings"""
        if not settings.name:
            raise ValueError("Settings must have a name")
            
        now = datetime.datetime.now().isoformat()
        if not settings.created_at:
            settings.created_at = now
        settings.last_modified = now
        
        self.settings[settings.name] = settings
        self.save_settings()
    
    def get_settings(self, name: str) -> Optional[ComparisonSettings]:
        """Get settings by name"""
        return self.settings.get(name)
    
    def list_settings(self) -> Dict[str, Dict]:
        """List all saved settings with metadata"""
        return {
            name: {
                'created_at': settings.created_at,
                'last_modified': settings.last_modified,
                'key_columns': len(settings.key_columns),
                'selected_columns': len(settings.selected_columns),
                'validation_rules': len(settings.validation_rules)
            }
            for name, settings in self.settings.items()
        }
    
    def delete_settings(self, name: str) -> None:
        """Delete settings by name"""
        if name in self.settings:
            del self.settings[name]
            self.save_settings() 