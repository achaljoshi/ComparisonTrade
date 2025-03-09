import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple
from io import BytesIO
import streamlit as st

class FileHandler:
    """Handles file operations for the application."""
    
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        
    def cleanup(self):
        """Clean up temporary files and directory."""
        try:
            shutil.rmtree(self.temp_dir)
            return True, "Temporary files cleaned up successfully!"
        except Exception as e:
            return False, f"Error during cleanup: {e}"
    
    def handle_config_upload(self, missing_files: Dict[str, str]) -> Tuple[Dict[str, str], bool]:
        """Handle the upload of configuration files.
        
        Args:
            missing_files: Dictionary of missing configuration files
            
        Returns:
            Tuple containing uploaded file paths and success status
        """
        uploaded_files = {}
        
        for file_name, key in missing_files.items():
            uploaded_file = st.file_uploader(
                f"Upload `{file_name}`",
                type=["json"],
                key=key
            )
            
            if uploaded_file is not None:
                success, message = self._save_uploaded_file(uploaded_file, file_name)
                if success:
                    uploaded_files[key] = message
                else:
                    st.error(message)
                    return {}, False
        
        return uploaded_files, len(uploaded_files) == len(missing_files)
    
    def _save_uploaded_file(self, uploaded_file, file_name: str) -> Tuple[bool, str]:
        """Save an uploaded file to the temporary directory.
        
        Args:
            uploaded_file: The uploaded file object
            file_name: Name to save the file as
            
        Returns:
            Tuple of (success, message/path)
        """
        try:
            save_path = os.path.join(self.temp_dir, file_name)
            with open(save_path, "wb") as f:
                shutil.copyfileobj(uploaded_file, f)
            return True, save_path
        except Exception as e:
            return False, f"Error saving {file_name}: {e}"
    
    @staticmethod
    def detect_file_type(file, selected_type: str) -> str:
        """Detect the file type based on extension and selected type.
        
        Args:
            file: The uploaded file object
            selected_type: The type selected by the user
            
        Returns:
            Detected file type string
        """
        if not hasattr(file, 'name'):
            raise ValueError("Invalid file object: missing filename attribute")
            
        filename = file.name.lower()
        
        # Map of selected types to allowed extensions and their corresponding internal types
        type_mapping = {
            "Excel": {
                "extensions": [".xlsx", ".xls"],
                "type": "Excel"
            },
            "DD": {
                "extensions": [".txt", ".dat"],
                "type": "DD"
            },
            "Text": {
                "extensions": [".txt", ".csv", ".json", ".log"],
                "types": {
                    ".txt": "TEXT",
                    ".csv": "CSV",
                    ".json": "JSON",
                    ".log": "LOG"
                }
            }
        }
        
        if selected_type not in type_mapping:
            raise ValueError(f"Invalid selected type: {selected_type}. Must be one of: {list(type_mapping.keys())}")
            
        type_info = type_mapping[selected_type]
        file_ext = Path(filename).suffix.lower()
        
        if file_ext not in type_info["extensions"]:
            allowed_exts = ", ".join(type_info["extensions"])
            raise ValueError(
                f"Invalid file extension for type '{selected_type}'. "
                f"File: '{filename}', Extension: '{file_ext}'. "
                f"Allowed extensions: {allowed_exts}"
            )
            
        # For Text type, we need to map the extension to the specific format
        if selected_type == "Text":
            return type_info["types"][file_ext]
            
        return type_info["type"]
    
    @staticmethod
    def prepare_file_for_processing(uploaded_file) -> BytesIO:
        """Prepare an uploaded file for processing.
        
        Args:
            uploaded_file: The uploaded file object
            
        Returns:
            BytesIO object ready for processing
        """
        file_bytes = BytesIO(uploaded_file.getvalue())
        file_bytes.seek(0)
        return file_bytes 