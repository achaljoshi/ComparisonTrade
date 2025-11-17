import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from typing import Dict, Any, Optional
import logging

class DatabaseHandler:
    """Handler for database operations and comparisons."""
    
    def __init__(self):
        """Initialize the database handler."""
        self.engines = {}
        
    def create_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Create a database connection string from configuration.
        
        Args:
            db_config: Database configuration dictionary
            
        Returns:
            Database connection string
        """
        db_type = db_config.get("type", "").lower()
        
        if db_type == "postgresql":
            return f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        elif db_type == "mysql":
            return f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        elif db_type == "oracle":
            return f"oracle+cx_oracle://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        elif db_type == "mssql":
            return f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def get_engine(self, db_config: Dict[str, Any]) -> sqlalchemy.engine.Engine:
        """Get or create a database engine for the given configuration.
        
        Args:
            db_config: Database configuration dictionary
            
        Returns:
            SQLAlchemy engine
        """
        conn_string = self.create_connection_string(db_config)
        
        if conn_string not in self.engines:
            try:
                self.engines[conn_string] = create_engine(conn_string)
            except Exception as e:
                logging.error(f"Error creating database engine: {str(e)}")
                raise
        
        return self.engines[conn_string]
    
    def read_data(self, db_config: Dict[str, Any]) -> pd.DataFrame:
        """Read data from database using the provided configuration.
        
        Args:
            db_config: Database configuration dictionary
            
        Returns:
            DataFrame containing query results
        """
        try:
            engine = self.get_engine(db_config)
            query = db_config.get("query")
            
            if not query:
                raise ValueError("No query provided in database configuration")
            
            return pd.read_sql(query, engine)
            
        except Exception as e:
            logging.error(f"Error reading data from database: {str(e)}")
            raise
    
    def cleanup(self):
        """Close all database connections."""
        for engine in self.engines.values():
            engine.dispose()
        self.engines.clear() 