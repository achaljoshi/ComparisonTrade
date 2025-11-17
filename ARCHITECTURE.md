# ComparisonTrade - System Architecture

## Overview

ComparisonTrade is an AI-powered automated discrepancy detection and validation system designed for financial trade validation, regulatory compliance, and structured/unstructured data reconciliation.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                    USER INTERFACES LAYER                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                │
│  │   CLI    │    │  Web UI  │    │   API    │                │
│  │  cli.py  │    │ app.py   │    │  api.py  │                │
│  └──────────┘    └──────────┘    └──────────┘                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                             │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              DataProcessor (Core Engine)                  │  │
│  │  • read_file()          • compare_files()                │  │
│  │  • extract_discrepancy() • classify_discrepancies()     │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │  Validator   │    │  JobManager  │    │  UIHelpers   │   │
│  │  Rule Engine │    │  Orchestrator│    │  UI Components│   │
│  └──────────────┘    └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA ACCESS LAYER                             │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │ FileHandler  │    │DatabaseHandler│    │ Visualizer   │   │
│  │ File I/O     │    │ DB Operations │    │ Charts/Graphs│   │
│  └──────────────┘    └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                   │
├─────────────────────────────────────────────────────────────────┤
│  Excel Files  │  CSV Files  │  Text Files  │  Databases        │
│  (.xlsx, .xls)│  (.csv)     │  (.txt, .log)│  (PostgreSQL,     │
│               │             │              │   MySQL, Oracle,   │
│               │             │              │   MSSQL)           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OUTPUT LAYER                                  │
├─────────────────────────────────────────────────────────────────┤
│  Discrepancy Results  │  Reports  │  Visualizations            │
│  (DataFrame)          │ (CSV/JSON │  (Plotly Charts)           │
│                       │  /XLSX)   │                             │
└─────────────────────────────────────────────────────────────────┘
```

## Component Architecture

### 1. Entry Points

#### CLI Interface (`cli.py`)
- Command-line tool for batch processing
- Supports file-based comparisons
- Output formats: CSV, JSON, Excel

#### Web UI (`app.py`)
- Streamlit-based interactive dashboard
- File upload interface
- Real-time visualization
- Filter and export capabilities

#### REST API (`api.py`)
- FastAPI-based RESTful endpoints
- Programmatic access for integrations
- Supports multiple file formats
- Returns JSON or file downloads

### 2. Core Processing Engine

#### DataProcessor (`utils/data_processor.py`)
**Responsibilities:**
- File reading and parsing (Excel, CSV, TXT, Database)
- Data normalization and alignment
- Discrepancy detection
- Classification (INFO, ACCEPTABLE, WARNING, FATAL)
- Filter processing

**Key Methods:**
- `read_file()`: Multi-format file reading
- `compare_files()`: Core comparison logic
- `extract_discrepancy()`: Identify differences
- `classify_discrepancies()`: Categorize by severity

#### Validator (`utils/validator.py`)
**Responsibilities:**
- Rule loading from configuration
- Rule validation execution
- Violation detection
- Result aggregation

**Key Classes:**
- `ValidationRule`: Individual rule definition
- `ValidationRuleLoader`: Configuration loader

### 3. Data Access Layer

#### FileHandler (`utils/file_handler.py`)
- File type detection
- Temporary file management
- File cleanup operations
- Multi-format support

#### DatabaseHandler (`utils/database_handler.py`)
- Database connection management
- Query execution
- Connection pooling
- Multi-database support (PostgreSQL, MySQL, Oracle, MSSQL)

#### Visualizer (`utils/visualizer.py`)
- Chart generation (Plotly)
- Data visualization
- Interactive graphs
- Export capabilities

### 4. Supporting Modules

#### UIHelpers (`utils/ui_helpers.py`)
- Streamlit UI components
- Filter rendering
- Results display
- Session state management

#### JobManager (`utils/job_manager.py`)
- Job lifecycle management
- Workflow orchestration
- Status tracking

#### TimeSeriesAnalyzer (`utils/time_series_analyzer.py`)
- Temporal data analysis
- Time-based comparisons
- Trend detection

### 5. Configuration Layer

#### directory_config.json
- Directory paths
- File locations
- Environment settings

#### job_creation_response.json
- Job metadata
- Database configurations
- Baseline and candidate sources

#### rules_config.json
- Validation rules
- Threshold definitions
- Classification criteria
- Rule priorities

## Data Flow

```
1. User Input
   ├─> Upload files via CLI/Web/API
   └─> Provide configuration files

2. Initialization
   ├─> Load configuration files
   ├─> Initialize DataProcessor
   └─> Load validation rules

3. Data Ingestion
   ├─> File-based: FileHandler reads files
   │   ├─> Detect file type
   │   ├─> Parse content (Excel/CSV/TXT)
   │   └─> Convert to DataFrame
   └─> Database: DatabaseHandler queries
       ├─> Establish connections
       ├─> Execute queries
       └─> Return DataFrames

4. Data Processing
   ├─> Merge baseline and candidate
   ├─> Align columns and keys
   ├─> Identify missing rows
   └─> Prepare for comparison

5. Validation
   ├─> Apply validation rules
   ├─> Detect rule violations
   ├─> Calculate differences
   └─> Classify discrepancies

6. Output Generation
   ├─> Create results DataFrame
   ├─> Generate visualizations
   ├─> Format reports
   └─> Export (CSV/JSON/XLSX)

7. Cleanup
   ├─> Close database connections
   ├─> Remove temporary files
   └─> Release resources
```

## Technology Stack

### Frontend
- **Streamlit**: Web dashboard framework
- **Plotly**: Interactive data visualization

### Backend
- **Python 3.12**: Core programming language
- **FastAPI**: REST API framework
- **Pandas**: Data manipulation and analysis
- **NumPy**: Numerical operations

### Database
- **SQLAlchemy**: ORM and database abstraction
- **PostgreSQL/MySQL/Oracle/MSSQL**: Supported databases

### File Processing
- **OpenPyXL**: Excel file processing
- **Pandas**: CSV and text file processing
- **Custom parsers**: DD file format support

### Utilities
- **Logging**: Application logging
- **JSON**: Configuration management
- **Pathlib**: File path handling

## System Characteristics

### Scalability
- Supports large datasets through chunked processing
- Efficient memory management
- Database connection pooling

### Extensibility
- Rule-based validation (easily configurable)
- Plugin architecture for new file formats
- Modular component design

### Reliability
- Comprehensive error handling
- Input validation
- Resource cleanup

### Usability
- Multiple interfaces (CLI, Web, API)
- Interactive visualizations
- Flexible export options

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Layer                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                 │
│  │ Browser  │  │ Terminal │  │  API     │                 │
│  │ (Web UI) │  │  (CLI)   │  │  Client  │                 │
│  └──────────┘  └──────────┘  └──────────┘                 │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  Application Server                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         ComparisonTrade Application                   │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │   │
│  │  │ Streamlit│  │ FastAPI  │  │   CLI    │          │   │
│  │  │  Server  │  │  Server  │  │  Script  │          │   │
│  │  └──────────┘  └──────────┘  └──────────┘          │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Data Layer                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                │
│  │  Files   │  │Database  │  │  Config  │                │
│  │  System  │  │ Servers  │  │  Files   │                │
│  └──────────┘  └──────────┘  └──────────┘                │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Patterns

### 1. Strategy Pattern
- Different file parsers for different formats
- Multiple validation strategies

### 2. Factory Pattern
- Database connection creation
- File handler instantiation

### 3. Observer Pattern
- Event-driven processing
- Status updates

### 4. Template Method Pattern
- Standardized comparison workflow
- Configurable rule application

## Security Considerations

- Input validation and sanitization
- Secure file handling
- Database connection security
- Configuration file protection
- Temporary file cleanup

## Performance Optimizations

- Chunked file processing for large files
- Lazy loading of data
- Efficient DataFrame operations
- Connection pooling for databases
- Caching of configuration files

## Future Enhancements

- Real-time streaming comparisons
- Machine learning-based anomaly detection
- Distributed processing support
- Enhanced visualization capabilities
- Advanced reporting features

