# ComparisonTrade - System Architecture Diagram

## High-Level Architecture

```mermaid
graph TB
    subgraph "User Interfaces Layer"
        CLI[CLI Interface<br/>cli.py]
        WEB[Web UI<br/>Streamlit - app.py]
        API[REST API<br/>FastAPI - api.py]
    end
    
    subgraph "Application Layer"
        DP[DataProcessor<br/>Core Processing Engine]
        VAL[Validator<br/>Rule Validation]
        JM[JobManager<br/>Job Orchestration]
    end
    
    subgraph "Data Access Layer"
        FH[FileHandler<br/>File Operations]
        DBH[DatabaseHandler<br/>Database Connections]
        VIZ[Visualizer<br/>Data Visualization]
    end
    
    subgraph "Configuration Layer"
        DC[Directory Config<br/>directory_config.json]
        JR[Job Response<br/>job_creation_response.json]
        RC[Rules Config<br/>rules_config.json]
    end
    
    subgraph "Data Sources"
        EXCEL[Excel Files<br/>.xlsx, .xls]
        CSV[CSV Files<br/>.csv]
        TXT[Text Files<br/>.txt, .log]
        DB[(Databases<br/>PostgreSQL, MySQL,<br/>Oracle, MSSQL)]
    end
    
    subgraph "Output Layer"
        RESULTS[Discrepancy Results<br/>DataFrame]
        REPORTS[Reports<br/>CSV, JSON, XLSX]
        CHARTS[Visualizations<br/>Plotly Charts]
    end
    
    CLI --> DP
    WEB --> DP
    API --> DP
    
    DP --> VAL
    DP --> FH
    DP --> DBH
    DP --> JM
    
    VAL --> RC
    DP --> DC
    DP --> JR
    DP --> RC
    
    FH --> EXCEL
    FH --> CSV
    FH --> TXT
    DBH --> DB
    
    DP --> RESULTS
    RESULTS --> REPORTS
    RESULTS --> CHARTS
    VIZ --> CHARTS
    WEB --> VIZ
```

## Detailed Component Architecture

```mermaid
graph LR
    subgraph "Entry Points"
        A1[CLI<br/>Command Line]
        A2[Web UI<br/>Streamlit Dashboard]
        A3[API<br/>FastAPI Endpoints]
    end
    
    subgraph "Core Processing"
        B1[DataProcessor<br/>- read_file<br/>- compare_files<br/>- extract_discrepancy<br/>- classify_discrepancies]
        B2[Validator<br/>- ValidationRule<br/>- ValidationRuleLoader<br/>- Rule Validation]
    end
    
    subgraph "Data Handlers"
        C1[FileHandler<br/>- detect_file_type<br/>- prepare_file<br/>- cleanup]
        C2[DatabaseHandler<br/>- create_connection<br/>- read_data<br/>- cleanup]
    end
    
    subgraph "Supporting Modules"
        D1[UIHelpers<br/>- render_filter_ui<br/>- render_results]
        D2[Visualizer<br/>- generate_charts<br/>- plot_discrepancies]
        D3[JobManager<br/>- job_creation<br/>- job_tracking]
        D4[TimeSeriesAnalyzer<br/>- temporal_analysis]
    end
    
    subgraph "Configuration"
        E1[directory_config.json<br/>Directory Paths]
        E2[job_creation_response.json<br/>Job & DB Config]
        E3[rules_config.json<br/>Validation Rules]
    end
    
    A1 --> B1
    A2 --> B1
    A3 --> B1
    
    B1 --> B2
    B1 --> C1
    B1 --> C2
    B1 --> E1
    B1 --> E2
    B1 --> E3
    
    A2 --> D1
    A2 --> D2
    B1 --> D3
    B1 --> D4
```

## Data Flow Architecture

```mermaid
sequenceDiagram
    participant User
    participant UI as Web UI/CLI/API
    participant DP as DataProcessor
    participant FH as FileHandler
    participant VAL as Validator
    participant DBH as DatabaseHandler
    participant VIZ as Visualizer
    
    User->>UI: Upload Files/Config
    UI->>DP: Initialize with Configs
    DP->>DP: Load Rules & Configs
    
    alt File-based Comparison
        UI->>FH: Detect File Type
        FH->>FH: Prepare Files
        FH->>DP: Read Files
        DP->>DP: Parse Data (Excel/CSV/TXT)
    else Database Comparison
        UI->>DBH: Connect to Databases
        DBH->>DBH: Execute Queries
        DBH->>DP: Return DataFrames
    end
    
    DP->>DP: Merge Baseline & Candidate
    DP->>DP: Identify Missing Rows
    DP->>VAL: Apply Validation Rules
    VAL->>DP: Return Rule Violations
    DP->>DP: Classify Discrepancies<br/>(INFO/ACCEPTABLE/WARNING/FATAL)
    DP->>DP: Generate Results DataFrame
    
    DP->>UI: Return Results
    UI->>VIZ: Generate Visualizations
    VIZ->>UI: Return Charts
    UI->>User: Display Results & Charts
    UI->>User: Export Reports (CSV/JSON/XLSX)
```

## System Layers

```mermaid
graph TD
    subgraph "Presentation Layer"
        P1[CLI Interface]
        P2[Streamlit Web UI]
        P3[FastAPI REST API]
    end
    
    subgraph "Business Logic Layer"
        B1[DataProcessor<br/>Core Comparison Logic]
        B2[Validator<br/>Rule Engine]
        B3[JobManager<br/>Workflow Management]
    end
    
    subgraph "Data Access Layer"
        D1[FileHandler<br/>File I/O Operations]
        D2[DatabaseHandler<br/>Database Operations]
        D3[File Parser<br/>Multi-format Support]
    end
    
    subgraph "Configuration Layer"
        C1[Config Loader<br/>JSON Configuration]
        C2[Rules Engine<br/>Dynamic Rule Application]
    end
    
    subgraph "Output Layer"
        O1[Report Generator<br/>CSV/JSON/XLSX]
        O2[Visualization Engine<br/>Plotly Charts]
        O3[Export Handler<br/>File Downloads]
    end
    
    P1 --> B1
    P2 --> B1
    P3 --> B1
    
    B1 --> B2
    B1 --> B3
    B1 --> D1
    B1 --> D2
    B1 --> D3
    
    B2 --> C1
    B2 --> C2
    B1 --> C1
    
    B1 --> O1
    B1 --> O2
    P2 --> O3
```

## Technology Stack

```mermaid
graph LR
    subgraph "Frontend"
        F1[Streamlit<br/>Web Dashboard]
    end
    
    subgraph "Backend Framework"
        B1[FastAPI<br/>REST API]
        B2[Python 3.12<br/>Core Language]
    end
    
    subgraph "Data Processing"
        D1[Pandas<br/>Data Manipulation]
        D2[NumPy<br/>Numerical Operations]
    end
    
    subgraph "Database"
        DB1[SQLAlchemy<br/>ORM]
        DB2[PostgreSQL/MySQL<br/>Oracle/MSSQL]
    end
    
    subgraph "Visualization"
        V1[Plotly<br/>Interactive Charts]
    end
    
    subgraph "File Processing"
        FP1[OpenPyXL<br/>Excel Processing]
        FP2[CSV Parser<br/>CSV Processing]
    end
    
    F1 --> B2
    B1 --> B2
    B2 --> D1
    B2 --> D2
    B2 --> DB1
    DB1 --> DB2
    B2 --> V1
    B2 --> FP1
    B2 --> FP2
```

## Component Responsibilities

### Entry Points
- **CLI (cli.py)**: Command-line interface for batch processing
- **Web UI (app.py)**: Interactive Streamlit dashboard for file upload and visualization
- **API (api.py)**: RESTful API endpoints for programmatic access

### Core Processing
- **DataProcessor**: Main orchestration engine for data comparison
  - File reading and parsing (Excel, CSV, TXT, Database)
  - Data merging and alignment
  - Discrepancy detection and classification
  - Filter processing and application

### Validation
- **Validator**: Rule-based validation engine
  - Loads rules from configuration
  - Applies validation rules to datasets
  - Categorizes discrepancies (INFO, ACCEPTABLE, WARNING, FATAL)

### Data Access
- **FileHandler**: Manages file operations
  - File type detection
  - Temporary file management
  - File cleanup

- **DatabaseHandler**: Database connectivity
  - Connection string creation
  - Query execution
  - Connection pooling and cleanup

### Supporting Modules
- **UIHelpers**: Streamlit UI components
- **Visualizer**: Chart and graph generation
- **JobManager**: Job lifecycle management
- **TimeSeriesAnalyzer**: Temporal data analysis

### Configuration
- **directory_config.json**: Directory paths and file locations
- **job_creation_response.json**: Job metadata and database configurations
- **rules_config.json**: Validation rules and thresholds

## Data Flow Summary

1. **Input**: User provides baseline and candidate files via CLI/Web/API
2. **Configuration**: System loads rules and directory configurations
3. **Processing**: DataProcessor reads, parses, and compares datasets
4. **Validation**: Validator applies rules and classifies discrepancies
5. **Output**: Results are formatted, visualized, and exported
6. **Cleanup**: Temporary files and connections are cleaned up

