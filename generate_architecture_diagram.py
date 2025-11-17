#!/usr/bin/env python3
"""
Generate Architecture Diagram for ComparisonTrade Project
This script creates a visual architecture diagram using graphviz.
"""

try:
    from graphviz import Digraph
    GRAPHVIZ_AVAILABLE = True
except ImportError:
    GRAPHVIZ_AVAILABLE = False
    print("Warning: graphviz not available. Install with: pip install graphviz")
    print("Also ensure graphviz is installed on your system: https://graphviz.org/download/")

def create_architecture_diagram():
    """Create the main architecture diagram."""
    if not GRAPHVIZ_AVAILABLE:
        print("Cannot generate diagram without graphviz. Creating text representation instead.")
        return
    
    # Create a new directed graph
    dot = Digraph(comment='ComparisonTrade Architecture', format='svg')
    dot.attr(rankdir='TB', size='16,12', dpi='300')
    dot.attr('node', shape='box', style='rounded,filled', fontname='Arial')
    dot.attr('edge', fontname='Arial')
    
    # User Interfaces Layer
    with dot.subgraph(name='cluster_ui') as ui:
        ui.attr(label='User Interfaces Layer', style='filled', color='lightblue', fontsize='16')
        ui.node('CLI', 'CLI Interface\ncli.py', fillcolor='#E3F2FD')
        ui.node('WEB', 'Web UI\nStreamlit - app.py', fillcolor='#E3F2FD')
        ui.node('API', 'REST API\nFastAPI - api.py', fillcolor='#E3F2FD')
    
    # Application Layer
    with dot.subgraph(name='cluster_app') as app:
        app.attr(label='Application Layer', style='filled', color='lightgreen', fontsize='16')
        app.node('DP', 'DataProcessor\nCore Processing Engine', fillcolor='#C8E6C9')
        app.node('VAL', 'Validator\nRule Validation', fillcolor='#C8E6C9')
        app.node('JM', 'JobManager\nJob Orchestration', fillcolor='#C8E6C9')
    
    # Data Access Layer
    with dot.subgraph(name='cluster_data') as data:
        data.attr(label='Data Access Layer', style='filled', color='lightyellow', fontsize='16')
        data.node('FH', 'FileHandler\nFile Operations', fillcolor='#FFF9C4')
        data.node('DBH', 'DatabaseHandler\nDatabase Connections', fillcolor='#FFF9C4')
        data.node('VIZ', 'Visualizer\nData Visualization', fillcolor='#FFF9C4')
    
    # Configuration Layer
    with dot.subgraph(name='cluster_config') as config:
        config.attr(label='Configuration Layer', style='filled', color='lightcoral', fontsize='16')
        config.node('DC', 'Directory Config\ndirectory_config.json', fillcolor='#FFCCBC', shape='ellipse')
        config.node('JR', 'Job Response\njob_creation_response.json', fillcolor='#FFCCBC', shape='ellipse')
        config.node('RC', 'Rules Config\nrules_config.json', fillcolor='#FFCCBC', shape='ellipse')
    
    # Data Sources
    with dot.subgraph(name='cluster_sources') as sources:
        sources.attr(label='Data Sources', style='filled', color='lavender', fontsize='16')
        sources.node('EXCEL', 'Excel Files\n.xlsx, .xls', fillcolor='#E1BEE7', shape='cylinder')
        sources.node('CSV', 'CSV Files\n.csv', fillcolor='#E1BEE7', shape='cylinder')
        sources.node('TXT', 'Text Files\n.txt, .log', fillcolor='#E1BEE7', shape='cylinder')
        sources.node('DB', 'Databases\nPostgreSQL, MySQL,\nOracle, MSSQL', fillcolor='#E1BEE7', shape='cylinder')
    
    # Output Layer
    with dot.subgraph(name='cluster_output') as output:
        output.attr(label='Output Layer', style='filled', color='lightpink', fontsize='16')
        output.node('RESULTS', 'Discrepancy Results\nDataFrame', fillcolor='#F8BBD0')
        output.node('REPORTS', 'Reports\nCSV, JSON, XLSX', fillcolor='#F8BBD0')
        output.node('CHARTS', 'Visualizations\nPlotly Charts', fillcolor='#F8BBD0')
    
    # Connections from UI to Application
    dot.edge('CLI', 'DP', label='Process')
    dot.edge('WEB', 'DP', label='Process')
    dot.edge('API', 'DP', label='Process')
    
    # Connections within Application Layer
    dot.edge('DP', 'VAL', label='Validate')
    dot.edge('DP', 'JM', label='Orchestrate')
    
    # Connections to Data Access
    dot.edge('DP', 'FH', label='Read Files')
    dot.edge('DP', 'DBH', label='Query DB')
    
    # Connections to Configuration
    dot.edge('DP', 'DC', label='Load', style='dashed')
    dot.edge('DP', 'JR', label='Load', style='dashed')
    dot.edge('DP', 'RC', label='Load', style='dashed')
    dot.edge('VAL', 'RC', label='Apply Rules', style='dashed')
    
    # Connections from Data Access to Sources
    dot.edge('FH', 'EXCEL', label='Read')
    dot.edge('FH', 'CSV', label='Read')
    dot.edge('FH', 'TXT', label='Read')
    dot.edge('DBH', 'DB', label='Query')
    
    # Connections to Output
    dot.edge('DP', 'RESULTS', label='Generate')
    dot.edge('RESULTS', 'REPORTS', label='Export')
    dot.edge('RESULTS', 'CHARTS', label='Visualize')
    dot.edge('VIZ', 'CHARTS', label='Create')
    dot.edge('WEB', 'VIZ', label='Display')
    
    return dot

def create_data_flow_diagram():
    """Create a data flow diagram."""
    if not GRAPHVIZ_AVAILABLE:
        return None
    
    dot = Digraph(comment='ComparisonTrade Data Flow', format='svg')
    dot.attr(rankdir='LR', size='20,10', dpi='300')
    dot.attr('node', shape='box', style='rounded,filled', fontname='Arial')
    dot.attr('edge', fontname='Arial')
    
    # Nodes
    dot.node('USER', 'User', fillcolor='#FFE0B2', shape='ellipse')
    dot.node('UI', 'Web UI/CLI/API', fillcolor='#E3F2FD')
    dot.node('DP', 'DataProcessor\n- Load Configs\n- Parse Files\n- Compare Data\n- Classify Discrepancies', fillcolor='#C8E6C9')
    dot.node('FH', 'FileHandler\n- Detect Type\n- Prepare Files', fillcolor='#FFF9C4')
    dot.node('DBH', 'DatabaseHandler\n- Connect\n- Query', fillcolor='#FFF9C4')
    dot.node('VAL', 'Validator\n- Apply Rules\n- Check Violations', fillcolor='#C8E6C9')
    dot.node('VIZ', 'Visualizer\n- Generate Charts', fillcolor='#F8BBD0')
    dot.node('OUT', 'Results\nCSV/JSON/XLSX', fillcolor='#F8BBD0', shape='ellipse')
    
    # Flow
    dot.edge('USER', 'UI', label='1. Upload Files/Config')
    dot.edge('UI', 'DP', label='2. Initialize')
    dot.edge('DP', 'FH', label='3a. File Processing')
    dot.edge('DP', 'DBH', label='3b. DB Processing')
    dot.edge('DP', 'VAL', label='4. Validate')
    dot.edge('VAL', 'DP', label='5. Rule Results')
    dot.edge('DP', 'VIZ', label='6. Generate Charts')
    dot.edge('DP', 'OUT', label='7. Export Results')
    dot.edge('VIZ', 'UI', label='8. Display')
    dot.edge('UI', 'USER', label='9. Show Results')
    
    return dot

def create_component_diagram():
    """Create a detailed component diagram."""
    if not GRAPHVIZ_AVAILABLE:
        return None
    
    dot = Digraph(comment='ComparisonTrade Components', format='svg')
    dot.attr(rankdir='TB', size='18,14', dpi='300')
    dot.attr('node', shape='box', style='rounded,filled', fontname='Arial')
    dot.attr('edge', fontname='Arial')
    
    # Entry Points
    with dot.subgraph(name='cluster_entry') as entry:
        entry.attr(label='Entry Points', style='filled', color='lightblue')
        entry.node('CLI', 'CLI\ncli.py', fillcolor='#E3F2FD')
        entry.node('WEB', 'Web UI\napp.py', fillcolor='#E3F2FD')
        entry.node('API', 'API\napi.py', fillcolor='#E3F2FD')
    
    # Core Processing
    with dot.subgraph(name='cluster_core') as core:
        core.attr(label='Core Processing', style='filled', color='lightgreen')
        core.node('DP', 'DataProcessor\n- read_file()\n- compare_files()\n- extract_discrepancy()\n- classify_discrepancies()', fillcolor='#C8E6C9')
        core.node('VAL', 'Validator\n- ValidationRule\n- ValidationRuleLoader', fillcolor='#C8E6C9')
    
    # Data Handlers
    with dot.subgraph(name='cluster_handlers') as handlers:
        handlers.attr(label='Data Handlers', style='filled', color='lightyellow')
        handlers.node('FH', 'FileHandler\n- detect_file_type()\n- prepare_file()', fillcolor='#FFF9C4')
        handlers.node('DBH', 'DatabaseHandler\n- create_connection()\n- read_data()', fillcolor='#FFF9C4')
    
    # Supporting Modules
    with dot.subgraph(name='cluster_support') as support:
        support.attr(label='Supporting Modules', style='filled', color='lavender')
        support.node('UIH', 'UIHelpers\n- render_filter_ui()\n- render_results()', fillcolor='#E1BEE7')
        support.node('VIZ', 'Visualizer\n- generate_charts()', fillcolor='#E1BEE7')
        support.node('JM', 'JobManager\n- job_creation()', fillcolor='#E1BEE7')
        support.node('TSA', 'TimeSeriesAnalyzer\n- temporal_analysis()', fillcolor='#E1BEE7')
    
    # Configuration
    with dot.subgraph(name='cluster_config') as config:
        config.attr(label='Configuration', style='filled', color='lightcoral')
        config.node('DC', 'directory_config.json', fillcolor='#FFCCBC', shape='ellipse')
        config.node('JR', 'job_creation_response.json', fillcolor='#FFCCBC', shape='ellipse')
        config.node('RC', 'rules_config.json', fillcolor='#FFCCBC', shape='ellipse')
    
    # Connections
    dot.edge('CLI', 'DP')
    dot.edge('WEB', 'DP')
    dot.edge('API', 'DP')
    dot.edge('DP', 'VAL')
    dot.edge('DP', 'FH')
    dot.edge('DP', 'DBH')
    dot.edge('DP', 'DC', style='dashed')
    dot.edge('DP', 'JR', style='dashed')
    dot.edge('DP', 'RC', style='dashed')
    dot.edge('WEB', 'UIH')
    dot.edge('WEB', 'VIZ')
    dot.edge('DP', 'JM')
    dot.edge('DP', 'TSA')
    
    return dot

def main():
    """Generate all architecture diagrams."""
    print("Generating ComparisonTrade Architecture Diagrams...")
    
    # Main architecture diagram
    arch_diagram = create_architecture_diagram()
    if arch_diagram:
        arch_diagram.render('Architecture_Diagram', format='svg', cleanup=True)
        arch_diagram.render('Architecture_Diagram', format='png', cleanup=True)
        print("✓ Created Architecture_Diagram.svg and Architecture_Diagram.png")
    
    # Data flow diagram
    flow_diagram = create_data_flow_diagram()
    if flow_diagram:
        flow_diagram.render('Data_Flow_Diagram', format='svg', cleanup=True)
        flow_diagram.render('Data_Flow_Diagram', format='png', cleanup=True)
        print("✓ Created Data_Flow_Diagram.svg and Data_Flow_Diagram.png")
    
    # Component diagram
    comp_diagram = create_component_diagram()
    if comp_diagram:
        comp_diagram.render('Component_Diagram', format='svg', cleanup=True)
        comp_diagram.render('Component_Diagram', format='png', cleanup=True)
        print("✓ Created Component_Diagram.svg and Component_Diagram.png")
    
    print("\nAll diagrams generated successfully!")
    print("\nNote: If diagrams were not generated, install graphviz:")
    print("  - pip install graphviz")
    print("  - Install system package: https://graphviz.org/download/")

if __name__ == '__main__':
    main()

