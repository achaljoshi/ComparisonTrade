#!/usr/bin/env python3
"""
Create a visual architecture diagram using matplotlib
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
import matplotlib.patches as patches

def create_architecture_diagram():
    """Create a visual architecture diagram."""
    fig, ax = plt.subplots(1, 1, figsize=(16, 18))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 18)
    ax.axis('off')
    
    # Define colors
    colors = {
        'ui': '#E3F2FD',      # Light blue
        'app': '#C8E6C9',     # Light green
        'data': '#FFF9C4',    # Light yellow
        'sources': '#E1BEE7', # Lavender
        'output': '#F8BBD0',  # Light pink
        'llm': '#FFE082',     # Light amber for LLM
        'border': '#1976D2',  # Blue border
        'text': '#212121'     # Dark text
    }
    
    # Layer 1: User Interfaces Layer
    y_start = 16
    layer_width = 9
    layer1 = FancyBboxPatch((0.5, y_start), layer_width, 1.5, 
                           boxstyle="round,pad=0.1", 
                           edgecolor=colors['border'], 
                           facecolor=colors['ui'],
                           linewidth=2)
    ax.add_patch(layer1)
    ax.text(5, y_start + 1.2, 'USER INTERFACES LAYER', 
            ha='center', va='center', fontsize=14, fontweight='bold', color=colors['text'])
    
    # CLI, Web UI, API boxes
    box_width = 2.5
    box_height = 0.8
    spacing = 0.3
    start_x = 1.5
    
    # CLI
    cli_box = FancyBboxPatch((start_x, y_start + 0.2), box_width, box_height,
                             boxstyle="round,pad=0.05", 
                             edgecolor='#1565C0', 
                             facecolor='white',
                             linewidth=1.5)
    ax.add_patch(cli_box)
    ax.text(start_x + box_width/2, y_start + 0.4, 'CLI', 
            ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(start_x + box_width/2, y_start + 0.25, 'cli.py', 
            ha='center', va='center', fontsize=9, style='italic')
    
    # Web UI
    web_x = start_x + box_width + spacing
    web_box = FancyBboxPatch((web_x, y_start + 0.2), box_width, box_height,
                             boxstyle="round,pad=0.05", 
                             edgecolor='#1565C0', 
                             facecolor='white',
                             linewidth=1.5)
    ax.add_patch(web_box)
    ax.text(web_x + box_width/2, y_start + 0.4, 'Web UI', 
            ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(web_x + box_width/2, y_start + 0.25, 'app.py', 
            ha='center', va='center', fontsize=9, style='italic')
    
    # API
    api_x = web_x + box_width + spacing
    api_box = FancyBboxPatch((api_x, y_start + 0.2), box_width, box_height,
                             boxstyle="round,pad=0.05", 
                             edgecolor='#1565C0', 
                             facecolor='white',
                             linewidth=1.5)
    ax.add_patch(api_box)
    ax.text(api_x + box_width/2, y_start + 0.4, 'API', 
            ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(api_x + box_width/2, y_start + 0.25, 'api.py', 
            ha='center', va='center', fontsize=9, style='italic')
    
    # Arrow down
    arrow1 = FancyArrowPatch((5, y_start), (5, y_start - 0.5),
                            arrowstyle='->', mutation_scale=20,
                            color='#424242', linewidth=2)
    ax.add_patch(arrow1)
    
    # LLM Configuration Generation Layer (on the right side)
    llm_y_start = 13.5
    llm_layer = FancyBboxPatch((10, llm_y_start), 1.8, 2.5, 
                              boxstyle="round,pad=0.1", 
                              edgecolor='#F57C00', 
                              facecolor=colors['llm'],
                              linewidth=2)
    ax.add_patch(llm_layer)
    ax.text(10.9, llm_y_start + 2.2, 'LLM\nCONFIG\nGENERATION', 
            ha='center', va='center', fontsize=11, fontweight='bold', color=colors['text'])
    
    # LLM Model box
    llm_box = FancyBboxPatch((10.2, llm_y_start + 1.3), 1.4, 0.6,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#E65100', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(llm_box)
    ax.text(10.9, llm_y_start + 1.7, 'LLM Model', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(10.9, llm_y_start + 1.5, '(GPT/Claude)', 
            ha='center', va='center', fontsize=8, style='italic')
    
    # Rules Config JSON output
    rules_box = FancyBboxPatch((10.2, llm_y_start + 0.3), 1.4, 0.6,
                              boxstyle="round,pad=0.05", 
                              edgecolor='#E65100', 
                              facecolor='white',
                              linewidth=1.5)
    ax.add_patch(rules_box)
    ax.text(10.9, llm_y_start + 0.7, 'rules_config.json', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(10.9, llm_y_start + 0.5, 'Generated Config', 
            ha='center', va='center', fontsize=8)
    
    # Arrow from LLM to Application Layer
    arrow_llm = FancyArrowPatch((10, llm_y_start + 1.6), (9.5, llm_y_start + 1.6),
                                arrowstyle='->', mutation_scale=15,
                                color='#E65100', linewidth=2, linestyle='--')
    ax.add_patch(arrow_llm)
    ax.text(9.75, llm_y_start + 1.8, 'Generates', 
            ha='center', va='bottom', fontsize=8, color='#E65100', style='italic')
    
    # Layer 2: Application Layer
    y_start = 13.5
    layer2 = FancyBboxPatch((0.5, y_start), layer_width, 2.5, 
                           boxstyle="round,pad=0.1", 
                           edgecolor=colors['border'], 
                           facecolor=colors['app'],
                           linewidth=2)
    ax.add_patch(layer2)
    ax.text(5, y_start + 2.2, 'APPLICATION LAYER', 
            ha='center', va='center', fontsize=14, fontweight='bold', color=colors['text'])
    
    # DataProcessor (Core Engine) - larger box
    dp_box = FancyBboxPatch((1, y_start + 1.1), 8, 0.9,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#2E7D32', 
                            facecolor='white',
                            linewidth=2)
    ax.add_patch(dp_box)
    ax.text(5, y_start + 1.65, 'DataProcessor (Core Engine)', 
            ha='center', va='center', fontsize=12, fontweight='bold')
    ax.text(2.5, y_start + 1.35, '• read_file()', 
            ha='left', va='center', fontsize=9)
    ax.text(5, y_start + 1.35, '• compare_files()', 
            ha='center', va='center', fontsize=9)
    ax.text(2.5, y_start + 1.2, '• extract_discrepancy()', 
            ha='left', va='center', fontsize=9)
    ax.text(5, y_start + 1.2, '• classify_discrepancies()', 
            ha='center', va='center', fontsize=9)
    
    # Validator, JobManager, UIHelpers
    small_box_width = 2.6
    small_box_height = 0.7
    small_spacing = 0.2
    small_start_x = 1.2
    
    # Validator
    val_box = FancyBboxPatch((small_start_x, y_start + 0.2), small_box_width, small_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#2E7D32', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(val_box)
    ax.text(small_start_x + small_box_width/2, y_start + 0.45, 'Validator', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(small_start_x + small_box_width/2, y_start + 0.3, 'Rule Engine', 
            ha='center', va='center', fontsize=9)
    
    # JobManager
    jm_x = small_start_x + small_box_width + small_spacing
    jm_box = FancyBboxPatch((jm_x, y_start + 0.2), small_box_width, small_box_height,
                           boxstyle="round,pad=0.05", 
                           edgecolor='#2E7D32', 
                           facecolor='white',
                           linewidth=1.5)
    ax.add_patch(jm_box)
    ax.text(jm_x + small_box_width/2, y_start + 0.45, 'JobManager', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(jm_x + small_box_width/2, y_start + 0.3, 'Orchestrator', 
            ha='center', va='center', fontsize=9)
    
    # UIHelpers
    uih_x = jm_x + small_box_width + small_spacing
    uih_box = FancyBboxPatch((uih_x, y_start + 0.2), small_box_width, small_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#2E7D32', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(uih_box)
    ax.text(uih_x + small_box_width/2, y_start + 0.45, 'UIHelpers', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(uih_x + small_box_width/2, y_start + 0.3, 'UI Components', 
            ha='center', va='center', fontsize=9)
    
    # Arrow down
    arrow2 = FancyArrowPatch((5, y_start), (5, y_start - 0.5),
                            arrowstyle='->', mutation_scale=20,
                            color='#424242', linewidth=2)
    ax.add_patch(arrow2)
    
    # Layer 3: Data Access Layer
    y_start = 10.5
    layer3 = FancyBboxPatch((0.5, y_start), layer_width, 1.5, 
                           boxstyle="round,pad=0.1", 
                           edgecolor=colors['border'], 
                           facecolor=colors['data'],
                           linewidth=2)
    ax.add_patch(layer3)
    ax.text(5, y_start + 1.2, 'DATA ACCESS LAYER', 
            ha='center', va='center', fontsize=14, fontweight='bold', color=colors['text'])
    
    # FileHandler, DatabaseHandler, Visualizer
    data_box_width = 2.6
    data_box_height = 0.8
    data_spacing = 0.2
    data_start_x = 1.2
    
    # FileHandler
    fh_box = FancyBboxPatch((data_start_x, y_start + 0.2), data_box_width, data_box_height,
                           boxstyle="round,pad=0.05", 
                           edgecolor='#F57F17', 
                           facecolor='white',
                           linewidth=1.5)
    ax.add_patch(fh_box)
    ax.text(data_start_x + data_box_width/2, y_start + 0.5, 'FileHandler', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(data_start_x + data_box_width/2, y_start + 0.3, 'File I/O', 
            ha='center', va='center', fontsize=9)
    
    # DatabaseHandler
    dbh_x = data_start_x + data_box_width + data_spacing
    dbh_box = FancyBboxPatch((dbh_x, y_start + 0.2), data_box_width, data_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#F57F17', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(dbh_box)
    ax.text(dbh_x + data_box_width/2, y_start + 0.5, 'DatabaseHandler', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(dbh_x + data_box_width/2, y_start + 0.3, 'DB Operations', 
            ha='center', va='center', fontsize=9)
    
    # Visualizer
    viz_x = dbh_x + data_box_width + data_spacing
    viz_box = FancyBboxPatch((viz_x, y_start + 0.2), data_box_width, data_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#F57F17', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(viz_box)
    ax.text(viz_x + data_box_width/2, y_start + 0.5, 'Visualizer', 
            ha='center', va='center', fontsize=10, fontweight='bold')
    ax.text(viz_x + data_box_width/2, y_start + 0.3, 'Charts/Graphs', 
            ha='center', va='center', fontsize=9)
    
    # Arrow down
    arrow3 = FancyArrowPatch((5, y_start), (5, y_start - 0.5),
                            arrowstyle='->', mutation_scale=20,
                            color='#424242', linewidth=2)
    ax.add_patch(arrow3)
    
    # Layer 4: Data Sources
    y_start = 8.5
    layer4 = FancyBboxPatch((0.5, y_start), layer_width, 1.5, 
                           boxstyle="round,pad=0.1", 
                           edgecolor=colors['border'], 
                           facecolor=colors['sources'],
                           linewidth=2)
    ax.add_patch(layer4)
    ax.text(5, y_start + 1.2, 'DATA SOURCES', 
            ha='center', va='center', fontsize=14, fontweight='bold', color=colors['text'])
    
    # Data source boxes
    source_box_width = 2
    source_box_height = 0.8
    source_spacing = 0.15
    source_start_x = 1.3
    
    # Excel Files
    excel_box = FancyBboxPatch((source_start_x, y_start + 0.2), source_box_width, source_box_height,
                              boxstyle="round,pad=0.05", 
                              edgecolor='#7B1FA2', 
                              facecolor='white',
                              linewidth=1.5)
    ax.add_patch(excel_box)
    ax.text(source_start_x + source_box_width/2, y_start + 0.5, 'Excel Files', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(source_start_x + source_box_width/2, y_start + 0.3, '(.xlsx, .xls)', 
            ha='center', va='center', fontsize=8)
    
    # CSV Files
    csv_x = source_start_x + source_box_width + source_spacing
    csv_box = FancyBboxPatch((csv_x, y_start + 0.2), source_box_width, source_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#7B1FA2', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(csv_box)
    ax.text(csv_x + source_box_width/2, y_start + 0.5, 'CSV Files', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(csv_x + source_box_width/2, y_start + 0.3, '(.csv)', 
            ha='center', va='center', fontsize=8)
    
    # Text Files
    txt_x = csv_x + source_box_width + source_spacing
    txt_box = FancyBboxPatch((txt_x, y_start + 0.2), source_box_width, source_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#7B1FA2', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(txt_box)
    ax.text(txt_x + source_box_width/2, y_start + 0.5, 'Text Files', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(txt_x + source_box_width/2, y_start + 0.3, '(.txt, .log)', 
            ha='center', va='center', fontsize=8)
    
    # Databases
    db_x = txt_x + source_box_width + source_spacing
    db_box = FancyBboxPatch((db_x, y_start + 0.2), source_box_width + 0.3, source_box_height,
                           boxstyle="round,pad=0.05", 
                           edgecolor='#7B1FA2', 
                           facecolor='white',
                           linewidth=1.5)
    ax.add_patch(db_box)
    ax.text(db_x + (source_box_width + 0.3)/2, y_start + 0.55, 'Databases', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(db_x + (source_box_width + 0.3)/2, y_start + 0.35, '(PostgreSQL,', 
            ha='center', va='center', fontsize=7)
    ax.text(db_x + (source_box_width + 0.3)/2, y_start + 0.25, 'MySQL, Oracle, MSSQL)', 
            ha='center', va='center', fontsize=7)
    
    # Arrow down
    arrow4 = FancyArrowPatch((5, y_start), (5, y_start - 0.5),
                            arrowstyle='->', mutation_scale=20,
                            color='#424242', linewidth=2)
    ax.add_patch(arrow4)
    
    # Layer 5: Output Layer
    y_start = 6.5
    layer5 = FancyBboxPatch((0.5, y_start), layer_width, 1.5, 
                           boxstyle="round,pad=0.1", 
                           edgecolor=colors['border'], 
                           facecolor=colors['output'],
                           linewidth=2)
    ax.add_patch(layer5)
    ax.text(5, y_start + 1.2, 'OUTPUT LAYER', 
            ha='center', va='center', fontsize=14, fontweight='bold', color=colors['text'])
    
    # Output boxes
    output_box_width = 2.6
    output_box_height = 0.8
    output_spacing = 0.2
    output_start_x = 1.2
    
    # Discrepancy Results
    res_box = FancyBboxPatch((output_start_x, y_start + 0.2), output_box_width, output_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#C2185B', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(res_box)
    ax.text(output_start_x + output_box_width/2, y_start + 0.5, 'Discrepancy Results', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(output_start_x + output_box_width/2, y_start + 0.3, '(DataFrame)', 
            ha='center', va='center', fontsize=8)
    
    # Reports
    rep_x = output_start_x + output_box_width + output_spacing
    rep_box = FancyBboxPatch((rep_x, y_start + 0.2), output_box_width, output_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#C2185B', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(rep_box)
    ax.text(rep_x + output_box_width/2, y_start + 0.5, 'Reports', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(rep_x + output_box_width/2, y_start + 0.3, '(CSV/JSON/XLSX)', 
            ha='center', va='center', fontsize=8)
    
    # Visualizations
    vis_x = rep_x + output_box_width + output_spacing
    vis_box = FancyBboxPatch((vis_x, y_start + 0.2), output_box_width, output_box_height,
                            boxstyle="round,pad=0.05", 
                            edgecolor='#C2185B', 
                            facecolor='white',
                            linewidth=1.5)
    ax.add_patch(vis_box)
    ax.text(vis_x + output_box_width/2, y_start + 0.5, 'Visualizations', 
            ha='center', va='center', fontsize=9, fontweight='bold')
    ax.text(vis_x + output_box_width/2, y_start + 0.3, '(Plotly Charts)', 
            ha='center', va='center', fontsize=8)
    
    # Set margins to ensure everything is visible
    plt.subplots_adjust(left=0.05, right=0.98, top=0.98, bottom=0.02)
    return fig

def main():
    """Generate the architecture diagram."""
    print("Creating visual architecture diagram...")
    fig = create_architecture_diagram()
    
    # Save as PNG
    fig.savefig('Architecture_Visual_Diagram.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print("✓ Created Architecture_Visual_Diagram.png")
    
    # Save as SVG
    fig.savefig('Architecture_Visual_Diagram.svg', format='svg', bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print("✓ Created Architecture_Visual_Diagram.svg")
    
    # Save as PDF
    fig.savefig('Architecture_Visual_Diagram.pdf', format='pdf', bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print("✓ Created Architecture_Visual_Diagram.pdf")
    
    plt.close(fig)
    print("\nAll visual diagrams generated successfully!")

if __name__ == '__main__':
    main()

