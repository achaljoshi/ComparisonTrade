import pandas as pd
from st_aggrid import GridOptionsBuilder

def convert_to_numeric(value):
    if pd.isna(value) or value == '':
        return 0
    if not isinstance(value, str):
        return float(value)
    
    # Remove any commas and spaces
    value = str(value).replace(',', '').replace(' ', '')
    
    # Define unit multipliers
    multipliers = {
        'K': 1e3,
        'M': 1e6,
        'B': 1e9,
        'T': 1e12
    }
    
    # Extract number and unit
    number = ''.join([c for c in value if c.isdigit() or c == '.'])
    unit = ''.join([c for c in value if c.isalpha()]).upper()
    
    try:
        number = float(number)
        if unit in multipliers:
            number *= multipliers[unit]
        return number
    except ValueError:
        return value

def get_grid_options(comparison_df):
    """
    Configure grid options with custom sorting for alphanumeric values
    """
    gb = GridOptionsBuilder.from_dataframe(comparison_df)
    gb.configure_default_column(sortable=True)
    
    gridOptions = gb.build()
    
    gridOptions['columnDefs'] = [{
        'field': col,
        'sortable': True,
        'sortingOrder': ['asc', 'desc', None],
        'valueFormatter': """function(params) {
            return params.value;
        }""",
        'comparator': """function(valueA, valueB, nodeA, nodeB, isDescending) {
            // Convert values using custom logic
            function convertToNumber(val) {
                if (!val) return 0;
                if (typeof val === 'number') return val;
                
                val = String(val).replace(/,/g, '').replace(/ /g, '').toUpperCase();
                let number = parseFloat(val.replace(/[^0-9.-]/g, ''));
                if (isNaN(number)) return val;
                
                if (val.includes('K')) {
                    return number * 1000;
                } else if (val.includes('M')) {
                    return number * 1000000;
                } else if (val.includes('B')) {
                    return number * 1000000000;
                } else if (val.includes('T')) {
                    return number * 1000000000000;
                }
                return number;
            }
            
            let numA = convertToNumber(valueA);
            let numB = convertToNumber(valueB);
            
            // If both are numbers, do numeric comparison
            if (typeof numA === 'number' && typeof numB === 'number') {
                return numA - numB;
            }
            
            // If one is a number and other isn't, numbers come first
            if (typeof numA === 'number') return -1;
            if (typeof numB === 'number') return 1;
            
            // Fall back to string comparison if not numbers
            if (valueA < valueB) return -1;
            if (valueA > valueB) return 1;
            return 0;
        }"""
    } for col in comparison_df.columns]
    
    return gridOptions 