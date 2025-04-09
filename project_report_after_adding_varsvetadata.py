import sys
import streamlit as st
from pathlib import Path
import pandas as pd
from datetime import datetime
import io
import calendar

# Assuming your current script is in a directory and the helper modules are in a sibling directory
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

# Import helper modules
from helper import bq, func

# Project name mapping to standardize project names from CSV
PROJECT_NAME_MAPPING = {
    'SDI': 'SDI',
    'Vars/Vita.': ['VARS', 'Vitawerks'],  # Combined project mapping
    'Kinergy': 'Kinergy',
    #'Nabis': 'Nabis:Data Warehouse',
    'Nabis - Retainer': 'Nabis',
    'Nabis - 3C': 'Nabis:Data Warehouse',
    'STT - Daily Shuttle': 'STT',
    'Lithium': 'Lithium',
    'Phoenix': 'Phoenix',
    'ForceMultipler': 'ForceMultiplier',
    'QualApps': 'QualApps',
    'Alma': 'Alma',
    'Super Eng.': 'SuperEngineer',
    'Super Eng.': 'Alpha',
    'Digital Twin': 'Digital Twin',
    'Project Alc.': 'Project Allocation',
    'VISO Training': 'Project Viso',
    'Rhythm': 'Rhythmm',
    'Text Vegas': 'Text Vegas',
}

# Month name mapping to handle abbreviations
MONTH_MAPPING = {
    'Jan': 'January',
    'Feb': 'February',
    'Mar': 'March',
    'Apr': 'April',
    'May': 'May',
    'Jun': 'June',
    'Jul': 'July',
    'Aug': 'August',
    'Sep': 'September',
    'Oct': 'October',
    'Nov': 'November',
    'Dec': 'December'
}

# Columns to ignore in the CSV
IGNORED_COLUMNS = ['Dept', 'Level', 'Internal Alc.', 'Avail. Bandwidth', 'Project Alc.']

def standardize_month_name(month_name):
    """Convert abbreviated month names to full month names"""
    if month_name in MONTH_MAPPING:
        return MONTH_MAPPING[month_name]
    return month_name  # Return original if not in mapping

def get_month_number(month_name):
    """Get the month number (1-12) from month name (handles abbreviations)"""
    standard_month = standardize_month_name(month_name)
    try:
        return list(calendar.month_name).index(standard_month)
    except ValueError:
        # Fallback for any format issues - try to match flexibly
        for i, full_month in enumerate(calendar.month_name):
            if i > 0 and (standard_month.lower() in full_month.lower() or 
                       full_month.lower() in standard_month.lower()):
                return i
        # If still not found, return January as default
        return 1

@st.cache_data(ttl=3600)
def fetch_all_actual_hours(month, year):
    """Fetch all actual hours data for the selected month/year"""
    try:
        # Format month string (e.g., "Mar 2025")
        month_abbr = calendar.month_abbr[get_month_number(month)]
        month_str = f"{month_abbr} {year}"
        
        # Get data from BigQuery
        report_df = bq.jira_tickets({"month": month_str})
        
        if not report_df.empty and "time_spent" in report_df.columns:
            # Convert seconds to hours (divide by 3600)
            report_df["actual_hours"] = report_df["time_spent"].astype(float) / 3600.0
            
            # Group by user and project
            grouped_df = report_df.groupby(["teric_name", "teric_project_name"])["actual_hours"] \
                                .sum() \
                                .round(2) \
                                .reset_index()
            
            # Create lookup dictionary
            return dict(zip(
                zip(grouped_df["teric_name"], grouped_df["teric_project_name"]),
                grouped_df["actual_hours"]
            ))
        
        return {}
    except Exception as e:
        st.warning(f"Error fetching actual hours: {str(e)}")
        return {}
    
@st.cache_data(ttl=3600)
def fetch_valid_users():
    """Cache user list to avoid repeated calls"""
    users_df = bq.get_users()
    return set(users_df["name"].unique()) if not users_df.empty else set()

def process_allocation_csv(df, selected_month, selected_year):
    """Process the uploaded CSV and extract project allocations"""
    with st.spinner("Processing allocation data and fetching actual hours..."):
        # Remove ignored columns if they exist
        for col in IGNORED_COLUMNS:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # Convert numeric columns to float if they're Decimal
        numeric_cols = [col for col in df.columns if col != 'Name']
        for col in numeric_cols:
            df[col] = df[col].astype(float)
        
        # Create an empty list to store the processed data
        processed_data = []
        
        # Get all potential project columns (exclude Name column)
        project_columns = [col for col in df.columns if col != 'Name']
        
        # Get users from BigQuery for validation (cached)
        valid_users = fetch_valid_users()
        
        # Fetch all actual hours data in a single query (cached)
        actual_hours_lookup = fetch_all_actual_hours(selected_month, selected_year)
        
        # Iterate through each row (employee)
        for _, row in df.iterrows():
            employee_name = row['Name']
            
            # Check each project column
            for project_col in project_columns:
                # If the employee has hours allocated for this project
                if pd.notna(row[project_col]) and row[project_col] > 0:
                    # Standardize project name using the mapping
                    # Standardize project name and handle Vars/Vita. specially
                    if project_col in PROJECT_NAME_MAPPING:
                        if project_col == 'Vars/Vita.':
                            # For Vars/Vita., sum hours from both projects
                            actual_hours = sum(
                                actual_hours_lookup.get((employee_name, p), 0)
                                for p in PROJECT_NAME_MAPPING['Vars/Vita.']
                            )
                            standardized_project = 'VARS/Vitawerks'  # Or 'Vars/Vita.' if you prefer
                        else:
                            # Normal project mapping
                            standardized_project = PROJECT_NAME_MAPPING[project_col]
                            actual_hours = actual_hours_lookup.get((employee_name, standardized_project), 0)
                    else:
                        standardized_project = project_col
                        actual_hours = actual_hours_lookup.get((employee_name, standardized_project), 0)
                    
                    # Round the actual hours
                    #actual_hours = round(actual_hours, 2)
                    # Expected hours from the CSV
                    expected_hours = float(row[project_col])
                    
                    
                    
                    # Calculate completion percentage
                    completion_percentage = 0
                    if expected_hours > 0:
                        completion_percentage = min(round((actual_hours / expected_hours) * 100, 2), 100)
                    
                    # Add to processed data
                    processed_data.append({
                        'Name': employee_name,
                        'Project': standardized_project,
                        'Expected Hours': expected_hours,
                        'Actual Hours': actual_hours,
                        'Completion %': completion_percentage
                    })
        
        # Create DataFrame from the processed data
        result_df = pd.DataFrame(processed_data)
        
        # Sort by Name and Project
        if not result_df.empty:
            result_df = result_df.sort_values(['Name', 'Project'])
        
        return result_df

def main():
    st.title("Project Allocation Report")
    
    # Use caching for months and years to improve performance
    @st.cache_data(ttl=3600)
    def get_months_and_years():
        # Get list of months and years from BigQuery
        months_df = bq.get_latest_months()
        months_set = set()
        years_set = set()
        
        if not months_df.empty and "month" in months_df.columns:
            for month_str in months_df["month"].unique():
                try:
                    parts = month_str.split()
                    if len(parts) == 2:
                        month_name = parts[0]
                        year = parts[1]
                        # Standardize month name for consistency
                        standard_month = standardize_month_name(month_name)
                        months_set.add(standard_month)
                        years_set.add(year)
                except Exception as e:
                    st.warning(f"Issue processing month string '{month_str}': {str(e)}")
        
        # Convert sets to sorted lists
        months_list = sorted(list(months_set), key=get_month_number)
        years_list = sorted(list(years_set))
        
        # If no months/years found in BigQuery, use defaults
        if not months_list:
            months_list = [
                'January', 'February', 'March', 'April', 'May', 'June', 
                'July', 'August', 'September', 'October', 'November', 'December'
            ]
        
        if not years_list:
            current_year = datetime.now().year
            years_list = [str(year) for year in range(current_year - 2, current_year + 1)]
            
        return months_list, years_list
    
    # Get months and years with caching
    months_list, years_list = get_months_and_years()
    
    # Create selection widgets for month and year
    col1, col2 = st.columns(2)
    
    with col1:
        selected_month = st.selectbox("Select Month", months_list)
    
    with col2:
        selected_year = st.selectbox("Select Year", years_list)
    
    # Check if a CSV file has already been uploaded and processed
    if 'allocation_df' not in st.session_state:
        # File uploader widget
        uploaded_file = st.file_uploader("Upload Project Allocation CSV", type=['csv'])
        
        if uploaded_file is not None:
            try:
                # Show progress indicator
                with st.spinner("Reading CSV file..."):
                    # Read the CSV file
                    df = pd.read_csv(uploaded_file)
                
                # Process the CSV data with progress indicator
                result_df = process_allocation_csv(df, selected_month, selected_year)
                
                # Store the processed DataFrame in session state
                st.session_state.allocation_df = result_df
                st.session_state.raw_df = df
                st.session_state.selected_month = selected_month
                st.session_state.selected_year = selected_year
                
                # Force a rerun to refresh the page without the file uploader
                st.rerun()
                
            except Exception as e:
                st.error(f"Error processing CSV file: {str(e)}")
    else:
        # Display the month and year for the loaded data
        st.info(f"Showing data for: {st.session_state.selected_month} {st.session_state.selected_year}")
        
        # Check if month/year selection has changed
        if selected_month != st.session_state.selected_month or selected_year != st.session_state.selected_year:
            # Re-process the data with the new month/year
            result_df = process_allocation_csv(
                st.session_state.raw_df, 
                selected_month, 
                selected_year
            )
            
            # Update session state
            st.session_state.allocation_df = result_df
            st.session_state.selected_month = selected_month
            st.session_state.selected_year = selected_year
        
        # Display the processed DataFrame
        if not st.session_state.allocation_df.empty:
            st.dataframe(st.session_state.allocation_df, use_container_width=True)
            
            # Summary statistics
            st.subheader("Summary Statistics")
            total_expected = st.session_state.allocation_df['Expected Hours'].sum()
            total_actual = st.session_state.allocation_df['Actual Hours'].sum()
            overall_completion = 0
            if total_expected > 0:
                overall_completion = min(round((total_actual / total_expected) * 100, 2), 100)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Expected Hours", f"{total_expected:.2f}")
            col2.metric("Total Actual Hours", f"{total_actual:.2f}")
            col3.metric("Overall Completion", f"{overall_completion}%")
            
            # Add download button
            csv_buffer = io.StringIO()
            st.session_state.allocation_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="Download Report as CSV",
                data=csv_data,
                file_name=f"project_report_{selected_month}_{selected_year}.csv",
                mime="text/csv",
            )
        else:
            st.warning("No project allocations found for the selected month and year.")
        
        # Option to reset and upload a new file
        if st.button("Upload a different allocation file"):
            # Clear the session state to allow uploading a new file
            for key in ['allocation_df', 'raw_df', 'selected_month', 'selected_year']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

if __name__ == "__main__":
    main()
