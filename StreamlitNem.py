import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import streamlit as st

def get_nem12_file_path():
    """
    Automatically find nem12data.csv in the same folder as the script
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'nem12data.csv')
    return file_path

def parse_nem12_csv_file(file_path):
    """
    Parse comma-delimited NEM12 file with multiple sections (E5, B5, E1)
    """
    data_records = []
    current_nmi = None
    current_section = None
    line_count = 0
    
    with open(file_path, 'r') as file:
        for line in file:
            line_count += 1
            fields = line.strip().split(',')
            
            if not fields:
                continue
                
            record_type = fields[0]
            
            if record_type == '200':
                # NMI Data Details Record
                if len(fields) >= 6:
                    current_nmi = fields[1]  # NMI is in field 1
                    section_info = fields[2]  # E5B5E1 in field 2
                    original_section = fields[3]  # Individual section (E5, B5, E1) in field 3
                    
                    # Rename sections to meaningful names with fallback for unmapped values
                    section_mapping = {
                        'E5': 'Import',
                        'B5': 'Export', 
                        'E1': 'Import',
                        'B1': 'Export',
                        'E2': 'Controlled Load'
                    }
                    current_section = section_mapping.get(original_section, 'Not Mapped')
                    
                    st.info(f"Found NMI: {current_nmi}, Section: {original_section} → {current_section}, Full section info: {section_info}")
                    
            elif record_type == '300':
                # Interval Data Record
                if current_nmi and current_section and len(fields) >= 3:
                    try:
                        # Date is in field 1
                        date_str = fields[1].strip()
                        
                        # Validate date
                        if not date_str or len(date_str) != 8:
                            continue
                            
                        date_obj = datetime.strptime(date_str, '%Y%m%d')
                        
                        # Interval values start from field 2 onwards
                        interval_values = []
                        for i in range(2, min(len(fields), 290)):  # Up to 288 intervals + 2 header fields
                            value_str = fields[i].strip()
                            # Remove any quotes or special characters
                            value_str = value_str.replace("'", "").replace('"', '').strip()
                            if value_str:
                                try:
                                    value = float(value_str)
                                    interval_values.append(value)
                                except ValueError:
                                    interval_values.append(np.nan)
                            else:
                                interval_values.append(np.nan)
                        
                        # Pad to 288 intervals if needed
                        while len(interval_values) < 288:
                            interval_values.append(np.nan)
                        
                        # Only keep first 288 values
                        interval_values = interval_values[:288]
                        
                        data_records.append({
                            'nmi': current_nmi,
                            'section': current_section,
                            'date': date_str,
                            'date_obj': date_obj,
                            'interval_values': interval_values
                        })
                        
                    except Exception as e:
                        st.warning(f"Error processing line {line_count}: {e}")
                        continue
    
    st.success(f"**Processing complete:**")
    st.write(f"- Total lines processed: {line_count:,}")
    st.write(f"- Valid data records: {len(data_records):,}")
    
    return data_records

def create_hourly_dataframe(data_records):
    """
    Convert 5-minute data to hourly aggregated data and filter to last 2 years
    """
    if not data_records:
        st.error("No valid data records found")
        return None
    
    # Convert to DataFrame with 5-minute data first
    df_5min_data = []
    
    for record in data_records:
        for i, value in enumerate(record['interval_values']):
            if not np.isnan(value):
                # Calculate timestamp for each 5-minute interval
                interval_time = record['date_obj'] + timedelta(minutes=5 * i)
                
                df_5min_data.append({
                    'nmi': record['nmi'],
                    'section': record['section'],
                    'timestamp': interval_time,
                    'date': record['date_obj'].date(),
                    'hour': interval_time.hour,
                    'minute': interval_time.minute,
                    'energy_kwh_5min': value,
                })
    
    if not df_5min_data:
        st.error("No valid interval data found")
        return None
        
    df_5min = pd.DataFrame(df_5min_data)
    
    # Find date range for filtering - now 2 years instead of 1
    if not df_5min.empty:
        max_date = df_5min['date'].max()
        min_date = df_5min['date'].min()
        cutoff_date = max_date - timedelta(days=730)  # 2 years back from latest date
        
        st.write(f"**Original date range:** {min_date} to {max_date}")
        st.write(f"**Keeping data from:** {cutoff_date} to {max_date} (last 2 years)")
        
        # Filter to last 2 years
        df_5min = df_5min[df_5min['date'] >= cutoff_date]
        st.write(f"**After 2-year filter:** {len(df_5min):,} 5-minute records")
    
    # Aggregate to hourly data
    st.subheader("Aggregating to Hourly Data")
    
    # Create hourly groups
    df_5min['hourly_group'] = df_5min['timestamp'].dt.floor('h')
    
    # Aggregate by hour, nmi, and section
    hourly_data = df_5min.groupby(['nmi', 'section', 'hourly_group']).agg({
        'energy_kwh_5min': ['sum', 'mean', 'min', 'max', 'count']
    }).round(6)
    
    # Flatten the column names
    hourly_data.columns = ['hourly_energy_kwh', 'avg_5min_energy_kwh', 'min_5min_energy_kwh', 'max_5min_energy_kwh', 'interval_count']
    
    # Reset index to get columns back
    hourly_df = hourly_data.reset_index()
    
    # Add date and hour columns for easier analysis
    hourly_df['date'] = hourly_df['hourly_group'].dt.date
    hourly_df['hour'] = hourly_df['hourly_group'].dt.hour
    hourly_df['day_of_week'] = hourly_df['hourly_group'].dt.day_name()
    hourly_df['month'] = hourly_df['hourly_group'].dt.month
    hourly_df['year'] = hourly_df['hourly_group'].dt.year
    
    # Calculate power metrics in kW
    hourly_df['avg_power_kw'] = hourly_df['avg_5min_energy_kwh'] * 12
    hourly_df['min_power_kw'] = hourly_df['min_5min_energy_kwh'] * 12
    hourly_df['max_power_kw'] = hourly_df['max_5min_energy_kwh'] * 12
    
    # Ensure all numeric columns are properly formatted as floats
    numeric_columns = ['hourly_energy_kwh', 'avg_5min_energy_kwh', 'min_5min_energy_kwh', 'max_5min_energy_kwh', 
                      'avg_power_kw', 'min_power_kw', 'max_power_kw']
    for col in numeric_columns:
        hourly_df[col] = pd.to_numeric(hourly_df[col], errors='coerce').round(6)
    
    st.success(f"**Final hourly records:** {len(hourly_df):,} (last 2 years only)")
    
    return hourly_df

def display_summary(hourly_df):
    """
    Display comprehensive summary of hourly NEM12 data in Streamlit
    """
    if hourly_df is None or hourly_df.empty:
        st.error("No hourly data available for summary")
        return
    
    st.header("📊 NEM12 Hourly Summary (Last 2 Years)")
    
    # Basic stats
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total NMIs", hourly_df['nmi'].nunique())
    with col2:
        st.metric("Total Sections", hourly_df['section'].nunique())
    with col3:
        st.metric("Total Days", hourly_df['date'].nunique())
    with col4:
        st.metric("Hourly Records", f"{len(hourly_df):,}")
    
    st.write(f"**Date Range:** {hourly_df['date'].min()} to {hourly_df['date'].max()}")
    st.write(f"**Sections Found:** {', '.join(sorted(hourly_df['section'].unique()))}")
    
    # Summary by Section
    st.subheader("Hourly Energy by Section")
    section_summary = hourly_df.groupby(['nmi', 'section']).agg({
        'hourly_energy_kwh': ['sum', 'mean', 'min', 'max', 'count'],
        'date': ['min', 'max', 'nunique']
    }).round(3)
    
    st.dataframe(section_summary, width='stretch')
    
    # Interactive data explorer
    st.subheader("📈 Data Explorer")
    
    col1, col2 = st.columns(2)
    with col1:
        selected_section = st.selectbox("Select Section", sorted(hourly_df['section'].unique()))
    with col2:
        selected_year = st.selectbox("Select Year", sorted(hourly_df['year'].unique()))
    
    filtered_data = hourly_df[(hourly_df['section'] == selected_section) & (hourly_df['year'] == selected_year)]
    
    if not filtered_data.empty:
        # Show metrics for filtered data
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(f"Total Energy ({selected_section})", 
                     f"{filtered_data['hourly_energy_kwh'].sum():,.1f} kWh")
        with col2:
            st.metric(f"Average Power", 
                     f"{filtered_data['avg_power_kw'].mean():.2f} kW")
        with col3:
            st.metric(f"Peak Power", 
                     f"{filtered_data['max_power_kw'].max():.2f} kW")
        
        # Show chart
        st.subheader(f"Hourly Energy Pattern - {selected_section} ({selected_year})")
        
        # Prepare data for chart - average by hour
        hourly_pattern = filtered_data.groupby('hour').agg({
            'hourly_energy_kwh': 'mean',
            'avg_power_kw': 'mean'
        }).reset_index()
        
        # Create tabs for different views
        tab1, tab2 = st.tabs(["📈 Line Chart", "📊 Data Table"])
        
        with tab1:
            st.line_chart(hourly_pattern.set_index('hour')['hourly_energy_kwh'])
        
        with tab2:
            st.dataframe(filtered_data[['date', 'hour', 'hourly_energy_kwh', 'avg_power_kw', 'min_power_kw', 'max_power_kw']].head(50))
    
    return section_summary

def process_nem12_file(file_path):
    """
    Complete processing pipeline for NEM12 files with hourly output (last 2 years)
    """
    st.info(f"Processing NEM12 file: {file_path}")
    
    # Parse the file
    data_records = parse_nem12_csv_file(file_path)
    
    if not data_records:
        st.error("No valid data found in the file")
        return None
    
    # Create hourly dataframe
    hourly_df = create_hourly_dataframe(data_records)
    
    return hourly_df

def main():
    """
    Main Streamlit app
    """
    st.set_page_config(
        page_title="NEM12 Data Processor",
        page_icon="⚡",
        layout="wide"
    )
    
    st.title("⚡ NEM12 Data Processor")
    st.markdown("""
    This application processes NEM12 energy data files, converts 5-minute interval data to hourly aggregates,
    and provides interactive analysis of the last 2 years of data.
    """)
    
    # File upload option
    st.sidebar.header("📁 Data Input")
    upload_option = st.sidebar.radio(
        "Select input method:",
        ["Use default file (nem12data.csv)", "Upload CSV file"]
    )
    
    file_path = None
    
    if upload_option == "Use default file (nem12data.csv)":
        file_path = get_nem12_file_path()
        st.sidebar.info(f"Using default file: {file_path}")
    else:
        uploaded_file = st.sidebar.file_uploader("Upload NEM12 CSV file", type=["csv"])
        if uploaded_file is not None:
            # Save uploaded file temporarily
            temp_path = "temp_nem12data.csv"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            file_path = temp_path
            st.sidebar.success(f"Uploaded: {uploaded_file.name}")
    
    # Process button
    if st.sidebar.button("🚀 Process NEM12 Data", type="primary"):
        if file_path and os.path.exists(file_path):
            with st.spinner("Processing NEM12 data..."):
                # Process the file
                hourly_df = process_nem12_file(file_path)
                
                if hourly_df is not None and not hourly_df.empty:
                    # Display summary
                    section_summary = display_summary(hourly_df)
                    
                    # Save processed data
                    st.subheader("💾 Save Processed Data")
                    
                    # Define custom sort order for sections
                    section_order = {'Import': 1, 'Export': 2, 'Controlled Load': 3, 'Not Mapped': 4}
                    hourly_df['section_order'] = hourly_df['section'].map(section_order)
                    
                    # Sort data
                    hourly_df = hourly_df.sort_values(['section_order', 'date', 'hour'])
                    hourly_df = hourly_df.drop('section_order', axis=1)
                    
                    # Select output columns
                    output_columns = [
                        'nmi', 'section', 'date', 'hour', 'day_of_week', 'month', 'year',
                        'hourly_energy_kwh', 'avg_power_kw', 'min_power_kw', 'max_power_kw',
                        'interval_count'
                    ]
                    
                    # Format numeric columns
                    numeric_columns = ['hourly_energy_kwh', 'avg_power_kw', 'min_power_kw', 'max_power_kw']
                    for col in numeric_columns:
                        hourly_df[col] = pd.to_numeric(hourly_df[col], errors='coerce')
                    
                    # Create download button
                    csv_data = hourly_df[output_columns].to_csv(index=False, float_format='%.6f')
                    
                    st.download_button(
                        label="📥 Download Hourly Data (CSV)",
                        data=csv_data,
                        file_name="nem12_hourly_summary.csv",
                        mime="text/csv"
                    )
                    
                    # Display final statistics
                    st.subheader("📋 Final Statistics")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Yearly Breakdown:**")
                        yearly_counts = hourly_df.groupby('year').size()
                        for year, count in yearly_counts.items():
                            st.write(f"- {year}: {count:,} hourly records")
                    
                    with col2:
                        st.write("**Energy by Section:**")
                        for section in ['Import', 'Export', 'Controlled Load', 'Not Mapped']:
                            if section in hourly_df['section'].unique():
                                section_data = hourly_df[hourly_df['section'] == section]
                                total_energy = section_data['hourly_energy_kwh'].sum()
                                avg_power = section_data['avg_power_kw'].mean()
                                st.write(f"- **{section}:** {total_energy:,.1f} kWh, {avg_power:.2f} kW avg")
                    
                    # Show sample data
                    with st.expander("🔍 View Sample Data (First 20 rows)"):
                        st.dataframe(hourly_df[output_columns].head(20))
                        
                else:
                    st.error("No valid data was processed.")
        else:
            st.error("Please select or upload a valid NEM12 CSV file.")
    
    # Display instructions when no processing is happening
    if not st.session_state.get('processed', False):
        st.info("👈 Configure your data input in the sidebar and click 'Process NEM12 Data' to begin.")

if __name__ == "__main__":
    main()