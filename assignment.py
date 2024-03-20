import pandas as pd
import pymysql

# Data extraction
def extract_data():
    # Connect to the database
    db = pymysql.connect(host='localhost', user='username', password='password', database='abc_utility')
    
    # Query to extract consumer data
    query = "SELECT * FROM consumer_data"
    
    # Execute query and fetch data
    cursor = db.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    
    # Close database connection
    db.close()
    
    # Convert data to DataFrame for further processing
    df = pd.DataFrame(data, columns=['Consumer ID', 'Name', 'Address', 'Contact Number', 'Email Address', 'Account Number', 'Meter Number', 'Tariff Plan', 'Consumption History', 'Payment Status'])
    
    return df

# Data mapping
def map_data(df):
    column_to_field_mapping = {
        'Consumer ID': 'Consumer ID',
        'Name': ['First Name', 'Last Name'],  # Assuming 'Name' includes both first and last name
        'Address': ['Address Line 1', 'Address Line 2'],  # Splitting address into two lines
        'Contact Number': 'Phone Number',
        'Email Address': 'Email Address',
    }
    
    # Map data columns to SMART360 fields
    mapped_data = pd.DataFrame()
    for column, fields in column_to_field_mapping.items():
        if isinstance(fields, list):
            mapped_data[fields[0]] = df[column].apply(lambda x: x.split()[0] if isinstance(x, str) else None)
            mapped_data[fields[1]] = df[column].apply(lambda x: x.split()[1] if isinstance(x, str) and len(x.split()) > 1 else None)
        else:
            mapped_data[fields] = df[column]
    
    return mapped_data

# Transformation and loading (example)
def load_data(mapped_data):
    # Example code to load data into SMART360 (using print statement as placeholder)
    for index, row in mapped_data.iterrows():
        print("Inserting data into SMART360:", row)

# Main function
def main():
    # Extract data from ABC Utility Company's databases
    consumer_data = extract_data()
    
    # Map extracted data to SMART360 fields
    mapped_data = map_data(consumer_data)
    
    # Load mapped data into SMART360
    load_data(mapped_data)

# Execute main function
if __name__ == "__main__":
    main()
