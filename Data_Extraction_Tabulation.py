#!/usr/bin/env python
# coding: utf-8

# In[58]:


import pandas as pd
# Load all sheets into a dictionary
file_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets.xlsx'
sheets_dict = pd.read_excel(file_path, sheet_name=None)  # 'sheet_name=None' reads all sheets

# Display sheet names
print(f"Sheets in the file: {list(sheets_dict.keys())}")

# Loop through each sheet
for sheet_name, df in sheets_dict.items():
    # Check if the word "Description" is in the header
    if "Description" or"                         Description                             " in df.columns:
        print(f"Processing sheet: {sheet_name}")
        # Perform processing here
        # Example: Display the first few rows of the dataframe
        #print(df.head())
    else:
        print(f"Skipping sheet: {sheet_name}, 'Description' not found in headers.")


# In[65]:


import pandas as pd
import re
#6-Warehouse.xlsx
# Define file paths
file_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets.xlsx'
output_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets_v1.xlsx'

# Define patterns
div_pattern = r"^\d{2}[- ].*"
sec_pattern = r'^(?:\d{6}.*|\d{5}.*|SECTION .*)'

# Load all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)  # 'sheet_name=None' reads all sheets

# Initialize an empty list to store processed DataFrames
processed_sheets = []

# Function to process a DataFrame
def process_dataframe(df):
    # Check for columns matching "Fair Priced Estimate," "Fair Priced estimate," or "Fair Price Estimate"
    columns_to_check = [col for col in df.columns if col.lower() in {"fair priced estimate ", "fair priced estimate",
                                                                     "fair priced etimate", "fair price estimate"}]
    if columns_to_check:
        column_name = columns_to_check[0]  # Get the actual column name in the DataFrame
        # Create new "Rate" and "Amount" columns from the matched column
        df["Rate"] = df[column_name].fillna("").astype(str).apply(lambda x: x.split()[0] if x else "")
        df["Amount"] = df[column_name].fillna("").astype(str).apply(lambda x: x.split()[1] if len(x.split()) > 1 else "")
        df.drop(columns=[column_name], inplace=True)  # Remove the old column

    df["Description"] = df["Description"].fillna("").astype(str)

    # Define the levels assignment function
    def assign_levels(row):
        if pd.notnull(row["Rate"]):  # Check if Rate is not null
            return "L4"
        elif re.match(div_pattern, row["Description"]):  # Match div_pattern
            return "L1"
        elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
            return "L2"
        elif row["Description"]:  # Check if Description is not empty
            return "L3"
        else:
            return None  # Leave as None if Description is null/empty

    # Apply the function to create the Levels column
    df["Levels"] = df.apply(assign_levels, axis=1)

    # Step 2: Handle consecutive L3s
    def add_consecutive_numbers(levels):
        result = []
        consecutive_count = 0
        for level in levels:
            if level == "L3":
                consecutive_count += 1
                if consecutive_count > 3:
                    result.append("L3")
                else:
                    result.append(f"L3.{consecutive_count}")
            else:
                consecutive_count = 0
                result.append(level)
        return result

    # Update Levels for consecutive L3s
    df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

    # Reset the index to ensure it's continuous and starts from 0
    df.reset_index(drop=True, inplace=True)

    # Add columns to hold the concatenated descriptions
    df['L4_Desc'] = ''
    df['L3_Desc'] = ''
    df['L2_Desc'] = ''
    df['L1_Desc'] = ''

    # Iterate through the DataFrame safely using iterrows()
    for i, row in df.iterrows():
        if row['Levels'] == 'L4':
            df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

            # Initialize variables for descriptions
            l2_description = None
            l1_description = None
            l3_descriptions = []
            encountered_levels = set()  # Set to track unique L3.x levels

            # Variable to keep the highest (most recent) L3.x level number
            highest_l3_level = 0

            # First, identify the highest L3.x level just above this L4
            j = i - 1
            while j >= 0:
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        highest_l3_level = max(highest_l3_level, l3_level_num)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected
                    break
                j -= 1

            # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
            j = i - 1
            while j >= 0 and (l1_description is None or l2_description is None):
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                # Collect L3 descriptions within the defined range
                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        if l3_level_num <= highest_l3_level and level not in encountered_levels:
                            l3_descriptions.append(desc)
                            encountered_levels.add(level)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected

                # Capture the first L2 and L1 descriptions found
                elif level == 'L2' and l2_description is None:
                    l2_description = desc
                elif level == 'L1' and l1_description is None:
                    l1_description = desc

                j -= 1  # Continue searching upwards

            # Reverse the list to maintain the order from L3.1 to the most recent L3.x
            l3_descriptions.reverse()

            # Assign the concatenated descriptions to their respective columns
            df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
            df.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
            df.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""

    def reverse_descriptions(desc):
        # Split the description by '/*/', reverse the list, and join it back together
        items = desc.split('/*/')
        items.reverse()  # Reverses the items in place
        return '/*/ '.join(items).strip()

    # Apply the function to the 'L3_Desc' column
    df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
    df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
    df = df[df['L4_Desc'] != '']
    df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

    # Selecting specific columns and rearranging them
    selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
    df_selected = df[selected_columns]

    # Drop rows with any null values in these columns
    df_cleaned = df_selected.dropna()

    return df_cleaned

# Process each sheet and append results
for sheet_name, df in sheets_dict.items():
    print(f"Processing sheet: {sheet_name}")
    try:
        processed_df = process_dataframe(df)
        processed_df['Sheet_Name'] = sheet_name  # Add a column to indicate the sheet name
        processed_sheets.append(processed_df)
    except Exception as e:
        print(f"Error processing sheet {sheet_name}: {e}")

# Combine all processed dataframes
if processed_sheets:
    final_df = pd.concat(processed_sheets, ignore_index=True)
    final_df.to_excel(output_path, index=False)
    print(f"All sheets processed and saved to {output_path}")
else:
    print("No sheets were processed.")


# In[66]:


# Assuming the DataFrame is named `df`
unique_values = final_df['L2_Desc'].unique()

# Print the unique values
print("Unique values in L1_Desc column:")
for value in unique_values:
    print(value)


# In[50]:


import pandas as pd
import re

# Define file paths
file_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets.xlsx'
output_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets_out_11.xlsx'
div_pattern = r'^\d{2}\s+[A-Z]+\s*\(.*\)$'
sec_pattern = r'^\d{6}\s+.*$' # Matches descriptions like "013100 PROJECT MANAGEMENT AND COORDINATION"

# Load all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)  # 'sheet_name=None' reads all sheets

# Initialize an empty list to store processed DataFrames
processed_sheets = []

def process_dataframe(df):
    # Check for columns matching "Fair Priced Estimate," "Fair Priced estimate," or "Fair Price Estimate"
    columns_to_check = [col for col in df.columns if col.lower() in {"fair priced estimate ", "fair priced estimate",
                                                                     "fair priced etimate", "fair price estimate"}]
    if columns_to_check:
        column_name = columns_to_check[0]  # Get the actual column name in the DataFrame
        # Create new "Rate" and "Amount" columns from the matched column
        df["Rate"] = df[column_name].fillna("").astype(str).apply(lambda x: x.split()[0] if x else "")
        df["Amount"] = df[column_name].fillna("").astype(str).apply(lambda x: x.split()[1] if len(x.split()) > 1 else "")
        df.drop(columns=[column_name], inplace=True)  # Remove the old column

    df["Description"] = df["Description"].fillna("").astype(str)

    # Define the levels assignment function
    def assign_levels(row):
        if pd.notnull(row["Rate"]):  # Check if Rate is not null
            return "L4"
        elif re.match(div_pattern, row["Description"]):  # Match div_pattern
            return "L1"
        elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
            return "L2"
        elif row["Description"]:  # Check if Description is not empty
            return "L3"
        else:
            return None  # Leave as None if Description is null/empty

    # Apply the function to create the Levels column
    df["Levels"] = df.apply(assign_levels, axis=1)

    # Step 1: Handle repeating L1 and L2 descriptions
    current_l1 = None
    current_l2 = None

    def update_descriptions(row):
        nonlocal current_l1, current_l2
        if row["Levels"] == "L1":
            current_l1 = row["Description"]
        elif row["Levels"] == "L2":
            current_l2 = row["Description"]
        
        row["L1_Desc"] = current_l1 if current_l1 else ""
        row["L2_Desc"] = current_l2 if current_l2 else ""
        return row

    df = df.apply(update_descriptions, axis=1)

    # Step 2: Handle consecutive L3s
    def add_consecutive_numbers(levels):
        result = []
        consecutive_count = 0
        for level in levels:
            if level == "L3":
                consecutive_count += 1
                if consecutive_count > 3:
                    result.append("L3")
                else:
                    result.append(f"L3.{consecutive_count}")
            else:
                consecutive_count = 0
                result.append(level)
        return result

    # Update Levels for consecutive L3s
    df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

    # Reset the index to ensure it's continuous and starts from 0
    df.reset_index(drop=True, inplace=True)

    # Add columns to hold the concatenated descriptions
    df['L4_Desc'] = ''
    df['L3_Desc'] = ''

    # Iterate through the DataFrame safely using iterrows()
    for i, row in df.iterrows():
        if row['Levels'] == 'L4':
            df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

            # Initialize variables for descriptions
            l3_descriptions = []
            encountered_levels = set()  # Set to track unique L3.x levels

            # Variable to keep the highest (most recent) L3.x level number
            highest_l3_level = 0

            # First, identify the highest L3.x level just above this L4
            j = i - 1
            while j >= 0:
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        highest_l3_level = max(highest_l3_level, l3_level_num)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected
                    break
                j -= 1

            # Restart the search from the point just above the L4 to collect L3 descriptions
            j = i - 1
            while j >= 0:
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                # Collect L3 descriptions within the defined range
                if level and level.startswith("L3") and level not in encountered_levels:  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        if l3_level_num <= highest_l3_level:
                            l3_descriptions.append(desc)
                            encountered_levels.add(level)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected

                j -= 1  # Continue searching upwards

            # Reverse the list to maintain the order from L3.1 to the most recent L3.x
            l3_descriptions.reverse()

            # Assign the concatenated descriptions to their respective columns
            df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)

    def reverse_descriptions(desc):
        # Split the description by '/*/', reverse the list, and join it back together
        items = desc.split('/*/')
        items.reverse()  # Reverses the items in place
        return '/*/ '.join(items).strip()

    # Apply the function to the 'L3_Desc' column
    df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
    df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
    df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

    # Selecting specific columns and rearranging them
    selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
    df_selected = df[selected_columns]

    # Drop rows with any null values in these columns
    df_cleaned = df_selected.dropna()

    return df_cleaned


# In[60]:





# In[42]:


final_df.head(100)


# In[22]:


import pandas as pd
import re
#6-Warehouse.xlsx
# Define file paths
file_path = 'C:\\Users\\yymahmoudali\\Desktop\\\Additional_BOQs\\BOQ_2\\23-PK2-MTH-BOQ-REV01.xlsx'
output_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets_out.xlsx'

# Define patterns
div_pattern = r"^\d{2}[- ].*"
sec_pattern = r'^(?:\d{6}.*|\d{5}.*|SECTION .*)'

# Load all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)  # 'sheet_name=None' reads all sheets

# Initialize an empty list to store processed DataFrames
processed_sheets = []

# Function to process a DataFrame
def process_dataframe(df):
    df["Description"] = df["Description"].fillna("").astype(str)

    # Define the levels assignment function
    def assign_levels(row):
        if pd.notnull(row["Rate"]):  # Check if Rate is not null
            return "L4"
        elif re.match(div_pattern, row["Description"]):  # Match div_pattern
            return "L1"
        elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
            return "L2"
        elif row["Description"]:  # Check if Description is not empty
            return "L3"
        else:
            return None  # Leave as None if Description is null/empty

    # Apply the function to create the Levels column
    df["Levels"] = df.apply(assign_levels, axis=1)

    # Step 2: Handle consecutive L3s
    def add_consecutive_numbers(levels):
        result = []
        consecutive_count = 0
        for level in levels:
            if level == "L3":
                consecutive_count += 1
                if consecutive_count > 3:
                    result.append("L3")
                else:
                    result.append(f"L3.{consecutive_count}")
            else:
                consecutive_count = 0
                result.append(level)
        return result

    # Update Levels for consecutive L3s
    df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

    # Reset the index to ensure it's continuous and starts from 0
    df.reset_index(drop=True, inplace=True)

    # Add columns to hold the concatenated descriptions
    df['L4_Desc'] = ''
    df['L3_Desc'] = ''
    df['L2_Desc'] = ''
    df['L1_Desc'] = ''

    # Iterate through the DataFrame safely using iterrows()
    for i, row in df.iterrows():
        if row['Levels'] == 'L4':
            df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

            # Initialize variables for descriptions
            l2_description = None
            l1_description = None
            l3_descriptions = []
            encountered_levels = set()  # Set to track unique L3.x levels

            # Variable to keep the highest (most recent) L3.x level number
            highest_l3_level = 0

            # First, identify the highest L3.x level just above this L4
            j = i - 1
            while j >= 0:
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        highest_l3_level = max(highest_l3_level, l3_level_num)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected
                    break
                j -= 1

            # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
            j = i - 1
            while j >= 0 and (l1_description is None or l2_description is None):
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                # Collect L3 descriptions within the defined range
                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        if l3_level_num <= highest_l3_level and level not in encountered_levels:
                            l3_descriptions.append(desc)
                            encountered_levels.add(level)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected

                # Capture the first L2 and L1 descriptions found
                elif level == 'L2' and l2_description is None:
                    l2_description = desc
                elif level == 'L1' and l1_description is None:
                    l1_description = desc

                j -= 1  # Continue searching upwards

            # Reverse the list to maintain the order from L3.1 to the most recent L3.x
            l3_descriptions.reverse()

            # Assign the concatenated descriptions to their respective columns
            df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
            df.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
            df.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""

    def reverse_descriptions(desc):
        # Split the description by '/*/', reverse the list, and join it back together
        items = desc.split('/*/')
        items.reverse()  # Reverses the items in place
        return '/*/ '.join(items).strip()

    # Apply the function to the 'L3_Desc' column
    df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
    df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
    df = df[df['L4_Desc'] != '']
    df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

    # Selecting specific columns and rearranging them
    selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
    df_selected = df[selected_columns]

    # Drop rows with any null values in these columns
    df_cleaned = df_selected.dropna()

    return df_cleaned

# Process each sheet and append results
for sheet_name, df in sheets_dict.items():
    print(f"Processing sheet: {sheet_name}")
    try:
        processed_df = process_dataframe(df)
        processed_df['Sheet_Name'] = sheet_name  # Add a column to indicate the sheet name
        processed_sheets.append(processed_df)
    except Exception as e:
        print(f"Error processing sheet {sheet_name}: {e}")

# Combine all processed dataframes
if processed_sheets:
    final_df = pd.concat(processed_sheets, ignore_index=True)
    final_df.to_excel(output_path, index=False)
    print(f"All sheets processed and saved to {output_path}")
else:
    print("No sheets were processed.")


# In[18]:


import pandas as pd
import re

# Define file paths
file_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets.xlsx'
output_path = 'C:\\Users\\yymahmoudali\\Desktop\\Appendix A - comparision sheets_out1.xlsx'

# Define patternsa
div_pattern = r"^\d{2}[- ].*"
sec_pattern = r'^(?:\d{6}.*|\d{5}.*|SECTION .*)'

# Load all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)  # 'sheet_name=None' reads all sheets

# Initialize an empty list to store processed DataFrames
processed_sheets = []

def process_dataframe(df):
    # Check for columns matching "Fair Priced Estimate," "Fair Priced estimate," or "Fair Price Estimate"
    columns_to_check = [col for col in df.columns if col.lower() in {"Fair priced estimate ","fair priced estimate","Fair priced etimate","Fair priced estimate ", "fair price estimate","Fair priced estimate "}]
    if columns_to_check:
        column_name = columns_to_check[0]  # Get the actual column name in the DataFrame
        # Create new "Rate" and "Amount" columns from the matched column
        df["Rate"] = df[column_name].fillna("").astype(str).apply(lambda x: x.split()[0] if x else "")
        df["Amount"] = df[column_name].fillna("").astype(str).apply(lambda x: x.split()[1] if len(x.split()) > 1 else "")
        df.drop(columns=[column_name], inplace=True)  # Remove the old column

    df["Description"] = df["Description"].fillna("").astype(str)

    # Define the levels assignment function
    def assign_levels(row):
        if pd.notnull(row["Rate"]):  # Check if Rate is not null
            return "L4"
        elif re.match(div_pattern, row["Description"]):  # Match div_pattern
            return "L1"
        elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
            return "L2"
        elif row["Description"]:  # Check if Description is not empty
            return "L3"
        else:
            return None  # Leave as None if Description is null/empty

    # Apply the function to create the Levels column
    df["Levels"] = df.apply(assign_levels, axis=1)

    # Step 2: Handle consecutive L3s
    def add_consecutive_numbers(levels):
        result = []
        consecutive_count = 0
        for level in levels:
            if level == "L3":
                consecutive_count += 1
                if consecutive_count > 3:
                    result.append("L3")
                else:
                    result.append(f"L3.{consecutive_count}")
            else:
                consecutive_count = 0
                result.append(level)
        return result

    # Update Levels for consecutive L3s
    df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

    # Reset the index to ensure it's continuous and starts from 0
    df.reset_index(drop=True, inplace=True)

    # Add columns to hold the concatenated descriptions
    df['L4_Desc'] = ''
    df['L3_Desc'] = ''
    df['L2_Desc'] = ''
    df['L1_Desc'] = ''

    # Iterate through the DataFrame safely using iterrows()
    for i, row in df.iterrows():
        if row['Levels'] == 'L4':
            df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

            # Initialize variables for descriptions
            l2_description = None
            l1_description = None
            l3_descriptions = []
            encountered_levels = set()  # Set to track unique L3.x levels

            # Variable to keep the highest (most recent) L3.x level number
            highest_l3_level = 0

            # First, identify the highest L3.x level just above this L4
            j = i - 1
            while j >= 0:
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        highest_l3_level = max(highest_l3_level, l3_level_num)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected
                    break
                j -= 1

            # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
            j = i - 1
            while j >= 0 and (l1_description is None or l2_description is None):
                level = df.loc[j, 'Levels']
                desc = df.loc[j, 'Description']

                # Collect L3 descriptions within the defined range
                if level and level.startswith('L3'):  # Ensure level is not None
                    try:
                        l3_level_num = int(level.split('.')[1])
                        if l3_level_num <= highest_l3_level and level not in encountered_levels:
                            l3_descriptions.append(desc)
                            encountered_levels.add(level)
                    except (IndexError, ValueError):
                        pass  # Handle cases where L3 format is unexpected

                # Capture the first L2 and L1 descriptions found
                elif level == 'L2' and l2_description is None:
                    l2_description = desc
                elif level == 'L1' and l1_description is None:
                    l1_description = desc

                j -= 1  # Continue searching upwards

            # Reverse the list to maintain the order from L3.1 to the most recent L3.x
            l3_descriptions.reverse()

            # Assign the concatenated descriptions to their respective columns
            df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
            df.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
            df.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""

    def reverse_descriptions(desc):
        # Split the description by '/*/', reverse the list, and join it back together
        items = desc.split('/*/')
        items.reverse()  # Reverses the items in place
        return '/*/ '.join(items).strip()

    # Apply the function to the 'L3_Desc' column
    df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
    df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
    df = df[df['L4_Desc'] != '']
    df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

    # Selecting specific columns and rearranging them
    selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
    df_selected = df[selected_columns]

    # Drop rows with any null values in these columns
    df_cleaned = df_selected.dropna()

    return df_cleaned

# Process each sheet and append results
for sheet_name, df in sheets_dict.items():
    print(f"Processing sheet: {sheet_name}")
    try:
        processed_df = process_dataframe(df)
        processed_df['Sheet_Name'] = sheet_name  # Add a column to indicate the sheet name
        processed_sheets.append(processed_df)
    except Exception as e:
        print(f"Error processing sheet {sheet_name}: {e}")

# Combine all processed dataframes
if processed_sheets:
    final_df = pd.concat(processed_sheets, ignore_index=True)
    final_df.to_excel(output_path, index=False)
    print(f"All sheets processed and saved to {output_path}")
else:
    print("No sheets were processed.")


# In[17]:


df_cleaned.head()


# In[23]:


# Assuming the DataFrame is named `df`
unique_values = final_df['L1_Desc'].unique()

# Print the unique values
print("Unique values in L1_Desc column:")
for value in unique_values:
    print(value)


# In[28]:


print("Row Index:", i)
print("L4 Description:", row["Description"])
print("L2 Description Found:", l2_description)
print("L1 Description Found:", l1_description)
print("L3 Descriptions:", l3_descriptions)


# ## Merging

# In[331]:


import pandas as pd

# Load the files
file1 = pd.read_excel('C:\\Users\\yymahmoudali\\Downloads\\Additional_BOQV1.xlsx', engine='openpyxl')  # Larger file
file2 = pd.read_excel('C:\\Users\\yymahmoudali\\Desktop\\MappingToCurrentLOI\\boq_to_update.xlsx', engine='openpyxl')  # Smaller file
file3 = pd.read_excel('C:\\Users\\yymahmoudali\\Desktop\\HBOQ_Old.xlsx', engine='openpyxl')


# In[332]:


df_list1 = file1.values.tolist()
print(len(df_list))  # Outputs the entire DataFrame as a list of rows
print(len(df_list[0]))


# In[333]:


df_list2 = file2.values.tolist()
print(len(df_list2))  # Outputs the entire DataFrame as a list of rows
print(len(df_list2[0]))


# In[334]:


df_list23 = file3.values.tolist()
print(len(df_list3))  # Outputs the entire DataFrame as a list of rows
print(len(df_list3[0]))


# In[320]:


df_list1 = [row + [None] * 18 for row in df_list1]


# In[322]:


#df_list1 = file1.values.tolist()
print(len(df_list1))  # Outputs the entire DataFrame as a list of rows
print(len(df_list1[31]))


# In[323]:


appended_list = df_list1 + df_list2


# In[324]:


print(len(appended_list))  # Outputs the entire DataFrame as a list of rows
print(len(appended_list[31]))


# In[326]:


column_names = file2.columns

print(column_names)


# In[327]:


df = pd.DataFrame(appended_list, columns=column_names)


# In[330]:


df.to_csv("C:\\Users\\yymahmoudali\\Downloads\\new_excel.csv", index=False)


# In[ ]:


# Get all unique columns
all_columns = list(set(file1.columns).union(set(file2.columns)))

# Reindex both DataFrames to have the same columns
file1 = file1.reindex(columns=all_columns)
file2 = file2.reindex(columns=all_columns)

# Append the files
combined = pd.concat([file1, file2], ignore_index=True)

# Save the combined file
combined.to_csv('combined_file.csv', index=False)

print("Files appended successfully!")


# In[297]:


import pandas as pd

# Load the files
file1 = pd.read_csv('C:\\Users\\yymahmoudali\\Downloads\\Additional_BOQV1.xlsx')  # Larger file
file2 = pd.read_csv('C:\\Users\\yymahmoudali\\Desktop\\MappingToCurrentLOI\\boq_to_update.xlsx')  # Smaller file

# Get all unique columns
all_columns = list(set(file1.columns).union(set(file2.columns)))

# Reindex both DataFrames to have the same columns
file1 = file1.reindex(columns=all_columns)
file2 = file2.reindex(columns=all_columns)

# Append the files
combined = pd.concat([file1, file2], ignore_index=True)

# Save the combined file
combined.to_csv('combined_file.csv', index=False)

print("Files appended successfully!")


# In[294]:


excel_files = ['C:\\Users\\yymahmoudali\\Downloads\\Additional_BOQV1.xlsx', 
               'C:\\Users\\yymahmoudali\\Desktop\\MappingToCurrentLOI\\boq_to_update.xlsx'
              ]

# Initialize an empty DataFrame to hold the merged data
merged_data = pd.DataFrame()

# Loop through each file and append its data to the merged_data DataFrame
for file in excel_files:
    data = pd.read_excel(file)  # Read the Excel file
    merged_data = pd.concat([merged_data, data], ignore_index=True)  # Append the data

# Save the merged data to a new Excel file
output_file = 'C:\\Users\\yymahmoudali\\Desktop\\Additional_BOQs\\BOQ_2\\Additional_boqs_1.xlsx'
merged_data.to_excel(output_file, index=False)

print(f"All files have been merged into {output_file}")


# In[296]:


import pandas as pd

# List of Excel files to merge
excel_files = [
    'C:\\Users\\yymahmoudali\\Downloads\\Additional_BOQV1.xlsx',
    'C:\\Users\\yymahmoudali\\Desktop\\MappingToCurrentLOI\\boq_to_update.xlsx'
]

# Initialize a list to hold DataFrames
dataframes = []

# Loop through each file and read its data
for file in excel_files:
    print(f"Reading file: {file}")
    data = pd.read_excel(file, engine='openpyxl')  # Use openpyxl for .xlsx files
    dataframes.append(data)

# Merge all DataFrames at once
merged_data = pd.concat(dataframes, ignore_index=True)

# Save the merged data to a new Excel file
output_file = 'C:\\Users\\yymahmoudali\\Desktop\\Additional_BOQs\\BOQ_2\\Additional_boqs_1.xlsx'
merged_data.to_excel(output_file, index=False, engine='openpyxl')  # Use openpyxl for writing

print(f"All files have been merged into {output_file}")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[208]:


import pandas as pd
import re

# Define file path
file_path = 'C:\\Users\\yymahmoudali\\Downloads\\Copy of 21-PK4-LI-BOQ-REV01.xlsx'
output_path = 'C:\\Users\\yymahmoudali\\Desktop\\p2.3_tabularformat_combined.xlsx'

# Define patterns
div_pattern = r"^\d{2}[- ].*"
sec_pattern = r'^(?:\d{6}.*|\d{5}.*|SECTION .*)'

# Load all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)  # 'sheet_name=None' reads all sheets

# Initialize an empty list to store processed DataFrames
processed_sheets = []

# Process each sheet
for sheet_name, df in sheets_dict.items():
    if "Description" in df.columns:
        print(f"Processing sheet: {sheet_name}")
        
        # Step 1: Ensure 'Description' is treated as a string
        df["Description"] = df["Description"].fillna("").astype(str)

        # Define the levels assignment function
        def assign_levels(row):
            if pd.notnull(row["Rate"]):  # Check if Rate is not null
                return "L4"
            elif re.match(div_pattern, row["Description"]):  # Match div_pattern
                return "L1"
            elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
                return "L2"
            elif row["Description"]:  # Check if Description is not empty
                return "L3"
            else:
                return None  # Leave as None if Description is null/empty

        # Apply the function to create the Levels column
        df["Levels"] = df.apply(assign_levels, axis=1)

        # Step 2: Handle consecutive L3s
        def add_consecutive_numbers(levels):
            result = []
            consecutive_count = 0
            for level in levels:
                if level == "L3":
                    consecutive_count += 1
                    if consecutive_count > 3:
                        result.append("L3")
                    else:
                        result.append(f"L3.{consecutive_count}")
                else:
                    consecutive_count = 0
                    result.append(level)
            return result

        # Update Levels for consecutive L3s
        df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

        # Reset the index to ensure it's continuous and starts from 0
        df.reset_index(drop=True, inplace=True)

        # Add columns to hold the concatenated descriptions
        df['L4_Desc'] = ''
        df['L3_Desc'] = ''
        df['L2_Desc'] = ''
        df['L1_Desc'] = ''

        # Iterate through the DataFrame safely using iterrows()
        for i, row in df.iterrows():
            if row['Levels'] == 'L4':
                df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

                # Initialize variables for descriptions
                l2_description = None
                l1_description = None
                l3_descriptions = []
                encountered_levels = set()  # Set to track unique L3.x levels

                # Variable to keep the highest (most recent) L3.x level number
                highest_l3_level = 0

                # First, identify the highest L3.x level just above this L4
                j = i - 1
                while j >= 0:
                    level = df.loc[j, 'Levels']
                    desc = df.loc[j, 'Description']

                    if level and level.startswith('L3'):  # Ensure level is not None
                        try:
                            l3_level_num = int(level.split('.')[1])
                            highest_l3_level = max(highest_l3_level, l3_level_num)
                        except (IndexError, ValueError):
                            pass  # Handle cases where L3 format is unexpected
                        break
                    j -= 1

                # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
                j = i - 1
                while j >= 0 and (l1_description is None or l2_description is None):
                    level = df.loc[j, 'Levels']
                    desc = df.loc[j, 'Description']

                    # Collect L3 descriptions within the defined range
                    if level and level.startswith('L3'):  # Ensure level is not None
                        try:
                            l3_level_num = int(level.split('.')[1])
                            if l3_level_num <= highest_l3_level and level not in encountered_levels:
                                l3_descriptions.append(desc)
                                encountered_levels.add(level)
                        except (IndexError, ValueError):
                            pass  # Handle cases where L3 format is unexpected

                    # Capture the first L2 and L1 descriptions found
                    elif level == 'L2' and l2_description is None:
                        l2_description = desc
                    elif level == 'L1' and l1_description is None:
                        l1_description = desc

                    j -= 1  # Continue searching upwards

                # Reverse the list to maintain the order from L3.1 to the most recent L3.x
                l3_descriptions.reverse()

                # Assign the concatenated descriptions to their respective columns
                df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
                df.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
                df.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""

        def reverse_descriptions(desc):
            # Split the description by '/*/', reverse the list, and join it back together
            items = desc.split('/*/')
            items.reverse()  # Reverses the items in place
            return '/*/ '.join(items).strip()

        # Apply the function to the 'L3_Desc' column
        df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
        df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
        df = df[df['L4_Desc'] != '']
        df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

        # Selecting specific columns and rearranging them
        selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
        df_selected = df[selected_columns]

        # Drop rows with any null values in these columns
        df_cleaned = df_selected.dropna()
        # df_cleaned['Trade']='EL'

        # Append the processed sheet to the list
        processed_sheets.append(df_cleaned)

# Concatenate all processed sheets
if processed_sheets:
    final_df = pd.concat(processed_sheets, ignore_index=True)
    # Save the combined DataFrame to an Excel file
    final_df.to_excel(output_path, index=False)
    print(f"Combined data saved to: {output_path}")
else:
    print("No sheets with 'Description' in headers were found.")


# In[209]:


df_cleaned.head(100)


# In[200]:


div_pattern = r"^\d{2}[- ].*"
sec_pattern = r'^(?:\d{6}.*|\d{5}.*|SECTION .*)'
# Step 1: Ensure 'Description' is treated as a string
#df_test["Description"] = df_test["Description"].fillna("").astype(str)

# Define the levels assignment function
def assign_levels(row):
   if pd.notnull(row["Rate"]):  # Check if Rate is not null
       return "L4"
   elif re.match(div_pattern, row["Description"]):  # Match div_pattern
       return "L1"
   elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
       return "L2"
   elif row["Description"]:  # Check if Description is not empty
       return "L3"
   else:
       return None  # Leave as None if Description is null/empty

# Apply the function to create the Levels column
df_test["Levels"] = df_test.apply(assign_levels, axis=1)

# Step 2: Handle consecutive L3s
def add_consecutive_numbers(levels):
   result = []
   consecutive_count = 0
   for level in levels:
       if level == "L3":
           consecutive_count += 1
           if consecutive_count > 3:
               result.append("L3")
           else:
               result.append(f"L3.{consecutive_count}")
       else:
           consecutive_count = 0
           result.append(level)
   return result

# Update Levels for consecutive L3s
df_test["Levels"] = add_consecutive_numbers(df_test["Levels"].tolist())

# Reset the index to ensure it's continuous and starts from 0
df_test.reset_index(drop=True, inplace=True)

# Add columns to hold the concatenated descriptions
df_test['L4_Desc'] = ''
df_test['L3_Desc'] = ''
df_test['L2_Desc'] = ''
df_test['L1_Desc'] = ''

# Iterate through the DataFrame safely using iterrows()
for i, row in df_test.iterrows():
   if row['Levels'] == 'L4':
       df_test.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

       # Initialize variables for descriptions
       l2_description = None
       l1_description = None
       l3_descriptions = []
       encountered_levels = set()  # Set to track unique L3.x levels

       # Variable to keep the highest (most recent) L3.x level number
       highest_l3_level = 0

       # First, identify the highest L3.x level just above this L4
       j = i - 1
       while j >= 0:
           level = df_test.loc[j, 'Levels']
           desc = df_test.loc[j, 'Description']

           if level and level.startswith('L3'):  # Ensure level is not None
               try:
                   l3_level_num = int(level.split('.')[1])
                   highest_l3_level = max(highest_l3_level, l3_level_num)
               except (IndexError, ValueError):
                   pass  # Handle cases where L3 format is unexpected
               break
           j -= 1

       # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
       j = i - 1
       while j >= 0 and (l1_description is None or l2_description is None):
           level = df_test.loc[j, 'Levels']
           desc = df_test.loc[j, 'Description']

           # Collect L3 descriptions within the defined range
           if level and level.startswith('L3'):  # Ensure level is not None
               try:
                   l3_level_num = int(level.split('.')[1])
                   if l3_level_num <= highest_l3_level and level not in encountered_levels:
                       l3_descriptions.append(desc)
                       encountered_levels.add(level)
               except (IndexError, ValueError):
                   pass  # Handle cases where L3 format is unexpected

           # Capture the first L2 and L1 descriptions found
           elif level == 'L2' and l2_description is None:
               l2_description = desc
           elif level == 'L1' and l1_description is None:
               l1_description = desc

           j -= 1  # Continue searching upwards

       # Reverse the list to maintain the order from L3.1 to the most recent L3.x
       l3_descriptions.reverse()

       # Assign the concatenated descriptions to their respective columns
       df_test.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
       df_test.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
       df_test.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""
       
def reverse_descriptions(desc):
   # Split the description by '/*/', reverse the list, and join it back together
   items = desc.split('/*/')
   items.reverse()  # Reverses the items in place
   return '/*/ '.join(items).strip()

# Apply the function to the 'L3_Desc' column
df_test['L3_Desc'] = df_test['L3_Desc'].apply(reverse_descriptions)
df_test = df_test[df_test['L4_Desc'].notna() & (df_test['L4_Desc'] != '')]
df_test = df_test[df_test['L4_Desc'] != '']
df_test = df_test[~df_test['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

# Selecting specific columns and rearranging them
selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc','Unit','Rate','Qty','Amount']
df_selected = df_test[selected_columns]

# Drop rows with any null values in these columns
df_cleaned = df_selected.dropna()
#df_cleaned['Trade']='EL'
df_cleaned.to_excel('C:\\Users\\yymahmoudali\\Desktop\\p2.3_tabularformat.xlsx', index=False)


# In[ ]:





# In[195]:


import pandas as pd
import re

# Define patterns
div_pattern = r"^\d{2}[- ].*"
sec_pattern = r"^(?:\d{6}.*|\d{5}.*|SECTION .*)"

# Path to the Excel file
file_path = 'C:\\Users\\yymahmoudali\\Downloads\\Copy of 21-PK4-LI-BOQ-REV01.xlsx'

# Function to process sheets if "Description" is in the header
def process_sheet(sheet_name, df):
    # Check if any column includes the word "Description"
    if any("Description" in col for col in df.columns):
        print(f"Processing sheet: {sheet_name}")
        
        # Step 1: Ensure 'Description' is treated as a string
        df["Description"] = df["Description"].fillna("").astype(str)

        # Define the levels assignment function
        def assign_levels(row):
            if pd.notnull(row["Rate"]):  # Check if Rate is not null
                return "L4"
            elif re.match(div_pattern, row["Description"]):  # Match div_pattern
                return "L1"
            elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
                return "L2"
            elif row["Description"]:  # Check if Description is not empty
                return "L3"
            else:
                return None  # Leave as None if Description is null/empty

        # Apply the function to create the Levels column
        df["Levels"] = df.apply(assign_levels, axis=1)

        # Step 2: Handle consecutive L3s
        def add_consecutive_numbers(levels):
            result = []
            consecutive_count = 0
            for level in levels:
                if level == "L3":
                    consecutive_count += 1
                    if consecutive_count > 3:
                        result.append("L3")
                    else:
                        result.append(f"L3.{consecutive_count}")
                else:
                    consecutive_count = 0
                    result.append(level)
            return result

        # Update Levels for consecutive L3s
        df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

        # Reset the index to ensure it's continuous and starts from 0
        df.reset_index(drop=True, inplace=True)

        # Add columns to hold the concatenated descriptions
        df['L4_Desc'] = ''
        df['L3_Desc'] = ''
        df['L2_Desc'] = ''
        df['L1_Desc'] = ''

        # Iterate through the DataFrame safely using iterrows()
        for i, row in df.iterrows():
            if row['Levels'] == 'L4':
                df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

                # Initialize variables for descriptions
                l2_description = None
                l1_description = None
                l3_descriptions = []
                encountered_levels = set()  # Set to track unique L3.x levels

                # Variable to keep the highest (most recent) L3.x level number
                highest_l3_level = 0

                # First, identify the highest L3.x level just above this L4
                j = i - 1
                while j >= 0:
                    level = df.loc[j, 'Levels']
                    desc = df.loc[j, 'Description']

                    if level and level.startswith('L3'):  # Ensure level is not None
                        try:
                            l3_level_num = int(level.split('.')[1])
                            highest_l3_level = max(highest_l3_level, l3_level_num)
                        except (IndexError, ValueError):
                            pass  # Handle cases where L3 format is unexpected
                        break
                    j -= 1

                # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
                j = i - 1
                while j >= 0 and (l1_description is None or l2_description is None):
                    level = df.loc[j, 'Levels']
                    desc = df.loc[j, 'Description']

                    # Collect L3 descriptions within the defined range
                    if level and level.startswith('L3'):  # Ensure level is not None
                        try:
                            l3_level_num = int(level.split('.')[1])
                            if l3_level_num <= highest_l3_level and level not in encountered_levels:
                                l3_descriptions.append(desc)
                                encountered_levels.add(level)
                        except (IndexError, ValueError):
                            pass  # Handle cases where L3 format is unexpected

                    # Capture the first L2 and L1 descriptions found
                    elif level == 'L2' and l2_description is None:
                        l2_description = desc
                    elif level == 'L1' and l1_description is None:
                        l1_description = desc

                    j -= 1  # Continue searching upwards

                # Reverse the list to maintain the order from L3.1 to the most recent L3.x
                l3_descriptions.reverse()

                # Assign the concatenated descriptions to their respective columns
                df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
                df.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
                df.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""

        def reverse_descriptions(desc):
            # Split the description by '/*/', reverse the list, and join it back together
            items = desc.split('/*/')
            items.reverse()  # Reverses the items in place
            return '/*/ '.join(items).strip()

        # Apply the function to the 'L3_Desc' column
        df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
        df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
        df = df[df['L4_Desc'] != '']
        df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

        # Selecting specific columns and rearranging them
        selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
        df_selected = df[selected_columns]

        # Drop rows with any null values in these columns
        df_cleaned = df_selected.dropna()
        # Save to Excel
        output_path = f'C:\\path_to_save\\{sheet_name}_processed.xlsx'
        df_cleaned.to_excel(output_path, index=False)
        print(f"Sheet {sheet_name} processed and saved to {output_path}")
    else:
        print(f"Skipping sheet: {sheet_name} (no 'Description' column)")

# Read all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)

# Process each sheet
for sheet_name, df in sheets_dict.items():
    process_sheet(sheet_name, df)


# In[179]:


import pandas as pd
import re

# Define patterns
div_pattern = r"^\d{2}[- ].*"
sec_pattern = r"^(?:\d{6}.*|\d{5}.*|SECTION .*)"

# Path to the Excel file
file_path = 'your_excel_file.xlsx'

# Function to process sheets if "Description" is in the header
def process_sheet(sheet_name, df):
    # Check if any column includes the word "Description"
    if any("Description" in col for col in df.columns):
        print(f"Processing sheet: {sheet_name}")
        
        # Step 1: Ensure 'Description' is treated as a string
        df["Description"] = df["Description"].fillna("").astype(str)

        # Define the levels assignment function
        def assign_levels(row):
            if pd.notnull(row["Rate"]):  # Check if Rate is not null
                return "L4"
            elif re.match(div_pattern, row["Description"]):  # Match div_pattern
                return "L1"
            elif re.match(sec_pattern, row["Description"]):  # Match sec_pattern
                return "L2"
            elif row["Description"]:  # Check if Description is not empty
                return "L3"
            else:
                return None  # Leave as None if Description is null/empty

        # Apply the function to create the Levels column
        df["Levels"] = df.apply(assign_levels, axis=1)

        # Step 2: Handle consecutive L3s
        def add_consecutive_numbers(levels):
            result = []
            consecutive_count = 0
            for level in levels:
                if level == "L3":
                    consecutive_count += 1
                    if consecutive_count > 3:
                        result.append("L3")
                    else:
                        result.append(f"L3.{consecutive_count}")
                else:
                    consecutive_count = 0
                    result.append(level)
            return result

        # Update Levels for consecutive L3s
        df["Levels"] = add_consecutive_numbers(df["Levels"].tolist())

        # Reset the index to ensure it's continuous and starts from 0
        df.reset_index(drop=True, inplace=True)

        # Add columns to hold the concatenated descriptions
        df['L4_Desc'] = ''
        df['L3_Desc'] = ''
        df['L2_Desc'] = ''
        df['L1_Desc'] = ''

        # Iterate through the DataFrame safely using iterrows()
        for i, row in df.iterrows():
            if row['Levels'] == 'L4':
                df.at[i, 'L4_Desc'] = row['Description']  # Set L4 description

                # Initialize variables for descriptions
                l2_description = None
                l1_description = None
                l3_descriptions = []
                encountered_levels = set()  # Set to track unique L3.x levels

                # Variable to keep the highest (most recent) L3.x level number
                highest_l3_level = 0

                # First, identify the highest L3.x level just above this L4
                j = i - 1
                while j >= 0:
                    level = df.loc[j, 'Levels']
                    desc = df.loc[j, 'Description']

                    if level and level.startswith('L3'):  # Ensure level is not None
                        try:
                            l3_level_num = int(level.split('.')[1])
                            highest_l3_level = max(highest_l3_level, l3_level_num)
                        except (IndexError, ValueError):
                            pass  # Handle cases where L3 format is unexpected
                        break
                    j -= 1

                # Restart the search from the point just above the L4 to collect L1, L2, and L3 descriptions
                j = i - 1
                while j >= 0 and (l1_description is None or l2_description is None):
                    level = df.loc[j, 'Levels']
                    desc = df.loc[j, 'Description']

                    # Collect L3 descriptions within the defined range
                    if level and level.startswith('L3'):  # Ensure level is not None
                        try:
                            l3_level_num = int(level.split('.')[1])
                            if l3_level_num <= highest_l3_level and level not in encountered_levels:
                                l3_descriptions.append(desc)
                                encountered_levels.add(level)
                        except (IndexError, ValueError):
                            pass  # Handle cases where L3 format is unexpected

                    # Capture the first L2 and L1 descriptions found
                    elif level == 'L2' and l2_description is None:
                        l2_description = desc
                    elif level == 'L1' and l1_description is None:
                        l1_description = desc

                    j -= 1  # Continue searching upwards

                # Reverse the list to maintain the order from L3.1 to the most recent L3.x
                l3_descriptions.reverse()

                # Assign the concatenated descriptions to their respective columns
                df.at[i, 'L3_Desc'] = '/*/'.join(str(desc) for desc in l3_descriptions)
                df.at[i, 'L2_Desc'] = l2_description if l2_description is not None else ""
                df.at[i, 'L1_Desc'] = l1_description if l1_description is not None else ""

        def reverse_descriptions(desc):
            # Split the description by '/*/', reverse the list, and join it back together
            items = desc.split('/*/')
            items.reverse()  # Reverses the items in place
            return '/*/ '.join(items).strip()

        # Apply the function to the 'L3_Desc' column
        df['L3_Desc'] = df['L3_Desc'].apply(reverse_descriptions)
        df = df[df['L4_Desc'].notna() & (df['L4_Desc'] != '')]
        df = df[df['L4_Desc'] != '']
        df = df[~df['L3_Desc'].str.contains('Bill|bill', case=False, na=False)]

        # Selecting specific columns and rearranging them
        selected_columns = ['L1_Desc', 'L2_Desc', 'L3_Desc', 'L4_Desc', 'Unit', 'Rate', 'Qty', 'Amount']
        df_selected = df[selected_columns]

        # Drop rows with any null values in these columns
        df_cleaned = df_selected.dropna()
        # Save to Excel
        output_path = f'C:\\path_to_save\\{sheet_name}_processed.xlsx'
        df_cleaned.to_excel(output_path, index=False)
        print(f"Sheet {sheet_name} processed and saved to {output_path}")
    else:
        print(f"Skipping sheet: {sheet_name} (no 'Description' column)")

# Read all sheets into a dictionary
sheets_dict = pd.read_excel(file_path, sheet_name=None)

# Process each sheet
for sheet_name, df in sheets_dict.items():
    process_sheet(sheet_name, df)


# In[198]:


df_cleaned.head(50)


# In[184]:





# In[186]:


print(df_test.columns)


# In[187]:





# In[188]:


# def process_dataframe(filename, df_test):
#     # Extract the first character of the filename
#     first_char = filename[0]
    
#     # Check if the first character is 'K'
#     if first_char == 'K':  # Change this line to check for 'K' instead of 'S'
#         df_test['Country'] = 'KSA'
        
#         # Extract the first two digits from the filename
#         first_two_digits = filename[1:3]  # Adjust slicing if necessary depending on filename structure
        
#         # Prepend '20' to the extracted digits and create the 'Year' column
#         df_test['Year'] = '2018'  # Update this as needed
        
#         # Define the dollar rate for Saudi Arabia for the given year
#         # For example purposes, we assume a fictional rate; replace this with your actual rate source
#         exchange_rate = 0.27
        
#         # Calculate the USD rate by multiplying the Rate column by the exchange rate
#         # Ensure the 'Rate' column exists and is in numeric format
#         if 'Rate' in df_test.columns:
#             df_test['Rate'] = pd.to_numeric(df_test['Rate'], errors='coerce')
#             df_test['USD_Rate'] = df_test['Rate'].astype(float) * exchange_rate
        
#         # Add filename to the DataFrame
#         df_test['Filename'] = filename  # Changed from 'Project No.' to 'Filename' for clarity
    
#     return df_test

# # Example usage
# filename = 'KSA18091'
# df_test = process_dataframe(filename, df_test)


# In[189]:





# In[190]:





# In[191]:


df_cleaned.head(20)


# In[ ]:





# In[ ]:




