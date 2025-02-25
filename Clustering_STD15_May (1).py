#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans


# In[2]:


df = pd.read_excel('C:\\Users\\yymahmoudali\\Downloads\\Merged_Last_Sheets.xlsx')


# In[3]:


#STD15A Reference for Divisions and Sections 
sheet_name='Compiled LOIs'
df_STD = pd.read_excel('C:\\Users\\yymahmoudali\\Desktop\\eBoQ Database.xlsx',sheet_name=sheet_name)


# # Division Clustering and Normalization

# In[4]:


# 1. Cluster the divisions in L1_Desc and normalize(Done)
# 2. If a divsion does not start with the word Division, get the first two digits of the section and compare search if it exist after the word Division in the unique Division group, if so, replace it with the matched Division
# 3. Cluster the L2_Desc and normalize it 
# 4. Cluster the L3_Desc and give it a definition 


# In[5]:


# Count records where both L1_Desc and L2_Desc are null
count_nulls = df[(df['L1_Desc'].isnull()) & (df['L2_Desc'].isnull())].shape[0]
print("Count of records with both L1_Desc and L2_Desc as null:", count_nulls)
# Drop these records from the DataFrame
df = df.drop(df[(df['L1_Desc'].isnull()) & (df['L2_Desc'].isnull())].index)


# In[6]:


df['Division_ID'] = df['L1_Desc'].str.extract(r'DIVISION\s*(\d{1,2})')
# Format the extracted digits to ensure they are two digits long
df['Division_ID'] = df['Division_ID'].apply(lambda x: f"{int(x):02d}" if pd.notna(x) else x)
# Group by the formatted Division ID
grouped = df.groupby('Division_ID')
# for name, group in grouped:
#     unique_descriptions = group['L1_Desc'].unique()
#     print(f"Group: {name}")
#     for desc in unique_descriptions:
#         print(desc)
#     print("\n")


# In[7]:


division_mapping = {f"{int(code):02d}": name for code, name in zip(df_STD['Division Code'], df_STD['Division Name'])}
#print(division_mapping)
division_mapping['01'] = 'GENERAL REQUIRMENTS'
# Assuming 'Division_ID' is already created in df and formatted correctly
# Map 'Division Name' from the dictionary to 'Division Name' in df
df['Division Name'] = df['Division_ID'].map(division_mapping)


# In[8]:


# Create 'Dar L1' column combining 'DIVISION' with 'Division_ID' and 'Division Name'
df['Dar L1'] = 'DIVISION ' + df['Division_ID'] + ' ' + df['Division Name']


# In[9]:


df['Dar L1'] = df.apply(lambda row: row['L1_Desc'].replace('-', '') if pd.isna(row['Division Name']) and isinstance(row['L1_Desc'], str) else row['Dar L1'], axis=1)


# In[10]:


#df.to_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out.xlsx')


# # Section Normalization and Clustering

# In[11]:


def remove_spaces_in_first_five(text):
    # Check if text is a string; if not, convert it to string unless it's NaN
    if pd.isna(text):
        return text  # Return as is if NaN
    text = str(text)  # Convert to string to handle non-string types safely
    # Take the first five characters and remove spaces
    first_five = text[:6].replace(' ', '')
    # Concatenate the modified first five characters with the rest of the string
    rest = text[6:]
    return first_five + rest

# Apply the function to the L2_Desc column
df['L2_Desc'] = df['L2_Desc'].apply(remove_spaces_in_first_five)


# In[12]:


de f extract_digits(desc):
    if pd.isna(desc):
        return None  # Handle NaN values if any
    desc = str(desc)  # Ensure the desc is a string
    if desc.startswith('SECTION'):
        # Extract digits immediately after 'SECTION'
        match = re.search(r'SECTION(\d+)', desc)
        if match:
            return match.group(1)  # Return the digits following 'SECTION'
    else:
        # Extract the first sequence of digits from the string
        match = re.search(r'\d+', desc)
        if match:
            return match.group(0)  # Return the first found digits
    return None  # Return None if no digits are found

# Apply the function and create a new column
df['sec_key'] = df['L2_Desc'].apply(extract_digits)


# In[13]:


mask = df['L2_Desc'].str.startswith('SECTION') & df['sec_key'].isnull()

# Update 'sec_key' by extracting digits from 'L2_Desc', ignoring spaces between digits
df.loc[mask, 'sec_key'] = df['L2_Desc'].str.extract('SECTION\s+([\d\s]+)', expand=False).str.replace(' ', '')


# In[14]:


def process_values(value):
    try:
        # Attempt to remove leading zeros by converting to integer, then to string
        value = str(int(value))
        #Add '.0' if there is no decimal point
        if '.' not in value:
            value += '.0'
    except (ValueError, TypeError):
        # Handle cases where conversion to integer fails or value is None
        return None
    return value
# Apply the function to the sec_ey column
df['sec_key'] = df['sec_key'].apply(process_values)


# In[15]:


# # Step 1: Copy 'sec_key' to 'section_code'
# df['section_code'] = df['sec_key']

# # Step 2: Convert to integer if it ends with '.0', then to string, handle None values safely
# df['section_code'] = df['section_code'].apply(lambda x: str(int(float(x))) if x and '.0' in x else x)

# # Step 3: Pad the numbers with leading zeros to ensure they are at least 6 digits
# df['section_code'] = df['section_code'].apply(lambda x: x.zfill(6) if x else x)


# In[16]:


df['section_code'] = df['sec_key'].str.strip()  # Remove any leading/trailing spaces

# Create the mapping dictionary
section_mapping = {str(code).strip(): name for code, name in zip(df_STD['L2.Section Code'], df_STD['L2.Section Name'])}

# Apply the mapping
df['Section Name'] = df['sec_key'].map(section_mapping)


# In[17]:


df['Section Name'] = df['Section Name'].fillna(df['L2_Desc'])


# In[18]:


def process_code(code):
    code = str(code)  # Convert code to string to handle any type of input
    if '.0' in code:
        code = code.replace('.0', '')  # Remove .0 if present
    if len(code) == 5:
        code = '0' + code  # Add '0' at the beginning if it's exactly 5 digits long
    return code
df['sec_code'] = df['section_code'].apply(process_code)


# In[19]:


# Convert Section Name to string and handle None values gracefully
df['Section Name'] = df['Section Name'].apply(lambda x: str(x) if x is not None else "")

# Function to process section_code
def process_code(code):
    code = str(code)  # Convert code to string to handle any type of input
    if '.0' in code:
        code = code.replace('.0', '')  # Remove .0 if present
    if len(code) == 5:
        code = '0' + code  # Add '0' at the beginning if it's exactly 5 digits long
    return code

# Apply the function to create the new sec_code column
df['sec_code'] = df['section_code'].apply(process_code)

# Function to create Dar L2 based on conditions
def create_dar_l2(row):
    if row['Section Name'] and row['Section Name'][0].isdigit():
        return row['Section Name']
    else:
        return row['sec_code'] + ' ' + row['Section Name']

# Apply the function to create the new Dar L2 column
df['Dar L2'] = df.apply(create_dar_l2, axis=1)


# In[20]:


def enforce_custom_dash_format(value):
    # Regular expression to detect if formatting is already correct
    pattern_correct = r'^(\d+[\.\d]*)(\s*-\s*)(.*)$'
    if not re.match(pattern_correct, value):
        # Apply formatting: digits and optional decimals followed by a dash and space
        return re.sub(r'^(\d+[\.\d]*)(\s*)(.*)', r'\1 - \3', value)
    return value  # Return as is if already correct

# Apply the formatting function to the 'Dar L2' column
df['Dar L2'] = df['Dar L2'].apply(enforce_custom_dash_format)


# In[21]:


df['Dar L2'] = df['Dar L2'].str.upper()


# ## Item Clustering 

# In[22]:


import pandas as pd
import re

# Define the functions for text normalization and key part extraction
def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""  # Handle NaN values by returning an empty string
    return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
    match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
    return cleaned_text[:6]  # Use first 6 characters if no decimal number

# Applying the functions
df['Normalized_L2'] = df['L4_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# Group data by 'Dar_L1' and 'Key_Part'
grouped = df.groupby(['L1_Desc', 'Key_Part'])

def filter_unique_groups(group):
    """Identify and filter groups based only on non-special character differences."""
    texts = group['L4_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []  # No valid cluster if all items are identical
    non_special_char_parts = [re.sub(r'\W+', '', text).lower() for text in texts]  # Ignore special characters
    if len(set(non_special_char_parts)) == 1:
        return texts  # Valid cluster if items differ only by special characters
    return []

# Extract clusters and assign IDs
clusters = {name: filter_unique_groups(group) for name, group in grouped if filter_unique_groups(group)}
cluster_id_map = {}
representative_name_map = {}

# Assign unique IDs and prepare to populate the new column
for i, (cluster_key, texts) in enumerate(clusters.items()):
    if texts:
        cluster_id = f"Cluster_{i + 1}"
        representative_name = sorted(texts)[0]  # Choosing the first name alphabetically as representative
        representative_name_map[cluster_id] = representative_name
        for text in texts:
            cluster_id_map[text] = cluster_id

# Map the cluster IDs and representative names back to the DataFrame
df['L4_Clustering'] = df['L4_Desc'].map(cluster_id_map).fillna("No Cluster")
df['Dar_L4'] = df['L4_Clustering'].map(representative_name_map).fillna("No Cluster")


# In[23]:


clusters = {}
cluster_id_map = {}
cluster_id = 1 
for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        additional_texts = []
        base_text = filtered_texts[0]
        for text in df['L4_Desc'].tolist():
            if text not in filtered_texts and is_similar(base_text, text):
                additional_texts.append(text)
        filtered_texts.extend(additional_texts)  # Extend original cluster with similar items

        cluster_identifier = f"Cluster_{cluster_id}"
        for text in filtered_texts:
            cluster_id_map[text] = cluster_identifier
        cluster_id += 1

# Map the cluster IDs back to the DataFrame
df['L4_Cluster_ID'] = df['L4_Desc'].map(cluster_id_map).fillna("No Cluster")


# In[ ]:


# Counting records where 'L3_Clustering' is not 'No Cluster'
count_not_null_or_no_cluster = df[df['L4_Cluster_ID'] != 'No Cluster']['L4_Cluster_ID'].count()

print(f"Number of records not 'No Cluster': {count_not_null_or_no_cluster}")


# In[ ]:


# Filter DataFrame where neither 'L4_Cluster_ID' nor 'L3_Cluster_ID' is 'No Cluster' or null
valid_clusters = df[(df['L3_Cluster_ID'] != 'No Cluster') & (df['L3_Cluster_ID'].notna()) &
                    (df['L4_Cluster_ID'] != 'No Cluster') & (df['L4_Cluster_ID'].notna())]

# Count the number of such records
count_valid_clusters = valid_clusters.shape[0]
print(f"Number of records with valid L3 and L4 Cluster IDs: {count_valid_clusters}")


# In[ ]:


import pandas as pd
import re
from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed and imported

# Functions as previously defined
def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""
    return re.sub(r'[^\w\s.]', '', str(text))

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))
    match = re.search(r'(\d+\.\d+)', cleaned_text)
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
    return cleaned_text[:6]

def filter_unique_groups(group):
    texts = group['L4_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []
    non_special_char_parts = [re.sub(r'\W+', '', text).lower() for text in texts]
    if len(set(non_special_char_parts)) == 1:
        return texts
    return []

def is_similar(text1, text2, threshold=70):
    text1 = str(text1) if not pd.isna(text1) else ""
    text2 = str(text2) if not pd.isna(text2) else ""
    norm1 = re.sub(r'\W+', '', text1).lower()
    norm2 = re.sub(r'\W+', '', text2).lower()
    return fuzz.ratio(norm1, norm2) >= threshold

def refine_clusters(texts):
    refined_texts = texts.copy()
    to_remove = set()
    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            words_i = texts[i].split()
            words_j = texts[j].split()
            if len(words_i) == len(words_j) and sum(1 for a, b in zip(words_i, words_j) if a != b) == 1:
                to_remove.add(j)
    return [text for index, text in enumerate(texts) if index not in to_remove]


# Normalize and extract key parts
df['Normalized_L2'] = df['L4_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)
grouped = df.groupby(['L1_Desc', 'Key_Part'])

# Create and refine clusters with uniqueness check
clusters = {}
cluster_map = {}
cluster_id = 1

for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        filtered_texts = refine_clusters(filtered_texts)
        if len(set(filtered_texts)) > 1:  # Check if there are more than one unique item
            cluster_identifier = f"Cluster_{cluster_id}"
            clusters[cluster_identifier] = filtered_texts
            for text in filtered_texts:
                cluster_map[text] = cluster_identifier
            cluster_id += 1

# Map cluster IDs back to the DataFrame
df['L3_Clustering'] = df['L3_Desc'].map(cluster_map).fillna("No Cluster")



# ## L3 Clustering

# In[ ]:


# import numpy as np
# from collections import defaultdict

# # Functions for text normalization and key part extraction
# def normalize_text(text):
#     """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
#     if pd.isna(text):
#         return ""  # Handle NaN values by returning an empty string
#     return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

# def extract_key_part(text):
#     """Extract key parts for clustering, focusing on non-special character content."""
#     cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
#     match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
#     return cleaned_text[:6]  # Use first 6 characters if no decimal number

# def alphabetic_normalize(text):
#     """Normalize text to remove special characters and keep base text for grouping."""
#     text = str(text)  # Ensure input is treated as string
#     return re.sub(r'[^a-zA-Z]', '', text).lower()

# def normalize(text):
#     """Normalize text for numeric comparison by removing all non-numeric characters."""
#     return re.sub(r'[^0-9]', '', str(text))

# def filter_unique_groups(group):
#     """Cluster similar items based on textual similarity, excluding items with different numbers."""
#     texts = group['L3_Desc'].tolist()

#     # Create dictionary to group by normalized text
#     groups = defaultdict(list)
#     for text in texts:
#         key = alphabetic_normalize(text)
#         groups[key].append(text)

#     # Collect clusters where items are similar and numbers are the same
#     clusters = []
#     for key, group_texts in groups.items():
#         if len(group_texts) == 1:
#             continue  # Skip unique items

#         # Check number consistency within the group
#         number_pattern = ''.join(re.findall(r'\d+', normalize(group_texts[0])))
#         if all(''.join(re.findall(r'\d+', normalize(text))) == number_pattern for text in group_texts):
#             clusters.extend(group_texts)  # All texts in the group have the same numbers

#     return clusters

# # Applying functions to your DataFrame
# df['Normalized_L2'] = df['L3_Desc'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# # Group the DataFrame by the extracted key part
# grouped = df.groupby('Key_Part')
# clusters = {name: filter_unique_groups(group) for name, group in grouped if filter_unique_groups(group)}
# cluster_id_map = {}
# representative_name_map = {}

# # Assign unique IDs and prepare to populate the new column
# for i, (cluster_key, texts) in enumerate(clusters.items()):
#     if texts:
#         cluster_id = f"Cluster_{i + 1}"
#         representative_name = sorted(texts)[0]  # Choosing the first name alphabetically as representative
#         representative_name_map[cluster_id] = representative_name
#         for text in texts:
#             cluster_id_map[text] = cluster_id

# # Map the cluster IDs and representative names back to the DataFrame
# df['L3_Clustering'] = df['L3_Desc'].map(cluster_id_map).fillna("No Cluster")
# df['Dar_L3'] = np.where(df['L3_Clustering'] == "No Cluster", df['L3_Desc'], df['L3_Clustering'].map(representative_name_map))

# # Flatten all texts and count occurrences first
# all_texts = [text for texts in clusters.values() for text in texts]
# text_counts = {text: all_texts.count(text) for text in set(all_texts)}

# # Now print only texts that occur exactly once across all clusters
# print("Clusters with Truly Unique Items:")
# for cluster_key, texts in clusters.items():
#     print(f"Cluster {cluster_key}:")
#     printed_any = False
#     for text in set(texts):
#         if text_counts[text] == 1:  # Only print if this text appears exactly once in all clusters
#             print(f" - {text}")
#             printed_any = True
#     if not printed_any:
#         print("  No unique items")


# In[ ]:


pip install fuzzywuzzy


# In[28]:


from fuzzywuzzy import fuzz


# In[ ]:


# import os
# os.environ["TRANSFORMERS_OFFLINE"] = "1"
# os.environ['HF_DATASETS_OFFLINE']="1"
# os.environ['HF_METRICS_OFFLINE']="1"
# from transformers import AutoTokenizer, BertModel
# tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased", use_auth_token=False)
# model = BertModel.from_pretrained("bert-base-uncased", use_auth_token=False)


# In[ ]:


# from transformers import AutoTokenizer, BertModel
# import torch
 
# tokenizer = AutoTokenizer.from_pretrained("google-bert/bert-base-uncased")
# model = BertModel.from_pretrained("google-bert/bert-base-uncased")
 
# text1= "Blinding under Foundations, ground beams, slab on grade, machine bases, beds, pits, trenches, Tie Beams and the like"
# text2 = "Under Foundations, ground beams slab on grade"
 
# inputs = tokenizer(text1, return_tensors="pt")
# outputs1 = model(**inputs)
# last_hidden_states = outputs1.last_hidden_state
# print(last_hidden_states)
 
# inputs = tokenizer(text2, return_tensors="pt")
# outputs2 = model(**inputs)
 
# last_hidden_states = outputs2.last_hidden_state
# print(last_hidden_states)


# In[31]:


import pandas as pd
import re

def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""  # Handle NaN values by returning an empty string
    return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
    match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
    return cleaned_text[:6]  # Use first 6 characters if no decimal number

def filter_unique_groups(group):
    """Identify and filter groups based only on non-special character differences."""
    texts = group['L3_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []  # No valid cluster if all items are identical
    non_special_char_parts = [re.sub(r'\W+', '', str(text)).lower() for text in texts]  # Ignore special characters and ensure string input
    if len(set(non_special_char_parts)) == 1:
        return texts  # Valid cluster if items differ only by special characters
    return []
def is_similar(text1, text2, threshold=70):
    """
    Check if two texts are similar based on fuzzy matching.
    
    Args:
        text1 (str): The first text to compare.
        text2 (str): The second text to compare.
        threshold (int): The minimum similarity score (0-100) for texts to be considered similar.
    
    Returns:
        bool: True if texts are similar above the specified threshold, False otherwise.
    """
    text1 = str(text1) if not pd.isna(text1) else ""
    text2 = str(text2) if not pd.isna(text2) else ""
    
    # Normalize texts by removing non-alphanumeric characters and converting to lower case
    norm1 = re.sub(r'\W+', '', text1).lower()
    norm2 = re.sub(r'\W+', '', text2).lower()
    
    # Use fuzzy matching to determine similarity
    similarity_score = fuzz.ratio(norm1, norm2)
    
    return similarity_score >= threshold
def exclude_keywords(texts, keyword='type'):
    """Exclude texts containing a specific keyword."""
    return [text for text in texts if keyword not in text.lower()]
# Applying functions
df['Normalized_L2'] = df['L3_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# Group data by 'Dar_L1' and 'Key_Part'
grouped = df.groupby(['L1_Desc', 'Key_Part'])


# In[ ]:


# Displaying clusters with unique items
for cluster_key, texts in clusters.items():
    if texts:  # Ensuring non-empty clusters
        unique_texts = set(texts)  # Using a set to ensure uniqueness
        print(f"Cluster {cluster_key}:")
        for text in unique_texts:
            print(f" - {text}")


# In[32]:


clusters = {}
cluster_id_map = {}
cluster_id = 1 
for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        additional_texts = []
        base_text = filtered_texts[0]
        for text in df['L3_Desc'].tolist():
            if text not in filtered_texts and is_similar(base_text, text):
                additional_texts.append(text)
        filtered_texts.extend(additional_texts)  # Extend original cluster with similar items

        cluster_identifier = f"Cluster_{cluster_id}"
        for text in filtered_texts:
            cluster_id_map[text] = cluster_identifier
        cluster_id += 1

# Map the cluster IDs back to the DataFrame
df['L3_Cluster_ID'] = df['L3_Desc'].map(cluster_id_map).fillna("No Cluster")


# In[36]:


# Mock function to simulate filtering unique groups (assuming it removes duplicates)
def filter_unique_groups(group):
    return list(set(group))

# Mock function for similarity checking
def is_similar(base_text, compare_text, threshold=70):
    return fuzz.ratio(base_text, compare_text) > threshold

# Mocking a grouped structure, here just using the same list as a group for simplicity
grouped = [('Group1', df['L3_Desc'].tolist())]

clusters = {}
cluster_id_map = {}
cluster_id = 1

for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        additional_texts = []
        base_text = filtered_texts[0]
        for text in df['L3_Desc'].tolist():
            if text not in filtered_texts and is_similar(base_text, text):
                additional_texts.append(text)
        filtered_texts.extend(additional_texts)  # Extend original cluster with similar items

        cluster_identifier = f"Cluster_{cluster_id}"
        for text in filtered_texts:
            cluster_id_map[text] = cluster_identifier
        clusters[cluster_identifier] = filtered_texts  # Store all texts under this cluster ID
        cluster_id += 1

# Map the cluster IDs back to the DataFrame
df['L3_Cluster_ID'] = df['L3_Desc'].map(cluster_id_map).fillna("No Cluster")

# Print the clusters with unique items within
for cluster, texts in clusters.items():
    print(f"{cluster}: {set(texts)}")  # Using set to display unique items


# In[34]:


# Displaying clusters with unique items
for cluster_key, texts in clusters.items():
    if texts:  # Ensuring non-empty clusters
        unique_texts = set(texts)  # Using a set to ensure uniqueness
        print(f"Cluster {cluster_key}:")
        for text in unique_texts:
            print(f" - {text}")


# In[29]:


# import pandas as pd
# import re
# from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed and imported

# def normalize_text(text):
#     """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
#     if pd.isna(text):
#         return ""  # Handle NaN values by returning an empty string
#     return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

# def extract_key_part(text):
#     """Extract key parts for clustering, focusing on non-special character content."""
#     cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
#     match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
#     return cleaned_text[:6]  # Use first 6 characters if no decimal number

# def filter_unique_groups(group):
#     """Identify and filter groups based only on non-special character differences."""
#     texts = group['L3_Desc'].tolist()
#     unique_texts = set(texts)
#     if len(unique_texts) == 1:
#         return []  # No valid cluster if all items are identical
#     non_special_char_parts = [re.sub(r'\W+', '', str(text)).lower() for text in texts]
#     if len(set(non_special_char_parts)) == 1:
#         return texts  # Valid cluster if items differ only by special characters
#     return []

# def is_similar(text1, text2, threshold=70):
#     text1 = str(text1) if not pd.isna(text1) else ""
#     text2 = str(text2) if not pd.isna(text2) else ""
#     norm1 = re.sub(r'\W+', '', text1).lower()
#     norm2 = re.sub(r'\W+', '', text2).lower()
#     similarity_score = fuzz.ratio(norm1, norm2)
#     return similarity_score >= threshold

# def refine_clusters(texts):
#     """Refine clusters by removing texts that are the same except for one word."""
#     refined_texts = texts.copy()
#     to_remove = set()
#     for i in range(len(texts)):
#         for j in range(i + 1, len(texts)):
#             words_i = texts[i].split()
#             words_j = texts[j].split()
#             if len(words_i) == len(words_j) and sum(1 for a, b in zip(words_i, words_j) if a != b) == 1:
#                 to_remove.add(j)  # Exclude the second item
#     refined_texts = [text for index, text in enumerate(texts) if index not in to_remove]
#     return refined_texts

# def exclude_keywords(texts, keyword='type'):
#     """Exclude texts containing a specific keyword."""
#     return [text for text in texts if keyword not in text.lower()]

# # Assuming df is the DataFrame you're working with
# # Initialize and group data
# df['Normalized_L2'] = df['L3_Desc'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)
# grouped = df.groupby(['L1_Desc', 'Key_Part'])

# # # Create and refine clusters
# # clusters = {}
# # for name, group in grouped:
# #     filtered_texts = filter_unique_groups(group)
# #     if filtered_texts:
# #         filtered_texts = exclude_keywords(filtered_texts)  # Exclude by keyword
# #         filtered_texts = refine_clusters(filtered_texts)  # Refine by similarity condition
# #         if filtered_texts:
# #             clusters[name] = filtered_texts

# # Create and refine clusters
# clusters = {}
# cluster_map = {}
# cluster_id = 1  # Start cluster IDs at 1

# for name, group in grouped:
#     filtered_texts = filter_unique_groups(group)
#     if filtered_texts:
#         filtered_texts = refine_clusters(filtered_texts)
#         if filtered_texts:
#             cluster_identifier = f"Cluster_{cluster_id}"
#             clusters[cluster_identifier] = filtered_texts
#             for text in filtered_texts:
#                 cluster_map[text] = cluster_identifier
#             cluster_id += 1

# # Map cluster IDs back to the DataFrame
# df['L3_Clustering'] = df['L3_Desc'].map(cluster_map).fillna("No Cluster")


# In[26]:


import pandas as pd
import re
from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed and imported

# Functions as previously defined
def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""
    return re.sub(r'[^\w\s.]', '', str(text))

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))
    match = re.search(r'(\d+\.\d+)', cleaned_text)
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
    return cleaned_text[:6]

def filter_unique_groups(group):
    texts = group['L3_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []
    non_special_char_parts = [re.sub(r'\W+', '', text).lower() for text in texts]
    if len(set(non_special_char_parts)) == 1:
        return texts
    return []

def is_similar(text1, text2, threshold=70):
    text1 = str(text1) if not pd.isna(text1) else ""
    text2 = str(text2) if not pd.isna(text2) else ""
    norm1 = re.sub(r'\W+', '', text1).lower()
    norm2 = re.sub(r'\W+', '', text2).lower()
    return fuzz.ratio(norm1, norm2) >= threshold

def refine_clusters(texts):
    refined_texts = texts.copy()
    to_remove = set()
    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            words_i = texts[i].split()
            words_j = texts[j].split()
            if len(words_i) == len(words_j) and sum(1 for a, b in zip(words_i, words_j) if a != b) == 1:
                to_remove.add(j)
    return [text for index, text in enumerate(texts) if index not in to_remove]


# Normalize and extract key parts
df['Normalized_L2'] = df['L3_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)
grouped = df.groupby(['L1_Desc', 'Key_Part'])

# Create and refine clusters with uniqueness check
clusters = {}
cluster_map = {}
cluster_id = 1

for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        filtered_texts = refine_clusters(filtered_texts)
        if len(set(filtered_texts)) > 1:  # Check if there are more than one unique item
            cluster_identifier = f"Cluster_{cluster_id}"
            clusters[cluster_identifier] = filtered_texts
            for text in filtered_texts:
                cluster_map[text] = cluster_identifier
            cluster_id += 1

# Map cluster IDs back to the DataFrame
df['L3_Clustering'] = df['L3_Desc'].map(cluster_map).fillna("No Cluster")



# In[35]:


# Displaying clusters with unique items
for cluster_key, texts in clusters.items():
    if texts:  # Ensuring non-empty clusters
        unique_texts = set(texts)  # Using a set to ensure uniqueness
        print(f"Cluster {cluster_key}:")
        for text in unique_texts:
            print(f" - {text}")


# In[62]:


# import pandas as pd
# import re
# from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed and imported

# def normalize_text(text):
#     """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
#     if pd.isna(text):
#         return ""  # Handle NaN values by returning an empty string
#     return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

# def extract_key_part(text):
#     """Extract key parts for clustering, focusing on non-special character content."""
#     cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
#     match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
#     return cleaned_text[:6]  # Use first 6 characters if no decimal number

# def filter_unique_groups(group):
#     """Identify and filter groups based only on non-special character differences."""
#     texts = group['L3_Desc'].tolist()
#     unique_texts = set(texts)
#     if len(unique_texts) == 1:
#         return []  # No valid cluster if all items are identical
#     non_special_char_parts = [re.sub(r'\W+', '', str(text)).lower() for text in texts]  # Ignore special characters and ensure string input
#     if len(set(non_special_char_parts)) == 1:
#         return texts  # Valid cluster if items differ only by special characters
#     return []

# def is_similar(text1, text2, threshold=60):
#     text1 = str(text1) if not pd.isna(text1) else ""
#     text2 = str(text2) if not pd.isna(text2) else ""
#     norm1 = re.sub(r'\W+', '', text1).lower()
#     norm2 = re.sub(r'\W+', '', text2).lower()
#     similarity_score = fuzz.ratio(norm1, norm2)
#     return similarity_score >= threshold

# def exclude_keywords(texts, keyword='type'):
#     """Exclude texts containing a specific keyword."""
#     return [text for text in texts if keyword not in text.lower()]


# df['Normalized_L2'] = df['L3_Desc'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# # Group data by 'Dar_L1' and 'Key_Part'
# grouped = df.groupby(['L1_Desc', 'Key_Part'])

# # Create clusters
# clusters = {}
# for name, group in grouped:
#     filtered_texts = filter_unique_groups(group)
#     if filtered_texts:
#         filtered_texts = exclude_keywords(filtered_texts)  # Apply keyword exclusion
#         if filtered_texts:  # Ensure there's still text left after exclusion
#             clusters[name] = filtered_texts

# # Process clusters to add similar items
# for cluster_key, texts in list(clusters.items()):
#     base_text = texts[0] if texts else None  # Safety check in case list is empty after keyword exclusion
#     additional_texts = []
#     for text in df['L3_Desc'].tolist():
#         if text not in texts and is_similar(base_text, text):
#             additional_texts.append(text)
#     clusters[cluster_key].extend(additional_texts)



# In[ ]:


# grouped = df.groupby(['L1_Desc', 'Key_Part'])
# clusters = {}

# for name, group in grouped:
#     filtered_texts = filter_unique_groups(group)
#     if filtered_texts:
#         refined_texts = refine_clusters(filtered_texts)
#         if len(set(refined_texts)) > 1:
#             clusters[name] = refined_texts

# # Check if any clusters are found
# if clusters:
#     for cluster_key, texts in clusters.items():
#         if texts:  # Ensuring non-empty clusters
#             unique_texts = set(texts)  # Using a set to ensure uniqueness
#             print(f"Cluster {cluster_key}:")
#             for text in unique_texts:
#                 print(f" - {text}")
# else:
#     print("No clusters with more than one unique item were found.")

    
    
# # Displaying clusters with unique items



# In[36]:


# import pandas as pd
# import re
# from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed

# def safe_text(text):
#     """Ensure that the input is a string and handle NaN values."""
#     if pd.isna(text):
#         return ""  # Return an empty string for NaN values
#     return str(text)  # Convert any float or other non-string data types to string

# def split_sections(text):
#     """Split text into sections based on '/*/', ensuring text is safe to process."""
#     text = safe_text(text)  # Ensure text is a string and handle NaN
#     return text.split('/*/')

# def compare_sections(cluster):
#     """Compare sections of each item in the cluster for similarity, ensuring safe text handling."""
#     # Initialize an empty list to hold items to be removed
#     items_to_remove = []
    
#     # Get lists of sections for each text in the cluster, ensuring all texts are safe to process
#     list_of_sections = [split_sections(text) for text in cluster]
    
#     # Check if all items have the same number of sections
#     section_lengths = [len(sections) for sections in list_of_sections]
#     if len(set(section_lengths)) > 1:
#         return cluster  # If items differ in number of sections, do nothing
    
#     # If all items have the same number of sections, compare corresponding sections
#     num_sections = section_lengths[0]
#     for section_index in range(num_sections):
#         base_section = list_of_sections[0][section_index]
#         for sections in list_of_sections[1:]:
#             if fuzz.ratio(base_section, sections[section_index]) < 80:
#                 items_to_remove.append('/*/'.join(sections))
    
#     # Return the updated cluster with items removed if necessary
#     return [item for item in cluster if item not in items_to_remove]

# # Assume `clusters` is your dictionary from the previous code
# # Apply section comparison and filtering
# for key, cluster_items in clusters.items():
#     clusters[key] = compare_sections(cluster_items)

# # Output the updated clusters
# print(clusters)


# In[ ]:





# In[ ]:


# # Assume `clusters` is your dictionary from the previous code
# # Apply section comparison and filtering
# for key, cluster_items in clusters.items():
#     clusters[key] = compare_sections(cluster_items)

# # Remove duplicates and filter clusters to only include those with more than one item
# unique_clusters = {key: list(set(items)) for key, items in clusters.items() if len(set(items)) > 1}

# # Output the unique clusters with more than one item
# for key, unique_items in unique_clusters.items():
#     print(f"Cluster {key}:")
#     for item in unique_items:
#         print(f"  - {item}")


# In[ ]:


# import pandas as pd
# import re
# from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed

# def safe_text(text):
#     """Ensure that the input is a string and handle NaN values."""
#     if pd.isna(text):
#         return ""  # Return an empty string for NaN values
#     return str(text)  # Convert any float or other non-string data types to string

# def split_sections(text):
#     """Split text into sections based on '/*/', ensuring text is safe to process."""
#     text = safe_text(text)  # Ensure text is a string and handle NaN
#     return text.split('/*/')

# def extract_numbers(inputString):
#     """Extract all numeric sequences from a string."""
#     return re.findall(r'\d+', inputString)

# def compare_sections(cluster):
#     """Compare sections of each item in the cluster to identify differences in numbers at the same positions."""
#     # Initialize an empty list to hold items to be removed
#     items_to_remove = []

#     # Get lists of sections for each text in the cluster, ensuring all texts are safe to process
#     list_of_sections = [split_sections(text) for text in cluster]

#     # Check if all items have the same number of sections
#     section_lengths = [len(sections) for sections in list_of_sections]
#     if len(set(section_lengths)) == 1:
#         num_sections = section_lengths[0]
#         # Collect all numbers from each section for comparison
#         numbers_in_sections = [[extract_numbers(section) for section in sections] for sections in list_of_sections]

#         # Compare numbers across all items for each section
#         for section_index in range(num_sections):
#             # Extract a list of number lists for the current section across all items
#             current_section_numbers = [numbers[section_index] for numbers in numbers_in_sections]

#             # Check if any numbers are different at the same position
#             if not all(numbers == current_section_numbers[0] for numbers in current_section_numbers):
#                 for sections in list_of_sections:
#                     items_to_remove.append('/*/'.join(sections))
#                 break

#     # Return the updated cluster with items removed if necessary
#     return [item for item in cluster if '/*/'.join(split_sections(item)) not in items_to_remove]

# # Assume `clusters` is your dictionary from the previous code
# # Apply section comparison and filtering
# for key, cluster_items in clusters.items():
#     clusters[key] = compare_sections(cluster_items)

# # Remove duplicates and filter clusters to only include those with more than one item
# unique_clusters = {key: list(set(items)) for key, items in clusters.items() if len(set(items)) > 1}

# # Output the unique clusters with more than one item
# for key, unique_items in unique_clusters.items():
#     print(f"Cluster {key}:")
#     for item in unique_items:
#         print(f"  - {item}")


# In[45]:


# import pandas as pd
# import re

# def safe_text(text):
#     """Ensure the input is a string and handle NaN values by returning a default string."""
#     return "" if pd.isna(text) else str(text)

# def extract_detailed_key_parts(text):
#     """Extract and prioritize key parts for clustering based on detailed sub-categories."""
#     text = safe_text(text)  # Convert text to string safely
#     sections = text.split('/*/')
#     if len(sections) > 1:
#         # Focus on sub-categories like 'Wet-Pipe' or 'Dry-Pipe'
#         category_section = sections[1].strip()
#         main_category, sub_category = sections[0].strip(), None
#         if 'Wet-Pipe' in category_section or 'Dry-Pipe' in category_section:
#             sub_category = 'Wet-Pipe' if 'Wet-Pipe' in category_section else 'Dry-Pipe'
#         return f"{main_category}/*/{sub_category}"
#     return sections[0].strip()  # Fallback to main category if no sub-categories are detailed

# # Example usage with a DataFrame
# # Assuming df is your DataFrame and it contains a column 'Description'
# df['Refined_Key_Part'] = df['L3_Desc'].apply(extract_detailed_key_parts)
# grouped = df.groupby(['Refined_Key_Part'])

# # Example of how you would form clusters
# clusters = {}
# for name, group in grouped:
#     # Assuming filter_unique_groups is a function you have defined to filter groups
#     filtered_texts = filter_unique_groups(group)  # Make sure this function handles the group data appropriately
#     if filtered_texts:
#         clusters[name] = filtered_texts


# In[ ]:


# # Filter clusters to only include those with more than one unique item
# filtered_clusters = {key: list(set(items)) for key, items in clusters.items() if len(set(items)) > 1}

# # Output the filtered clusters
# for key, unique_items in filtered_clusters.items():
#     print(f"Cluster {key}:")
#     for item in unique_items:
#         print(f"  - {item}")


# In[ ]:


df.loc[df['Dar_L4'] == 'No Cluster', 'Dar_L4'] = df['L4_Desc']


# In[41]:


df.to_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out_found_perc.xlsx')


# ## BOQ Test

# In[44]:


# Counting records where 'L3_Clustering' is not 'No Cluster'
count_not_null_or_no_cluster = df[df['L3_Clustering'] != 'No Cluster']['L3_Clustering'].count()

print(f"Number of records not 'No Cluster': {count_not_null_or_no_cluster}")


# In[45]:


# Counting records where 'L3_Clustering' is not 'No Cluster'
count_not_null_or_no_cluster = df[df['L4_Clustering'] != 'No Cluster']['L4_Clustering'].count()

print(f"Number of records not 'No Cluster': {count_not_null_or_no_cluster}")


# In[47]:


# Function to update status based on conditions
def update_status(row):
    if row['Project No.'] == 'J15123' and row['L3_Clustering'] != 'No Cluster' and row['L4_Clustering'] != 'No Cluster':
        return 'Found'
    return 'Not Found'

# Apply the function to each row in the DataFrame
df['Status'] = df.apply(update_status, axis=1)


# In[48]:


# Assuming df is your DataFrame and it already has a 'Status' column updated with 'Found' or 'Not Found'
status_counts = df['Status'].value_counts()
found_count = status_counts.get('Found', 0)  # Default to 0 if 'Found' is not in the index

print(f"Number of records where status is 'Found': {found_count}")


# ## New BOQ Test to see if its items are found in DB

# In[45]:


# # Filter to get the records for Project No. J21182
# filtered_data = df[df['Project No.'] == 'J15123'][['Project No.', 'Dar L1', 'Dar L2', 'L3_Desc', 'Dar_L4']]

# # Save the filtered data to a new Excel file
# #filtered_data.to_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out_filtered_data.xlsx', index=False, engine='openpyxl')

# # Remove these records from the original DataFrame
# df = df[df['Project No.'] != 'J15123']


# In[51]:


# filtered_data['Status'] = 'Not Found'
# from fuzzywuzzy import fuzz  # Ensure fuzzywuzzy is installed and imported

# # Example setup for demonstration. Ensure df and filtered_data are properly defined and loaded.
# # df might contain columns 'Dar_L4' and 'L3_Desc'
# # filtered_data should be the DataFrame you want to apply the checks to

# # Create a set of tuples from df for faster look-up, ensuring all are strings
# # This will store both Dar_L4 and Dar_L3 values
# dar_l4_l3_set = set(df[['Dar_L4', 'L3_Desc']].dropna().apply(lambda row: (str(row['Dar_L4']), str(row['L3_Desc'])), axis=1))

# # Function to check similarity for Dar_L4 and then Dar_L3, and annotate with similar Dar_L4
# def check_similarity(row):
#     dar_l4, dar_l3 = str(row['Dar_L4']), str(row['L3_Desc'])  # Ensure both columns are treated as strings
#     for item in dar_l4_l3_set:
#         # Use fuzz.ratio to check if similarity is above 70%
#         if fuzz.ratio(dar_l4, item[0]) > 70 and fuzz.ratio(dar_l3, item[1]) > 70:
#             return f'Found - Similar Dar_L4: {item[0]}, Dar_L3: {item[1]}'
#     return 'Not Found'

# # Apply the function to each row in filtered_data
# filtered_data['Status'] = filtered_data.apply(check_similarity, axis=1)


# In[65]:


import pandas as pd
import re
from fuzzywuzzy import fuzz

def check_similarity(df, project_no):
    # Filter the row for the specific project
    project_data = df[df['Project No.'] == project_no]
    if project_data.empty:
        return 'Project not found.'

    # Get L4_Desc and L3_Desc for the specific project
    specific_l4 = project_data.iloc[0]['L4_Desc']
    specific_l3 = project_data.iloc[0]['L3_Desc']
    specific_l4_numbers = ''.join(re.findall(r'\d+', specific_l4))  # Extract numbers
    specific_l3_numbers = ''.join(re.findall(r'\d+', specific_l3))

    # Iterate over other projects
    for _, row in df[df['Project_No'] != project_no].iterrows():
        l4_desc = row['L4_Desc']
        l3_desc = row['L3_Desc']
        l4_numbers = ''.join(re.findall(r'\d+', l4_desc))
        l3_numbers = ''.join(re.findall(r'\d+', l3_desc))

        # Check if both descriptions contain the same numbers
        if l4_numbers == specific_l4_numbers and l3_numbers == specific_l3_numbers:
            # Check if the descriptions are similar
            if fuzz.ratio(specific_l4, l4_desc) > 70 and fuzz.ratio(specific_l3, l3_desc) > 70:
                return f'Found - Similar L4_Desc: {l4_desc}, L3_Desc: {l3_desc}'

    return 'Not Found'


result = check_similarity(df, 'J15123')



# In[45]:


sheet_name='Sheet1'
df = pd.read_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out (version 1).xlsx',sheet_name=sheet_name)


# In[46]:


df.head()


# In[49]:


import pandas as pd
import re
from fuzzywuzzy import fuzz

def check_similarity_and_update_avg(df, project_no):
    # Ensure the description columns are treated as strings
    df['L4_Desc'] = df['L4_Desc'].astype(str)
    df['L3_Desc'] = df['L3_Desc'].astype(str)

    # Filter the row for the specific project
    project_data = df[df['Project No.'] == project_no]
    if project_data.empty:
        return 'Project not found.', df

    # Get L4_Desc and L3_Desc for the specific project
    specific_l4 = project_data.iloc[0]['L4_Desc']
    specific_l3 = project_data.iloc[0]['L3_Desc']
    specific_l4_numbers = ''.join(re.findall(r'\d+', specific_l4))  # Extract numbers
    specific_l3_numbers = ''.join(re.findall(r'\d+', specific_l3))

    similar_indices = []

    # Iterate over other projects
    for index, row in df.iterrows():
        l4_desc = row['L4_Desc']
        l3_desc = row['L3_Desc']
        l4_numbers = ''.join(re.findall(r'\d+', l4_desc))
        l3_numbers = ''.join(re.findall(r'\d+', l3_desc))

        # Check if both descriptions contain the same numbers
        if l4_numbers == specific_l4_numbers and l3_numbers == specific_l3_numbers:
            # Check if the descriptions are similar
            if fuzz.ratio(specific_l4, l4_desc) > 70 and fuzz.ratio(specific_l3, l3_desc) > 70:
                similar_indices.append(index)

    # Check if any similar items were found
    if similar_indices:
        # Calculate the average USD_Rate for these similar items
        avg_usd_rate = df.loc[similar_indices, 'USD_Rate'].mean()
        # Add the average to a new column for these similar rows
        df.loc[similar_indices, 'new_avg'] = avg_usd_rate
        return f'Found - Similar items updated with average USD rate: {avg_usd_rate}', df

    return 'No similar items found.', df  # Ensure a tuple is returned even if no similar items are found


result, updated_df = check_similarity_and_update_avg(df, 'J15123')


# In[55]:


# Load data from both files
df1 = pd.read_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out (version 1).xlsx')
df2 = pd.read_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out_filtered_data___.xlsx')


# In[57]:


df2.head()


# In[59]:


def merge_status(file1_path, file2_path):


    # Ensure the key columns are of the same data type
    df1['Project No.'] = df1['Project No.'].astype(str)
    df2['Project No.'] = df2['Project No.'].astype(str)
    df1['L3_Desc'] = df1['L3_Desc'].astype(str)
    df2['L3_Desc'] = df2['L3_Desc'].astype(str)
    df1['Dar_L4'] = df1['Dar_L4'].astype(str)
    df2['Dar_L4'] = df2['Dar_L4'].astype(str)

    # Merge df2 into df1 based on the specified columns to match
    merged_df = pd.merge(df1, df2[['Project No.', 'L3_Desc', 'Dar_L4', 'Status']], 
                         on=['Project No.', 'L3_Desc', 'Dar_L4'], how='left')

    # Optionally, you can fill NaN values in the Status column if no match found
    merged_df['Status'] = merged_df['Status'].fillna('No Status Available')

    # Save the merged data back to a new Excel file or overwrite the old one
    merged_df.to_excel(file1_path.replace('.xlsx', '_updated.xlsx'), index=False)
    print('Updated file saved.')

# Paths to the Excel files
file1_path = 'C:\\Users\\yymahmoudali\\Desktop\\merged_out (version 1).xlsx'
file2_path = 'C:\\Users\\yymahmoudali\\Desktop\\merged_out_filtered_data___.xlsx'

# Execute the function
merge_status(file1_path, file2_path)


# In[ ]:





# In[52]:


updated_df.to_excel('C:\\Users\\yymahmoudali\\Desktop\\merged_out_filtered_data___._.xlsx', index=False, engine='openpyxl')


# In[49]:


import pandas as pd


# In[58]:


import pandas as pd

# Assuming df is already loaded with the relevant data

# Group by 'L4_Clustering' and aggregate the unique 'Project No.' for each group
grouped = df.groupby('L4_Clustering')['Project No.'].nunique()

# Filter groups where more than one unique 'Project No.' exists
common_l4_clusters = grouped[grouped > 1].index

# Print the L4_Clustering values that are common across different projects
print("L4_Clustering values common across different Project Nos.:", common_l4_clusters.tolist())


# In[ ]:


# # Function to extract digits
# def extract_digits(desc):
#     if pd.isna(desc):
#         return None  # Handle NaN values if any
#     desc = str(desc)  # Ensure the desc is a string
#     if desc.startswith('SECTION'):
#         # Extract digits after 'SECTION'
#         match = re.search(r'SECTION(\d+)', desc)
#         if match:
#             return match.group(1)  # Return the digits following 'SECTION'
#     else:
#         # Extract the first sequence of digits from the string
#         match = re.search(r'\d+', desc)
#         if match:
#             return match.group(0)  # Return the first found digits
#     return None  # Return None if no digits are found

# # Apply the function and create a new column
# df['sec_ey'] = df['L2_Desc'].apply(extract_digits)


# In[4]:


# def extract_digits(desc):
#     # Convert desc to string in case it's NaN or None
#     desc = str(desc)
    
#     # Try to extract digits at the beginning of the string
#     if desc.split()[0].isdigit():
#         return desc.split()[0]
#     else:
#         # Find the digits after the word "SECTION"
#         import re
#         match = re.search(r'SECTION (\d+)', desc)
#         if match:
#             return match.group(1)
#         else:
#             return None  # or some default value or error handling

# # Apply the function to create the new column
# df['sec_key'] = df['L2_Desc'].apply(extract_digits)


# In[5]:


# section_dict = pd.Series(df_STD['L2.Section Name'].values, index=df_STD['L2.Section Code']).to_dict()
# #print(section_dict)


# In[8]:


# df['Section Name'] = df['sec_key'].map(section_dict)


# In[11]:


# def extract_digits(text):
#     if pd.isna(text):
#         return None  # Skip NaN values
#     text = str(text)  # Ensure the text is a string
#     if text.startswith('SECTION'):
#         # Find first sequence of digits in the text
#         import re
#         match = re.search(r'\d+', text[len('SECTION'):])
#         if match:
#             return match.group(0)
#     return None

# df['sec_key'] = df['L2_Desc'].apply(extract_digits)

# # Step 2: Mapping codes to names
# code_to_name = dict(zip(df_STD['L2.Section Code'], df_STD['L2.Section Name']))

# # Step 3: Map and assign the corresponding section names
# df['Section Name'] = df['sec_key'].map(code_to_name)


# In[60]:


# def adjust_sec_code(code):
#     # Check if code is None
#     if code is None:
#         return None
#     # If code ends with '.0', remove it
#     if code.endswith('.0'):
#         code = code[:-2]
#     # Ensure the code is at least 6 characters long by padding with zeros
#     code = code.zfill(6)
#     return code

# df['sec_code'] = df['sec_key'].apply(adjust_sec_code)

# # Step 2: Creating Dar L2
# def create_dar_l2(row):
#     # If the sec_code or Section Name is None, handle it appropriately
#     if row['sec_code'] is None or row['Section Name'] is None:
#         return None
#     # Check if Section Name starts with a digit
#     if row['Section Name'][0].isdigit():
#         return row['Section Name']
#     else:
#         return f"{row['sec_code']} - {row['Section Name']}"

# df['Dar L2'] = df.apply(create_dar_l2, axis=1)


# In[38]:


# def normalize_text(text):
#     if pd.isna(text):
#         return ""
#     text = str(text)
#     return re.sub(r'[^A-Za-z0-9\s\.]', '', text)

# def extract_key_part(text):
#     if text.startswith("SECTION"):
#         return re.sub(r'SECTION\s+', '', text).split()[0][:12]
#     if "SECTION 28 23 00 Video Surveillance System" in text or "SECTION 28 23 00 PART 1 SECURITY SYSTEMS" in text:
#         return "SpecialSecurityCluster"
#     cleaned_text = re.sub(r'[^A-Za-z0-9\.]', '', text)
#     match = re.search(r'(\d+\.\d+)', cleaned_text)
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
#     return cleaned_text[:6]

# df['Normalized_L2'] = df['L2_Desc'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)
# df['Anomaly'] = False
# grouped = df.groupby(['L1_Desc', 'Key_Part'])

# def filter_unique_groups(group):
#     texts = group['Normalized_L2'].tolist()
#     unique_texts = set(texts)
#     if len(unique_texts) > 1:
#         return list(unique_texts)
#     return []

# clusters_dict = {}
# cluster_id = 0

# def assign_cluster_id(row):
#     for (division, key_part), (id, texts) in clusters_dict.items():
#         if (row['L1_Desc'], row['Key_Part']) == (division, key_part) and row['Normalized_L2'] in texts:
#             # Check existence in df_STD and retrieve name
#             if id in df_STD['L2.Section Code'].values:
#                 section_name = df_STD.loc[df_STD['L2.Section Code'] == id, 'L2.Section Name'].values[0]
#                 return id, section_name
#             else:
#                 return id, "Doesn't Exist"
#     return None, None

# for name, group in grouped:
#     unique_texts = filter_unique_groups(group)
#     if unique_texts:
#         clusters_dict[name] = (cluster_id, unique_texts)
#         cluster_id += 1

# df['Cluster_ID'], df['Cluster_Name'] = zip(*df.apply(assign_cluster_id, axis=1))

# # Printing the unique items of each cluster that has more than one item
# for (cluster_key, (id, unique_texts)) in clusters_dict.items():
#     print(f"Cluster {cluster_key} ID {id}:")
#     section_name = df_STD.loc[df_STD['L2.Section Code'] == id, 'L2.Section Name'].values[0] if id in df_STD['L2.Section Code'].values else "Doesn't Exist"
#     print(f"Cluster Name: {section_name}")
#     for text in unique_texts:
#         print(f" - {text}")


# In[ ]:


# # Define a function to process each row
# def process_data(row):
#     if 'BILL NO. 3 / DIVISION 7 - THERMAL & MOISTURE PROTECTION' in str(row['L1_Desc']):
#         # Get first two digits from L2_Desc
#         first_two_digits = ''.join(filter(str.isdigit, str(row['L2_Desc'])))[:2]
        
#         # Search for these digits in L1_Desc and replace if found
#         for idx, target_row in df.iterrows():
#             if first_two_digits in str(target_row['L1_Desc']):
#                 df.at[idx, 'L1_Desc'] = 'BILL NO. 3 / DIVISION 7 - THERMAL & MOISTURE PROTECTION'

# # Apply the function to each row
# df.apply(process_data, axis=1)


# In[39]:


# from itertools import combinations
# import numpy as np

# def mark_anomalies(group):
#     texts = list(group['Normalized_L2'])
#     # Extract suffixes after the first 6 characters
#     suffixes = [text[6:] for text in texts]
#     indices = list(range(len(suffixes)))

#     # Use itertools.combinations to generate all pairs of indices
#     for i, j in combinations(indices, 2):
#         lev_distance = levenshtein_distance(suffixes[i], suffixes[j])
#         max_len = max(len(suffixes[i]), len(suffixes[j]))
#         if max_len > 0:  # Ensure max_len is not zero to avoid division by zero
#             similarity_ratio = 1 - lev_distance / max_len
#             # Mark as anomaly if similarity ratio is below a threshold (e.g., 0.2)
#             if similarity_ratio < 0.2:
#                 group['Anomaly'].iloc[i] = True
#                 group['Anomaly'].iloc[j] = True

#     return group

# # Reset the index of the DataFrame if necessary
# df.reset_index(inplace=True)

# # Check if the DataFrame already contains the index columns as regular columns to avoid duplicate columns
# index_columns = ['level_0', 'level_1']  # List these based on your DataFrame's structure
# need_to_reset_index = any(col in df.columns for col in index_columns)

# if need_to_reset_index:
#     # Reset the index but drop the index columns to avoid duplicates
#     df.reset_index(drop=True, inplace=True)
# else:
#     # Safe to reset the index without dropping if it hasn't been done before
#     df.reset_index(inplace=True)

# # Ensure the Anomaly column is initialized before applying functions
# df['Anomaly'] = False

# # Apply the function to mark anomalies
# df = df.groupby(['L1_Desc', 'Key_Part']).apply(mark_anomalies)

# # Prepare a dictionary mapping each Key_Part to its corresponding cluster name
# key_part_to_cluster_name = {kp: cluster_names[id] for (division, kp), (id, texts) in clusters_dict.items()}

# # Initialize the 'Dar_L2' column by copying 'L2_Desc'
# df['Dar_L2'] = df['L2_Desc']

# # Apply the precomputed cluster names to Dar_L2 using a vectorized map approach
# df['Dar_L2'] = df['Key_Part'].map(key_part_to_cluster_name).fillna(df['Dar_L2'])

# # Optionally, print or inspect some rows to confirm changes


# In[ ]:


# # Normalize and extract key parts
# def normalize_text(text):
#     """Remove special characters except decimals and normalize spaces."""
#     if pd.isna(text):
#         return ""  # Return empty string if text is NaN
#     text = str(text)  # Ensure text is a string
#     return re.sub(r'[^A-Za-z0-9\s\.]', '', text)

# def extract_key_part(text):
#     """Extract first 6 characters and first 2 after the decimal if present."""
#     text = str(text)  # Ensure text is a string
#     cleaned_text = re.sub(r'[^A-Za-z0-9\.]', '', text)
#     match = re.search(r'(\d+\.\d+)', cleaned_text)
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
#     else:
#         return cleaned_text[:6]

# df['Normalized_L2'] = df['L2_Desc'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# # Grouping and filtering
# grouped = df.groupby(['L1_Desc', 'Key_Part'])

# def filter_unique_groups(group):
#     """Filter groups to return only those with non-identical but similar items."""
#     texts = group['Normalized_L2'].tolist()
#     unique_texts = set(texts)
#     if len(unique_texts) == 1:
#         return []  # All items are identical, no valid cluster
#     non_digit_parts = [re.sub(r'\d', '', text) for text in texts]  # Remove all digits
#     if len(set(non_digit_parts)) == 1:
#         return []  # All items differ only by digits, not a valid cluster
#     return list(unique_texts)  # Return list of unique texts

# # Excluding specific clusters
# excluded_keys = [('DIVISION 27 - COMMUNICATIONS', '27411621'), ('DIVISION 27 - COMMUNICATIONS', '27411663')]
# clusters = {name: filter_unique_groups(group) for name, group in grouped if filter_unique_groups(group) and name not in excluded_keys}

# # Displaying clusters
# for cluster_key, texts in clusters.items():
#     if texts:  # Ensuring non-empty clusters
#         print(f"Cluster {cluster_key}:")
#         for text in texts:
#             print(f" - {text}")


# In[ ]:


# import pandas as pd
# import re

# # Assuming df is already defined and includes the 'Section' column

# # Normalize and extract key parts
# def normalize_text(text):
#     """Remove special characters except decimals and normalize spaces."""
#     if pd.isna(text):
#         return ""  # Return empty string if text is NaN
#     text = str(text)
#     return re.sub(r'[^A-Za-z0-9\s\.]', '', text)

# def extract_key_part(text):
#     """Extract first 6 characters and first 2 after the decimal if present."""
#     cleaned_text = re.sub(r'[^A-Za-z0-9\.]', '', text)
#     match = re.search(r'(\d+\.\d+)', cleaned_text)
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
#     else:
#         return cleaned_text[:6]

# df['Normalized_L2'] = df['Section'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# # Grouping and filtering
# grouped = df.groupby(['Division', 'Key_Part'])
# cluster_id = 0
# clusters_dict = {}

# for name, group in grouped:
#     unique_texts = filter_unique_groups(group)
#     if unique_texts and name not in excluded_keys:
#         clusters_dict[name] = (cluster_id, unique_texts)
#         cluster_id += 1

# # Assigning cluster IDs to DataFrame
# def assign_cluster_id(row):
#     for (division, key_part), (id, texts) in clusters_dict.items():
#         if (row['Division'], row['Key_Part']) == (division, key_part) and row['Normalized_L2'] in texts:
#             return id
#     return None  # Return None if no cluster ID is found

# df['L2_Clusters'] = df.apply(assign_cluster_id, axis=1)


# In[ ]:


# import difflib

# df['anomaly'] = ''  # Initialize the anomaly column

# # List of specific prefixes that trigger anomalies
# anomaly_prefixes = ['089119', '097200', '232113']

# # Normalize and extract key parts
# def normalize_text(text):
#     """Remove special characters except decimals and normalize spaces."""
#     if pd.isna(text):
#         return ""  # Return empty string if text is NaN
#     text = str(text)  # Ensure text is a string
#     return re.sub(r'[^A-Za-z0-9\s\.]', '', text)

# def extract_key_part(text):
#     """Extract first 6 characters and first 2 after the decimal if present, excluding anomaly triggers."""
#     if pd.isna(text):
#         return 'NaN'
#     cleaned_text = re.sub(r'[^A-Za-z0-9\.]', '', text)
#     if any(cleaned_text.startswith(prefix) for prefix in anomaly_prefixes):
#         return 'anomaly'  # Use 'anomaly' as a special key part for exclusion
#     match = re.search(r'(\d+\.\d+)', cleaned_text)
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
#     else:
#         return cleaned_text[:6]

# df['Normalized_L2'] = df['Dar_L2'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# # Mark entries with anomaly prefixes directly
# for prefix in anomaly_prefixes:
#     df.loc[df['Dar_L2'].fillna('').str.startswith(prefix), 'anomaly'] = 'anomaly - same sec num with different sec name'

# # Filter out anomalies for clustering
# filtered_df = df[df['Key_Part'] != 'anomaly']

# # Grouping and filtering for clustering
# grouped = filtered_df.groupby(['Dar_L1', 'Key_Part'])

# def check_anomaly(group):
#     """Check for anomalies where the first 6 digits are the same but subsequent text has less than 20% similarity."""
#     texts = [text[6:] for text in group['Normalized_L2'].tolist()]  # Get the subsequent text after the first 6 digits
#     if len(texts) > 1:  # Only consider groups with more than one entry
#         # Check for similarity by comparing each pair of texts
#         for i in range(len(texts)):
#             for j in range(i + 1, len(texts)):
#                 # Remove all non-alphanumeric characters for a clean comparison
#                 clean_text1 = re.sub(r'[^A-Za-z0-9]', '', texts[i])
#                 clean_text2 = re.sub(r'[^A-Za-z0-9]', '', texts[j])
#                 # Calculate similarity
#                 similarity = difflib.SequenceMatcher(None, clean_text1, clean_text2).ratio()
#                 # If similarity is less than 20%, mark as anomaly
#                 if similarity < 0.20:
#                     for idx in group.index:
#                         df.at[idx, 'anomaly'] = 'anomaly'

# for _, group in grouped:
#     check_anomaly(group)



# In[ ]:


# import pandas as pd
# import re

# # Assume df is your DataFrame and it has been properly defined

# # Normalize and extract key parts
# def normalize_text(text):
#     """Remove special characters except decimals and normalize spaces."""
#     if pd.isna(text):
#         return ""
#     text = str(text)
#     return re.sub(r'[^A-Za-z0-9\s\.]', '', text)

# def extract_key_part(text):
#     """Extract first 6 characters and first 2 after the decimal if present."""
#     cleaned_text = re.sub(r'[^A-Za-z0-9\.]', '', text)
#     match = re.search(r'(\d+\.\d+)', cleaned_text)
#     if match:
#         return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]
#     else:
#         return cleaned_text[:6]

# df['Normalized_L2'] = df['Section'].apply(normalize_text)
# df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# # Grouping and filtering
# grouped = df.groupby(['Division', 'Key_Part'])

# def filter_unique_groups(group):
#     texts = group['Normalized_L2'].tolist()
#     unique_texts = set(texts)
#     if len(unique_texts) == 1:
#         return []
#     non_digit_parts = [re.sub(r'\d', '', text) for text in texts]
#     if len(set(non_digit_parts)) == 1:
#         return []
#     return list(unique_texts)

# clusters = {name: filter_unique_groups(group) for name, group in grouped if filter_unique_groups(group)}

# # Renaming clusters and replacing items
# for cluster_key, texts in clusters.items():
#     if texts:
#         shortest_item = min(texts, key=len)  # Select the shortest item to name the cluster
#         # Update the DataFrame
#         df.loc[df['Normalized_L2'].isin(texts), 'L2'] = shortest_item  # Copy 'Dar_L2' to 'L2' and replace all items in the cluster with the shortest one



# In[ ]:


# def filter_unique_groups(group):
#     texts = group['Normalized_L2'].tolist()
#     unique_texts = set(texts)
#     if len(unique_texts) == 1:
#         return {}
#     non_digit_parts = [re.sub(r'\d', '', text) for text in texts]
#     if len(set(non_digit_parts)) == 1:
#         return {}
#     return {name: name for name in unique_texts}


# # Item Normalization and Clustering 

# In[52]:


import pandas as pd
import re

# Define the functions for text normalization and key part extraction
def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""  # Handle NaN values by returning an empty string
    return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
    match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
    return cleaned_text[:6]  # Use first 6 characters if no decimal number

# Applying the functions
df['Normalized_L2'] = df['L4_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# Group data by 'Dar_L1' and 'Key_Part'
grouped = df.groupby(['L1_Desc', 'Key_Part'])

def filter_unique_groups(group):
    """Identify and filter groups based only on non-special character differences."""
    texts = group['L4_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []  # No valid cluster if all items are identical
    non_special_char_parts = [re.sub(r'\W+', '', text).lower() for text in texts]  # Ignore special characters
    if len(set(non_special_char_parts)) == 1:
        return texts  # Valid cluster if items differ only by special characters
    return []

# Extract clusters and assign IDs
clusters = {name: filter_unique_groups(group) for name, group in grouped if filter_unique_groups(group)}
cluster_id_map = {}
representative_name_map = {}

# Assign unique IDs and prepare to populate the new column
for i, (cluster_key, texts) in enumerate(clusters.items()):
    if texts:
        cluster_id = f"Cluster_{i + 1}"
        representative_name = sorted(texts)[0]  # Choosing the first name alphabetically as representative
        representative_name_map[cluster_id] = representative_name
        for text in texts:
            cluster_id_map[text] = cluster_id

# Map the cluster IDs and representative names back to the DataFrame
df['L4_Clustering'] = df['L4_Desc'].map(cluster_id_map).fillna("No Cluster")
df['Dar_L4'] = df['L4_Clustering'].map(representative_name_map).fillna("No Cluster")



# In[50]:


import pandas as pd
import re

# Define the functions for text normalization and key part extraction
def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""  # Handle NaN values by returning an empty string
    return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
    match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
    #return cleaned for each text in the group by appending the cluster ID to each element in the DataFrame row.
    return cleaned_text[:6]  # Use first 6 characters if no decimal number

# Applying the functions
df['Normalized_L2'] = df['L4_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# Group data by 'Dar_L1' and 'Key_Part'
grouped = df.groupby(['L1_Desc', 'Key_Part'])

def filter_unique_groups(group):
    """Identify and filter groups based only on non-special character differences."""
    texts = group['L4_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []  # No valid cluster if all items are identical
    non_special_char_parts = [re.sub(r'\W+', '', text).lower() for text in texts]  # Ignore special characters
    if len(set(non_special_char_parts)) == 1:
        return texts  # Valid cluster if items differ only by special characters
    return []

# Extract clusters and assign IDs
clusters = {name: filter_unique_groups(group) for name, group in grouped if filter_unique_groups(group)}
cluster_id_map = {}

# Assign unique IDs and prepare to populate the new column
for i, (cluster_key, texts) in enumerate(clusters.items()):
    if texts:
        cluster_id = f"Cluster_{i + 1}"
        for text in texts:
            cluster_id_map[text] = cluster_id

# Map the cluster IDs back to the DataFrame
df['L4_Clustering'] = df['L4_Desc'].map(cluster_id_map).fillna("No Cluster")


# In[54]:


# Map the cluster IDs and representative names back to the DataFrame
df['L4_Clustering'] = df['L4_Desc'].map(cluster_id_map).fillna("No Cluster")
df['Dar_L4'] = np.where(df['L4_Clustering'] == "No Cluster", df['L4_Desc'], df['L4_Clustering'].map(representative_name_map))


# In[55]:


df.head()


# In[ ]:


# Iterate through clusters, gather items, choose cluster name, and replace occurrences in 'l4'
for cluster_key, texts in clusters.items():
    if texts:  # Ensuring non-empty clusters
        # Gather all items within the cluster
        cluster_items = [item for sublist in clusters.values() for item in sublist]
        # Choose the shortest item as the cluster name
        cluster_name = min(cluster_items, key=len)
        # Replace occurrences in 'l4' with the chosen cluster name
        df.loc[df['l4'].isin(texts), 'l4'] = cluster_name

# Displaying the updated DataFrame with 'l4' column containing unified cluster names
print(df[['Dar_L1', 'Key_Part', 'Normalized_L2', 'l4']])


# In[ ]:


df.to_excel('C:\\Users\\yymahmoudali\\Desktop\\sorting.xlsx')


# ## Level 3 Normalization and Clustering 

# In[63]:


clusters = {}
for name, group in grouped:
    texts = filter_unique_groups(group)
    if texts:
        proposed_name = min(texts, key=len)  # Choosing the shortest name as the unified name
        clusters[name] = proposed_name

# Mapping proposed names back to the 'L4' column based on the clusters
def get_proposed_name(row):
    key = (row['L1_Desc'], row['Key_Part'])
    return clusters.get(key, row['L4_Desc'])

df['Unified_L4'] = df.apply(get_proposed_name, axis=1)


# In[6]:


import pandas as pd
import re

def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""  # Handle NaN values by returning an empty string
    return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
    match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
    return cleaned_text[:6]  # Use first 6 characters if no decimal number

def filter_unique_groups(group):
    """Identify and filter groups based only on non-special character differences."""
    texts = group['L4_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []  # No valid cluster if all items are identical
    non_special_char_parts = [re.sub(r'\W+', '', str(text)).lower() for text in texts]  # Ignore special characters and ensure string input
    if len(set(non_special_char_parts)) == 1:
        return texts  # Valid cluster if items differ only by special characters
    return []

def is_similar(text1, text2):
    """Check if two texts are similar based on a simplistic similarity criterion."""
    text1 = str(text1) if not pd.isna(text1) else ""
    text2 = str(text2) if not pd.isna(text2) else ""
    return re.sub(r'\W+', '', text1).lower() == re.sub(r'\W+', '', text2).lower()


# Applying functions
df['Normalized_L2'] = df['L4_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# Group data by 'Dar_L1' and 'Key_Part'
grouped = df.groupby(['L1_Desc', 'Key_Part'])

# Create clusters
clusters = {}
for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        clusters[name] = filtered_texts

# Process clusters to add similar items
for cluster_key, texts in list(clusters.items()):
    base_text = texts[0]
    additional_texts = []
    for text in df['L4_Desc'].tolist():
        if text not in texts and is_similar(base_text, text):
            additional_texts.append(text)
    clusters[cluster_key].extend(additional_texts)

# Displaying clusters
for cluster_key, texts in clusters.items():
    if texts:  # Ensuring non-empty clusters
        print(f"Cluster {cluster_key}:")
        for text in texts:
            print(f" - {text}")


# In[9]:


import pandas as pd
import re

def normalize_text(text):
    """Normalize text by removing special characters, keeping only alphanumerics and spaces."""
    if pd.isna(text):
        return ""  # Handle NaN values by returning an empty string
    return re.sub(r'[^\w\s.]', '', str(text))  # Keep alphanumerics, spaces, and periods

def extract_key_part(text):
    """Extract key parts for clustering, focusing on non-special character content."""
    cleaned_text = re.sub(r'[^\w.]', '', str(text))  # Remove special characters but keep alphanumerics and decimals
    match = re.search(r'(\d+\.\d+)', cleaned_text)  # Find decimal numbers
    if match:
        return match.group(1).split('.')[0][:6] + match.group(1).split('.')[1][:2]  # Use first 6 and 2 decimal digits
    return cleaned_text[:6]  # Use first 6 characters if no decimal number

def filter_unique_groups(group):
    """Identify and filter groups based only on non-special character differences."""
    texts = group['L4_Desc'].tolist()
    unique_texts = set(texts)
    if len(unique_texts) == 1:
        return []  # No valid cluster if all items are identical
    non_special_char_parts = [re.sub(r'\W+', '', str(text)).lower() for text in texts]  # Ignore special characters and ensure string input
    if len(set(non_special_char_parts)) == 1:
        return texts  # Valid cluster if items differ only by special characters
    return []

def is_similar(text1, text2):
    """Check if two texts are similar based on a simplistic similarity criterion."""
    text1 = str(text1) if not pd.isna(text1) else ""
    text2 = str(text2) if not pd.isna(text2) else ""
    return re.sub(r'\W+', '', text1).lower() == re.sub(r'\W+', '', text2).lower()

# Assuming you have your DataFrame 'df' ready

# Applying functions
df['Normalized_L2'] = df['L4_Desc'].apply(normalize_text)
df['Key_Part'] = df['Normalized_L2'].apply(extract_key_part)

# Group data by 'Dar_L1' and 'Key_Part'
grouped = df.groupby(['L1_Desc', 'Key_Part'])

# Create clusters
clusters = {}
cluster_names = {}
for name, group in grouped:
    filtered_texts = filter_unique_groups(group)
    if filtered_texts:
        clusters[name] = filtered_texts
        cluster_names[filtered_texts[0]] = filtered_texts  # Mapping first text to the list of texts in the cluster

# Process clusters to add similar items
for cluster_name, texts in list(cluster_names.items()):
    base_text = texts[0]
    additional_texts = []
    for text in df['L4_Desc'].tolist():
        if text not in texts and is_similar(base_text, text):
            additional_texts.append(text)
    cluster_names[cluster_name].extend(additional_texts)

# Replacing 'Dar_L2' entries with the cluster names
for cluster_key, texts in clusters.items():
    if texts:  # Ensuring non-empty clusters
        cluster_name = cluster_names[texts[0]][0]  # Get the cluster name
        df.loc[df['L2_Desc'].isin(texts), 'L2_Desc'] = cluster_name



# ## Clustering L4

# In[ ]:


## L4 ##
# 1. If there is similar items check if they have the same Dar_L3 and Dar_L2. If so, put them in a cluster 
# 2. If the items of the clusters are the same but with different numbers, get them out of the cluster 
# 3. Rename the clusters with one name represent all the items of the cluster 


# In[ ]:


# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.metrics.pairwise import cosine_similarity
# from scipy.cluster.hierarchy import linkage, fcluster
# from scipy.spatial.distance import squareform
# import pandas as pd

# # Assuming df is your DataFrame containing 'L4' and 'Dar_L2'
# data_grouped = df.dropna(subset=['L4', 'Dar_L2']).groupby('Dar_L2')

# clustered_items = {}
# cluster_label = 1

# for name, group in data_grouped:
#     data = group['L4'].unique()
#     if len(data) > 1:  # Ensure there are at least two items to cluster
#         vectorizer = TfidfVectorizer()
#         X = vectorizer.fit_transform(data)

#         # Calculate cosine similarity matrix
#         similarity_matrix = cosine_similarity(X)

#         # Convert similarity matrix to a distance matrix
#         distance_matrix = 1 - similarity_matrix
#         # Ensure there are no negative distances
#         distance_matrix[distance_matrix < 0] = 0

#         # Convert the square distance matrix to a condensed format required by the linkage function
#         condensed_distance_matrix = squareform(distance_matrix, checks=False)

#         # Compute linkage for hierarchical clustering
#         linked = linkage(condensed_distance_matrix, method='average')

#         # Apply flat clustering to form clusters based on distance threshold
#         labels = fcluster(linked, t=0.15, criterion='distance')  # 85% similarity threshold

#         # Map each item to its cluster within this group
#         for item, label in zip(data, labels):
#             full_label = f"{name}-{cluster_label}-{label}"
#             if full_label not in clustered_items:
#                 clustered_items[full_label] = []
#             clustered_items[full_label].append(item)
#     else:
#         # Handle groups with fewer than two items by assigning them to a default single-item cluster
#         for item in data:
#             full_label = f"{name}-{cluster_label}"
#             clustered_items[full_label] = [item]

#     cluster_label += 1  # Increment the cluster label for uniqueness across groups

# # Print the first three clusters with more than one item
# cluster_count = 0
# for label, items in clustered_items.items():
#     if len(items) > 1:
#         print(f"Cluster {label}: {items}")
#         cluster_count += 1
#         if cluster_count == 3:  # Stop after printing the first three valid clusters
#             break


# In[ ]:


import re

def normalize_text(text):
    """Normalize text by extracting the non-numeric part."""
    return re.sub(r'\d+', '', text).strip()

# Assuming `clustered_items` contains your original clusters
data = [item for sublist in clustered_items.values() for item in sublist]  # Flatten the list of items

# Initialize a dictionary to store new clusters
new_clustered_items = {}

# Generate unique clusters based on text normalization and existing numbers
for item in data:
    normalized_key = normalize_text(item)
    numbers = re.findall(r'\d+', item)  # Extract numbers
    full_key = f"{normalized_key} {numbers}"  # Use the extracted numbers to form the full key

    if full_key not in new_clustered_items:
        new_clustered_items[full_key] = []
    new_clustered_items[full_key].append(item)

# Print the newly formed clusters that have more than one distinct item
for label, items in new_clustered_items.items():
    if len(set(items)) > 1:  # Check if the cluster has more than one unique item
        print(f"Cluster for {label}: {items}")



# ## Clustering L3

# In[ ]:


## Dar_L3 ##
# 1. Parse the Dar_L3 based on '/*/'
# 2. get a list of the sentences of Dar_L3
# 3. comapre this list with the other lists in Dar_L3 
# 4. if they are similar and has the same Dar_L2 put them in a cluster 


# In[3]:


vectorizer = TfidfVectorizer(max_features=10000)  # Adjust the number of features according to your dataset


# In[44]:


from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd
import re

def extract_numbers(text):
    numbers = re.findall(r'\b\d+\b', text)
    return sorted(float(num) for num in numbers)

# Example: Using a subset of data for demonstration
df_sample = df.sample(frac=0.1)  # Adjust the fraction according to your memory capacity

# Fill NaN values
df_sample['L3_Desc'] = df_sample['L3_Desc'].fillna("")
df_sample['Dar_L2'] = df_sample['Dar_L2'].fillna("")

# Vectorizing with limited features
vectorizer = TfidfVectorizer(max_features=10000)
X = vectorizer.fit_transform(df_sample['L3_Desc'])

# Computing cosine similarity matrix
similarity_matrix = cosine_similarity(X)

# Further steps would be similar to your original approach, with attention to memory optimization


# Define a similarity threshold
similarity_threshold = 0.9

# Creating clusters based on similarity threshold, Dar_L2 value, and numerical consistency
clusters = []
used = np.zeros(len(df), dtype=bool)

for i in range(len(df)):
    if not used[i]:
        current_numbers = extract_numbers(df.iloc[i]['Dar_L3'])
        new_cluster = []
        for j in range(len(df)):
            if similarity_matrix[i][j] > similarity_threshold and not used[j] and df.iloc[j]['Dar_L2'] == df.iloc[i]['Dar_L2']:
                candidate_numbers = extract_numbers(df.iloc[j]['Dar_L3'])
                if candidate_numbers == current_numbers:
                    new_cluster.append(j)
        clusters.append(new_cluster)
        used[new_cluster] = True

# Ensuring items in clusters are not repeated and only clusters with more than one item are considered
cluster_items = set()
filtered_clusters = []

for cluster in clusters:
    new_cluster = []
    for idx in cluster:
        item = df.iloc[idx]['L3_Desc']
        if item not in cluster_items:
            cluster_items.add(item)
            new_cluster.append(idx)
    if new_cluster and len(new_cluster) > 1:  # Only add non-empty clusters with more than one item
        filtered_clusters.append(new_cluster)
print("Number of output clusters:", len(filtered_clusters))

# Print only the clusters with more than one item with numbering for each item
for index, cluster in enumerate(filtered_clusters):
    print(f"Cluster {index + 1}:")
    for idx, item_idx in enumerate(cluster):
        print(f"{idx + 1}. {df.iloc[item_idx]['L3_Desc']} (L2_Desc: {df.iloc[item_idx]['Dar_L2']})")
    print("\n")


# In[5]:


from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd
import re

def extract_numbers(text):
    numbers = re.findall(r'\b\d+\b', text)
    return sorted(float(num) for num in numbers)

# Example: Using a subset of data for demonstration
df_sample = df.sample(frac=0.1)  # Adjust the fraction according to your memory capacity

# Fill NaN values
df_sample['L3_Desc'] = df_sample['L3_Desc'].fillna("")
df_sample['Dar_L2'] = df_sample['Dar_L2'].fillna("")

# Vectorizing with limited features
vectorizer = TfidfVectorizer(max_features=10000)
X = vectorizer.fit_transform(df_sample['L3_Desc'])

# Computing cosine similarity matrix
similarity_matrix = cosine_similarity(X)

# Define a similarity threshold
similarity_threshold = 0.9

# Creating clusters based on similarity threshold, Dar_L2 value, and numerical consistency
clusters = []
used = np.zeros(len(df_sample), dtype=bool)  # Use the length of df_sample here

for i in range(len(df_sample)):
    if not used[i]:
        current_numbers = extract_numbers(df_sample.iloc[i]['L3_Desc'])
        new_cluster = []
        for j in range(len(df_sample)):
            if similarity_matrix[i][j] > similarity_threshold and not used[j] and df_sample.iloc[j]['Dar_L2'] == df_sample.iloc[i]['Dar_L2']:
                candidate_numbers = extract_numbers(df_sample.iloc[j]['L3_Desc'])
                if candidate_numbers == current_numbers:
                    new_cluster.append(j)
        if new_cluster:
            clusters.append(new_cluster)
            used[new_cluster] = True

# Initialize new columns for the cluster name and number in df_sample
df_sample['Cluster'] = ''  # Empty string initialization

# Assign cluster names and numbers only to clusters with diverse items
for index, cluster in enumerate(clusters):
    if len(cluster) > 1:
        unique_texts = set(df_sample.iloc[idx]['L3_Desc'] for idx in cluster)
        if len(unique_texts) > 1:  # Check if there are diverse texts in the cluster
            shortest_name = min(unique_texts, key=len)
            cluster_name = f"Cluster {index + 1}"
            for idx in cluster:
                df_sample.at[idx, 'Cluster'] = cluster_name



# In[45]:


# Initialize new columns for the cluster name and number in df_sample
df_sample['Cluster'] = ''  # Empty string initialization

# Assign cluster names and numbers only to clusters with diverse items
for index, cluster in enumerate(clusters):
    if len(cluster) > 1:
        unique_texts = set(df_sample.iloc[idx]['L3_Desc'] for idx in cluster)
        if len(unique_texts) > 1:  # Check if there are diverse texts in the cluster
            shortest_name = min(unique_texts, key=len)
            cluster_name = f"Cluster {index + 1}: {shortest_name}"
            cluster_detail = []
            for idx in cluster:
                item_desc = df_sample.iloc[idx]['L3_Desc']
                df_sample.at[idx, 'Cluster'] = cluster_name
                cluster_detail.append(item_desc)
            # Print the cluster name and its unique items
            print(f"\n{cluster_name}")
            for item in sorted(set(cluster_detail)):  # Ensure items are unique and sorted
                print(f" - {item}")


# In[16]:


# Initialize new columns for the cluster name and number in df_sample
df_sample['Cluster'] = ''  # Empty string initialization
df_sample['L3_Clustering'] = ''  # Initialize a new column to store cluster numbers or identifiers

# Assign cluster names and numbers only to clusters with diverse items
for index, cluster in enumerate(clusters):
    if len(cluster) > 1:
        unique_texts = set(df_sample.iloc[idx]['L3_Desc'] for idx in cluster)
        if len(unique_texts) > 1:  # Check if there are diverse texts in the cluster
            shortest_name = min(unique_texts, key=len)
            cluster_name = f"Cluster {index + 1}: {shortest_name}"
            cluster_detail = []
            for idx in cluster:
                item_desc = df_sample.iloc[idx]['L3_Desc']
                df_sample.at[idx, 'Cluster'] = cluster_name
                df_sample.at[idx, 'L3_Clustering'] = f"Cluster {index + 1}"
                cluster_detail.append(item_desc)
            # Print the cluster name and its unique items
            print(f"\n{cluster_name}")
            for item in sorted(set(cluster_detail)):  # Ensure items are unique and sorted
                print(f" - {item}")

# After processing all clusters, check if there are any descriptions not assigned to a cluster
for i in range(len(df_sample)):
    if df_sample.iloc[i]['L3_Clustering'] == '':  # This checks if the cluster number is still empty
        df_sample.at[i, 'L3_Clustering'] = 'No Cluster'  # Assign 'No Cluster' if the description was not part of any clusters

# Displaying the final DataFrame to check results


# In[8]:


# Initialize new columns for the cluster name and number in df_sample
df_sample['Cluster'] = ''  # Empty string initialization
df_sample['L3_Clustering'] = ''  # Initialize a new column to store cluster numbers or identifiers

# Store details for printing unique items later
cluster_details = {}

# Assign cluster names and numbers only to clusters with diverse items
for index, cluster in enumerate(clusters):
    if len(cluster) > 1:
        unique_texts = set(df_sample.iloc[idx]['L3_Desc'] for idx in cluster)
        if len(unique_texts) > 1:  # Check if there are diverse texts in the cluster
            shortest_name = min(unique_texts, key=len)
            cluster_name = f"Cluster {index + 1}: {shortest_name}"
            for idx in cluster:
                item_desc = df_sample.iloc[idx]['L3_Desc']
                df_sample.at[idx, 'Cluster'] = cluster_name
                df_sample.at[idx, 'L3_Clustering'] = f"Cluster {index + 1}"
            # Store unique items for later printing
            cluster_details[cluster_name] = unique_texts

# Print unique items for each cluster after all have been processed
for cluster_name, items in cluster_details.items():
    print(f"\n{cluster_name}")
    for item in sorted(items):  # Ensure items are unique and sorted
        print(f" - {item}")

# After processing all clusters, check if there are any descriptions not assigned to a cluster
for i in range(len(df_sample)):
    if df_sample.iloc[i]['L3_Clustering'] == '':  # This checks if the cluster number is still empty
        df_sample.at[i, 'L3_Clustering'] = 'No Cluster'  # Assign 'No Cluster' if the description was not part of any clusters

# Optionally, display some of the DataFrame to verify the cluster assignments (for debugging or verification)


# In[7]:





# In[9]:


# Initialize new columns for the cluster name and number in df_sample
df_sample['Cluster'] = ''  # Empty string initialization
df_sample['L3_Clustering'] = ''  # Initialize a new column to store cluster numbers or identifiers

# Store details for printing unique items later and for assigning cluster numbers
description_to_cluster = {}

# Assign cluster names and numbers only to clusters with diverse items
for index, cluster in enumerate(clusters):
    if len(cluster) > 1:
        unique_texts = set(df_sample.iloc[idx]['L3_Desc'] for idx in cluster)
        if len(unique_texts) > 1:  # Check if there are diverse texts in the cluster
            shortest_name = min(unique_texts, key=len)
            cluster_name = f"Cluster {index + 1}: {shortest_name}"
            for idx in cluster:
                item_desc = df_sample.iloc[idx]['L3_Desc']
                df_sample.at[idx, 'Cluster'] = cluster_name
                # Map descriptions to cluster number
                description_to_cluster[item_desc] = f"Cluster {index + 1}"

# Assign cluster numbers to L3_Desc based on found descriptions
for i in range(len(df_sample)):
    item_desc = df_sample.iloc[i]['L3_Desc']
    if item_desc in description_to_cluster:
        df_sample.at[i, 'L3_Clustering'] = description_to_cluster[item_desc]
    else:
        df_sample.at[i, 'L3_Clustering'] = ''  # Leave empty if no matching cluster

# Print unique items for each cluster after all have been processed
for cluster_name, items in description_to_cluster.items():
    print(f"\n{cluster_name}")
    for item in sorted(set(items)):  # Ensure items are unique and sorted
        print(f" - {item}")

# Optionally, display some of the DataFrame to verify the cluster assignments


# In[10]:


df_sample.to_excel('C:\\Users\\yymahmoudali\\Desktop\\STD15_clustered.xlsx')


# In[ ]:





# In[ ]:


## New Approach:
## Sort first L3 based on letters and group by L1, L2
## If you found in Dar_L6 a null row, check the similarity between it and the next and previous rows, if high similarity, cluster them. 
## When as per is mentioned in one of the compared similarity cells mark as "Possible Anomaly"
# if there is main differnce in numbers, don't cluster 
## If two consective empty rows, compare the first one wit hthe above row and the next one with the following one and compare them to each other too


# In[ ]:


## Trila I ##


# In[ ]:


import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform


# Group by 'Dar_L2' and count the occurrences
group_counts = df.groupby('Dar_L2').size()

# Filter groups by those having the same number of elements
same_size_groups = group_counts[group_counts == group_counts.iloc[0]].index

# Filter the DataFrame to only include groups of the same size
filtered_df = df[df['Dar_L2'].isin(same_size_groups)]

# Vectorize the filtered data
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(filtered_df['L4'])

# Calculate cosine similarity matrix
similarity_matrix = cosine_similarity(X)

# Convert similarity matrix to a distance matrix
distance_matrix = 1 - similarity_matrix
np.fill_diagonal(distance_matrix, 0)
distance_matrix[distance_matrix < 0] = 0

# Convert to condensed format
condensed_distance_matrix = squareform(distance_matrix)

# Compute linkage for hierarchical clustering
linked = linkage(condensed_distance_matrix, method='average')

# Apply flat clustering
labels = fcluster(linked, t=0.2, criterion='distance')

# Output cluster assignment
filtered_df['Cluster'] = labels

# Print clusters with more than one unique 'Dar_L3' item
for cluster_id, group in filtered_df.groupby('Cluster'):
    if len(group['Dar_L3'].unique()) > 1:
        print(f"Cluster {cluster_id} with unique 'Dar_L3' items:")
        print(group[[ 'Dar_L3']], '\n')


# ## Statitical Analysis on the Rate 

# In[ ]:


df['Dar_Rate'] = pd.to_numeric(df['Dar_Rate'], errors='coerce')

# Drop rows where 'Dar_Rate' became NaN after conversion
df = df.dropna(subset=['Dar_Rate'])

# Group by 'L4' and calculate statistics
grouped_stats = df.groupby('L4')['Dar_Rate'].agg(['mean', 'median', 'max', 'min', 'std'])

# Merge these statistics back into the original DataFrame
df = df.merge(grouped_stats, on='L4', how='left')

# Optionally, save the result to a new Excel file
df.to_excel('C:\\Users\\yymahmoudali\\Desktop\\output_statistics.xlsx')


# In[ ]:


import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfV]ectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

# Example DataFrame setup (ensure this reflects your actual data structure)

# Group items by 'Dar_L3' and filter groups with at least two items
grouped = df.groupby('Dar_L3').filter(lambda x: len(x) > 1)

# Initialize dictionary for storing clusters
clustered_items = {}

# Vectorizing and clustering process
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(grouped['L4'])
similarity_matrix = cosine_similarity(X)
distance_matrix = 1 - similarity_matrix
np.fill_diagonal(distance_matrix, 0)
condensed_distance_matrix = squareform(distance_matrix)
linked = linkage(condensed_distance_matrix, method='average')
labels = fcluster(linked, t=0.15, criterion='distance')

# Append items and rates to clusters based on labels
for label, item, rate in zip(labels, grouped['L4'], grouped['Dar_Rate']):
    if label not in clustered_items:
        clustered_items[label] = {'items': [], 'rates': []}
    clustered_items[label]['items'].append(item)
    clustered_items[label]['rates'].append(rate)

# Function to detect outliers using IQR
def detect_outliers(rates):
    Q1, Q3 = np.percentile(rates, [25, 75])
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = [rate for rate in rates if rate < lower_bound or rate > upper_bound]
    return outliers

# Print clusters that contain outliers
for cluster_id, content in clustered_items.items():
    outliers = detect_outliers(content['rates'])
    if outliers:
        print(f"Cluster {cluster_id} Outliers: {outliers}")
        print(f"Items: {content['items']}")
        print(f"Rates: {content['rates']}\n")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


pip install pandas forex-python


# In[ ]:


import pandas as pd
from forex_python.converter import CurrencyRates

# Function to convert currency
def convert_to_usd(amount):
    try:
        # Instantiate CurrencyRates object
        c = CurrencyRates()
        # Convert JOD to USD
        usd_amount = c.convert('JOD', 'USD', amount)
        return usd_amount
    except Exception as e:
        print(f"Error converting currency: {e}")
        return None

# Load your dataset

# Convert the 'Dar_Rate' column to USD and create a new column
df['Dar_Rate_in_USD'] = df['Dar_Rate'].apply(convert_to_usd)

# Save the updated DataFrame to a new CSV file

print("Conversion completed and file saved.")


# In[ ]:




