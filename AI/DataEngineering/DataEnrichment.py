

import openai
import os
import json
import pandas as pd
from openai import OpenAI
import numpy as np
import matplotlib.pyplot as plt

# Set up OpenAI API key
client = OpenAI(api_key='')

def clean(dict_variable):
    return next(iter(dict_variable.values()))

# 2. Example 1: Analyzing and Classifying News Articles

# Sample news article
news_article = """
[ARTICLE 1]
The stock market saw significant gains today as tech companies reported strong quarterly earnings. 
Apple, Microsoft, and Amazon all beat analyst expectations, driving the NASDAQ to new highs. 
Meanwhile, concerns about inflation have eased slightly following the Federal Reserve's latest statement.

[ARTICLE 2]
Simone Biles fought through pain in her calf on Sunday to post an impressive all-around score and display 
the qualities which have made one of the greatest gymnasts of all time on her return to the Olympics.
"""

# Analyze and classify the article
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful assistant that analyzes and classifies news articles."},
        {"role": "user", "content": f"Analyze this news article and classify it into one of these categories: Technology, Sports, Other. Output in JSON form: {news_article}"}
    ],
    response_format={"type": "json_object"}
)

print(response.choices[0].message.content)

# This example demonstrates how Gen AI can analyze a news article, classify it into categories, and provide a summary. 
# This is useful for data engineers working with large collections of news articles, helping to organize and extract key information efficiently.

# 2. Example 1: Filling Missing Text Data (Text Data Imputation)

# Sample dataset with missing text data
data = {
    'Title': ['The Great Gatsby', 'To Kill a Mockingbird', '1984', 'Pride and Prejudice'],
    'Author': ['F. Scott Fitzgerald', 'Harper Lee', 'George Orwell', None],
    'Genre': ['Novel', 'Fiction', 'Dystopian', 'Romance']
}
df = pd.DataFrame(data)
print("Original DataFrame:")
print(df)

# Function to predict missing author
for index, row in df.iterrows():
    if row['Author'] is None:
        prompt = 'Who is the author of this book? Return only the answer. {}'.format(row['Title'])
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        df.at[index, 'Author'] = response.choices[0].message.content

print(df)

# Sample dataset with missing text data
data = {
    'Book Title': [None, 'To Kill a Mockingbird', '1984', 'Pride and Prejudice'],
    'Author': ['F. Scott Fitzgerald', 'Harper Lee', 'George Orwell', None],
    'Genre': ['Novel', 'Fiction', 'Dystopian', 'Romance']
}
df = pd.DataFrame(data)
print("Original DataFrame:")
print(df)

for index, row in df.iterrows():
    for col in df.columns:
        if row[col] is None:
            prompt = 'Given this information: {}. What is the {}? Return only the answer'.format(
                [{col_loop: row[col_loop]} for col_loop in df.columns if col_loop != col], col)
            print(prompt)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}]
            )
            df.at[index, col] = response.choices[0].message.content

print(df)

# 2. Example 2: Filling Missing Numerical Data

# Sample dataset with missing numerical data
data_num = pd.DataFrame({
    'Product': ['A', 'B', 'C', 'D', 'E'],
    'Price': [10.99, np.nan, 15.50, 8.75, np.nan],
    'Quantity': [100, 150, np.nan, 200, 175]
})

print("Original dataset:")
print(data_num)

# Function to fill missing numerical data
def fill_missing_numerical(row):
    if pd.isna(row['Price']) or pd.isna(row['Quantity']):
        prompt = f"Predict the missing values for this product: Product: {row['Product']}, Price: {row['Price']}, Quantity: {row['Quantity']}. Consider market trends and product characteristics. Output in JSON form."
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        if pd.isna(row['Price']):
            row['Price'] = float(result.get('Price', row['Price']))
        if pd.isna(row['Quantity']):
            row['Quantity'] = int(result.get('Quantity', row['Quantity']))
    return row

# Apply the function to each row
data_num_filled = data_num.apply(fill_missing_numerical, axis=1)

print("Dataset with filled missing values:")
print(data_num_filled)

# Sample dataset with missing numerical data
data_num = pd.DataFrame({
    'Product': ['iPhone 12', 'iPhone 11', 'iPhone X', 'iPhone 7', 'iPhone 14'],
    'Price': [599, 499, 399, 299, np.nan],
    'Number of days since release': [1374, 1773, np.nan, 2872, 681]
})

print("Original dataset:")
print(data_num)

# Function to fill missing numerical data
def fill_missing_numerical(row):
    if pd.isna(row['Price']) or pd.isna(row['Number of days since release']):
        prompt = f"Predict the missing values for this product: Product: {row['Product']}, Price: {row['Price']}, Quantity: {row['Number of days since release']}. Consider market trends and product characteristics. You must provide a value. Only return the value, nothing else"
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        result = response.choices[0].message.content
        if pd.isna(row['Price']):
            row['Price'] = result
        if pd.isna(row['Number of days since release']):
            row['Number of days since release'] = result
    return row

# Apply the function to each row
data_num_filled = data_num.apply(fill_missing_numerical, axis=1)

print("Dataset with filled missing values:")
print(data_num_filled)

# 2. Example 3: Filling Missing Time Series Data

# Sample time series dataset with missing values
dates = pd.date_range(start='2023-01-01', end='2023-01-10', freq='D')
data_ts = pd.DataFrame({
    'Date': dates,
    'Temperature': [20, 22, np.nan, 19, 18, np.nan, 23, 25, 24, np.nan]
})

print("Original dataset:")
print(data_ts)

# Function to fill missing time series data
def fill_missing_timeseries(data):
    context = data.to_json(orient='records', date_format='iso')
    prompt = f"Fill in the missing temperature values in this time series data. Consider trends and patterns. Data: {context}. Output in JSON form."
    print(prompt)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    result = json.loads(response.choices[0].message.content)
    filled_data = pd.DataFrame(clean(result))
    filled_data['Date'] = pd.to_datetime(filled_data['Date'])
    return filled_data

# Apply the function to the dataset
data_ts_filled = fill_missing_timeseries(data_ts)

print("Dataset with filled missing values:")
print(data_ts_filled)

# Plotting the data
plt.figure(figsize=(10, 6))
plt.plot(data_ts['Date'], data_ts['Temperature'], marker='o')
plt.title('Temperature Over Time')
plt.xlabel('Date')
plt.ylabel('Temperature')
plt.grid(True)
plt.show()

plt.figure(figsize=(10, 6))
plt.plot(data_ts_filled['Date'], data_ts_filled['Temperature'], marker='o')
plt.title('Temperature Over Time')
plt.xlabel('Date')
plt.ylabel('Temperature')
plt.grid(True)
plt.show()

# 2. Example 4: Converting Data Types

# Sample data with mixed data types
data = {
    'ID': ['001', '002', '003', '00006'],
    'Value': ['10.5', '15', 'twenty', 'thirty-one'],
    'Date': ['2023-01-01', '01/15/2023', 'March 1, 2023', "Dec 4,, '94"]
}
df = pd.DataFrame(data)
print("Original data:")
print(df)

# Using Gen AI to convert data types
prompt = f"Convert the following data to appropriate data types. ID should be integer, Value should be float (convert 'twenty' to 20.0), and Date should be in ISO format (YYYY-MM-DD). Output in JSON form:\n{df.to_json()}"

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)

converted_data = json.loads(response.choices[0].message.content)
print("\nConverted data:")
print(json.dumps(converted_data, indent=2))

# Create a new DataFrame with converted values
df_converted = pd.DataFrame(converted_data)
df_converted['ID'] = df_converted['ID'].astype(int)
df_converted['Value'] = df_converted['Value'].astype(float)
df_converted['Date'] = pd.to_datetime(df_converted['Date'])
print("\nFinal converted DataFrame:")
print(df_converted)
print("\nData types:")
print(df_converted.dtypes)

addresses = [
    "123 Main St, Apt 4, New York, NY 10001",
    "60601 456 Elm Avenue, Chicago",
    "Los Angeles 789 Oak Rd Suite 100 Cali 90001"
]

prompt = f"Normalize these addresses to a standard format: {addresses}. Output in JSON form with keys: street, city, state, zip."

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)

print(response.choices[0].message.content)

pd.DataFrame(clean(json.loads(response.choices[0].message.content)))

# 2. Example 5: Normalizing Text Data

# Sample data with inconsistent text formatting
data = {
    'descriptions': ['RED apple', 'Green BANANA', 'yellow Lemon', 'ORANGE orange']
}
df = pd.DataFrame(data)
print("Original data:")
print(df)

# Use Gen AI to normalize text data
prompt = f"Normalize these fruit descriptions by capitalizing only the first letter of each word: {df['descriptions'].tolist()}. Output in JSON form"

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)

normalized_descriptions = json.loads(response.choices[0].message.content)
print("Normalized descriptions:")
print(clean(normalized_descriptions))

# Update the DataFrame with normalized descriptions
df['normalized_descriptions'] = clean(normalized_descriptions)
print("Updated DataFrame:")
print(df)

# 2. Example 6: Standardizing Categorical Data

# Sample data with inconsistent categorical values
data = {
    'categories': ['IT', 'Information Technology', 'Tech', 'I.T.', 'Information Tech', 'Cyber', 'Cyberscryity', 'Cybersecurity']
}
df = pd.DataFrame(data)
print("Original data:")
print(df)

# Use Gen AI to standardize categorical data
prompt = f"Standardize these category names to a single consistent format (either 'IT' or 'Cybersecurity'): {df['categories'].tolist()}. Output in JSON form"

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)

standardized_categories = json.loads(response.choices[0].message.content)
print("Standardized categories:")
print(standardized_categories)

# Update the DataFrame with standardized categories
df['standardized_categories'] = clean(standardized_categories)
print("Updated DataFrame:")
print(df)

# 2. Example 7: Standardizing Product Names

# Sample product names
product_names = [
    "iPhone 12 Pro Max 256GB",
    "Apple iphone 12 pro max (256 gb)",
    "12 Pro Max iPhone - 256GB"
]

# Function to standardize product name
def standardize_product_name(name):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a product name standardization assistant."},
            {"role": "user", "content": f"Standardize this product name to the format 'Brand Model Storage'. Product name: {name}. Output in JSON form."}
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

# Standardize product names
standardized_names = [standardize_product_name(name) for name in product_names]

# Display results
for original, standardized in zip(product_names, standardized_names):
    print(f"Original: {original}")
    print(f"Standardized: {clean(standardized)}\n")

# Sample customer data
customers = [
    "John Doe, 35 years old, buys electronics frequently",
    "Jane Smith, 28 years old, purchases cosmetics monthly",
    "Bob Johnson, 50 years old, buys gardening supplies occasionally"
]

# Function to categorize customer
def categorize_customer(description):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a data categorization assistant."},
            {"role": "user", "content": f"Categorize this customer into one of these categories: Tech Enthusiast, Beauty Lover, Home & Garden. Customer description: {description}. Output in JSON form."}
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

# Categorize customers
categorized_customers = [categorize_customer(customer) for customer in customers]

# Display results
for customer, category in zip(customers, categorized_customers):
    print(f"Customer: {customer}")
    print(f"Category: {category['category']}\n")

# Sample location data
locations = [
    "New York, NY",
    "Los Angeles, California",
    "Chicago, IL, USA",
    "San Fran"
]

# Function to normalize location data
def normalize_location(location):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a location data normalization assistant."},
            {"role": "user", "content": f"Normalize this location: {location}. Output in JSON form with city, state, and country."}
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

# Normalize locations
normalized_locations = [normalize_location(loc) for loc in locations]

# Display results
for original, normalized in zip(locations, normalized_locations):
    print(f"Original: {original}")
    print(f"Normalized: {normalized}\n")
