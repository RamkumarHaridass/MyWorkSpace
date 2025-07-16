
import openai
import pandas as pd
import json
from openai import OpenAI
import numpy as np
from pprint import pprint

# Set up OpenAI API key from environment variable
import os
# Set up OpenAI API key from environment variable
import os
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

def clean(dict_variable):
    return next(iter(dict_variable.values()))

# --- Test our API call ---
completion = client.chat.completions.create(
    model="gpt-4o",
    messages=[{
        "role": "user",
        "content": "Write a one-sentence bedtime story about a unicorn."
    }],
)
print(completion.choices[0].message.content)

n = json.loads(completion.choices[0].message.content)
clean(n)

# 2. Example 1: Generating Synthetic Data

prompt = "Generate synthetic sales data for an e-commerce platform. Include fields for date, customer_id (Customer ###), order total (in $USD). For certain orders, the order total should be negative. Create data for 10 customers. Output in JSON form."

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
customer_data = json.loads(response.choices[0].message.content)
print(json.dumps(customer_data, indent=2))

# Convert to DataFrame
df_customers = pd.DataFrame(clean(customer_data))
print(df_customers)

prompt = """Generate 5 synthetic product reviews for a smartphone. Include fields for review_id, rating (1-5), and review_text. Output in JSON form."""

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
review_data = json.loads(response.choices[0].message.content)
print(json.dumps(review_data, indent=2))

# Convert to DataFrame
df_reviews = pd.DataFrame(clean(review_data))
print(df_reviews)

# 2. Example 2: Augmenting Existing Data

# Assuming we have an existing dataset
existing_data = pd.DataFrame({
    'product': ['Laptop', 'Smartphone'],
    'price': [1200, 800],
    'category': ['Electronics', 'Electronics']
})
print("Existing Data:")
print(existing_data)

prompt = f"""Given this product data: {existing_data.to_dict('records')}, 
generate 3 additional products in the same format, maintaining similar patterns but with different values. Output in JSON form."""

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
new_products = clean(json.loads(response.choices[0].message.content))
print("\nGenerated New Products:")
print(json.dumps(new_products, indent=2))

print(pd.DataFrame(data=new_products))

# Original dataset
original_data = [
    {"id": 1, "age": 25, "income": 50000},
    {"id": 2, "age": 40, "income": 75000}
]
print("Original data:")
print(json.dumps(original_data, indent=2))

prompt = f"Generate 3 new data points similar to these, maintaining realistic relationships between age and income. Output in JSON form: {json.dumps(original_data)}"

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
new_data = json.loads(response.choices[0].message.content)
augmented_data = original_data + clean(new_data)
print("Augmented data:")
print(json.dumps(augmented_data, indent=2))

pd.DataFrame(augmented_data).sort_values('age')

# 2. Example 3: Creating Time Series Data

prompt = "Generate synthetic daily sales data for a coffee shop for over 4 weeks, for product. Include date and total_sales. Show a realistic pattern with weekend peaks. Output in JSON form."

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
sales_data = json.loads(response.choices[0].message.content)
print(json.dumps(sales_data, indent=2))

# Convert to DataFrame
df_sales = pd.DataFrame(clean(sales_data))
df_sales['date'] = pd.to_datetime(df_sales['date'])
print(df_sales)

# visualize it
import matplotlib.pyplot as plt

# Plotting the data
plt.figure(figsize=(10, 6))
plt.plot(df_sales['date'], df_sales['total_sales'], marker='o', linestyle='-', color='b')
plt.title('Total Sales Over Time')
plt.xlabel('Date')
plt.ylabel('Total Sales')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 2. Example 4: Generating Edge Cases for a Weather Dataset

# Create a standard weather dataset
weather_data = pd.DataFrame({
    'date': pd.date_range(start='2023-01-01', periods=100),
    'temperature': np.random.uniform(0, 30, 100),
    'precipitation': np.random.uniform(0, 50, 100),
    'wind_speed': np.random.uniform(0, 20, 100)
})
print(weather_data.head())
print(f"\nTemperature range: {weather_data['temperature'].min():.2f} to {weather_data['temperature'].max():.2f}")
print(f"Precipitation range: {weather_data['precipitation'].min():.2f} to {weather_data['precipitation'].max():.2f}")
print(f"Wind speed range: {weather_data['wind_speed'].min():.2f} to {weather_data['wind_speed'].max():.2f}")

# Use Gen AI to generate edge cases for weather data
prompt = "Generate 5 edge cases for a weather dataset with temperature (°C), precipitation (mm), and wind speed (m/s). Include extreme but plausible values. Output in JSON form"

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
edge_cases = json.loads(response.choices[0].message.content)
pprint(edge_cases)

# Add edge cases to the weather dataset
for case in clean(edge_cases):
    new_row = {
        'date': pd.Timestamp.now(),
        'temperature': case['temperature'],
        'precipitation': case['precipitation'],
        'wind_speed': case['wind_speed']
    }
    new_row_df = pd.DataFrame([new_row])
    weather_data = pd.concat([weather_data, new_row_df], ignore_index=True)

print(weather_data.tail())
print(f"\nUpdated temperature range: {weather_data['temperature'].min():.2f} to {weather_data['temperature'].max():.2f}")
print(f"Updated precipitation range: {weather_data['precipitation'].min():.2f} to {weather_data['precipitation'].max():.2f}")
print(f"Updated wind speed range: {weather_data['wind_speed'].min():.2f} to {weather_data['wind_speed'].max():.2f}")

# 2. Example 5: Creating a Sample DataFrame with PII

data = {
    'Name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
    'Email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
    'Phone': ['123-456-7890', '234-567-8901', '345-678-9012'],
    'Age': [30, 25, 45],
    'Salary': [50000, 60000, 75000]
}
df = pd.DataFrame(data)
print(df)

prompt = f"""Mask the following PII data. Keep the Age and Salary as is. Output in JSON form.
Original data:
{df.to_json(orient='records')}
"""

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
masked_data = json.loads(response.choices[0].message.content)
masked_df = pd.DataFrame(clean(masked_data))
print(masked_df)

prompt = f"""Generate synthetic data similar to the following, but with different PII. Keep the Age and Salary distributions similar. Output in JSON form.
Original data:
{df.to_json(orient='records')}
"""

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    response_format={"type": "json_object"}
)
synthetic_data = json.loads(response.choices[0].message.content)
synthetic_df = pd.DataFrame(clean(synthetic_data))
print(synthetic_df)

pii_text = 'My name is Henry and I live in Toronto and I was having trouble accessing my bank account (account ID: 125526). Could you please help me?'

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Remove any PII from the following text, replace it with *: {}".format(pii_text)}]
)
print(response.choices[0].message.content)

# 2. Example 6: Generating Synthetic Samples for an Imbalanced Dataset

# Create an unbalanced dataset of product reviews
positive_reviews = [
    "This product is amazing! I love it!",
    "Great quality and fast shipping.",
    "Exceeded my expectations. Highly recommended!",
    "Best purchase I've made in years.",
    "Fantastic product, will buy again."
]
negative_reviews = [
    "Disappointed with the quality."
]
reviews = positive_reviews + negative_reviews
labels = [1] * len(positive_reviews) + [0] * len(negative_reviews)
df = pd.DataFrame({'review': reviews, 'sentiment': labels})
print(df)
print(f"\nClass distribution:\n{df['sentiment'].value_counts()}")

# Function to generate negative reviews
def generate_negative_review():
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates product reviews."},
            {"role": "user", "content": "Generate a negative product review similar to these negative reviews: {}. Output in JSON form.".format(negative_reviews)}
        ],
        response_format={"type": "json_object"}
    )
    return clean(json.loads(response.choices[0].message.content))

# Generate additional negative reviews
num_to_generate = len(positive_reviews) - len(negative_reviews)
new_negative_reviews = [generate_negative_review() for _ in range(num_to_generate)]

# Add new negative reviews to the dataset
new_data = pd.DataFrame({'review': new_negative_reviews, 'sentiment': [0] * len(new_negative_reviews)})
df_balanced = pd.concat([df, new_data], ignore_index=True)
print(df_balanced)
print(f"\nNew class distribution:\n{df_balanced['sentiment'].value_counts()}")

# Visualize class distribution
plt.figure(figsize=(10, 5))
df_balanced['sentiment'].value_counts().plot(kind='bar')
plt.title('Class Distribution in Balanced Dataset')
plt.xlabel('Sentiment')
plt.ylabel('Count')
plt.xticks([0, 1], ['Negative', 'Positive'])
plt.show()

# Display some generated negative reviews
print("Sample of generated negative reviews:")
print(df_balanced[df_balanced['sentiment'] == 0]['review'].head())

# 3. References and Further Reading
# 1. OpenAI API Documentation: https://platform.openai.com/docs/
# 2. "Synthetic Data for Deep Learning" by Sergey I. Nikolenko
# 3. "The Synthetic Data Vault" by Neha Patki et al.
# 4. "Data Augmentation in Time Series Domain" by Eamonn Keogh and Jessica Lin
# 5. "Generative AI: A Creative New World" by