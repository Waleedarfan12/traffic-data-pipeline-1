# ETL Script for NYC Vehicle Collisions Data
# This script extracts data from a public API, transforms it, and saves it to a CSV
import requests
import pandas as pd
from pandas import json_normalize
import matplotlib.pyplot as plt
import seaborn as sns

# 🚀 Extraction Phase
print("Starting ETL process...")
url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
response = requests.get(url)
data = response.json()
df = pd.DataFrame(data)
df = json_normalize(data)  # Flatten nested JSON structures
print("Data extracted successfully.")

# 🔧 Transformation Phase
df = df.drop_duplicates()
df = df.dropna(subset=["crash_date", "crash_time"])
print("Duplicate and null values removed.")

# Convert date and time columns
df["crash_date"] = pd.to_datetime(df["crash_date"], errors='coerce')
df["crash_time"] = pd.to_datetime(df["crash_time"], format='%H:%M', errors='coerce').dt.time

# Convert numeric columns
df["number_of_persons_injured"] = pd.to_numeric(df["number_of_persons_injured"], errors='coerce')
df["number_of_persons_killed"] = pd.to_numeric(df["number_of_persons_killed"], errors='coerce')
print("Data transformation completed.")

# Summary statistics
total_injured = df['number_of_persons_injured'].sum()
total_killed = df['number_of_persons_killed'].sum()
print(f"Total Injured: {total_injured}, Total Killed: {total_killed}")

# 💾 Loading Phase
df.to_csv("cleaned_nyc_vehicle_collisions.csv", index=False)
print("Data saved to cleaned_nyc_vehicle_collisions.csv")
print("generating visualizations...")
# Visualizations
plt.figure(figsize=(10, 6))
sns.histplot(df['number_of_persons_injured'].dropna(), bins=30, kde=True)
plt.title('Distribution of Number of Persons Injured')
plt.xlabel('Number of Persons Injured')
plt.ylabel('Frequency')
plt.savefig("injured_distribution.png")
plt.close()
plt.figure(figsize=(10, 6))
sns.histplot(df['number_of_persons_killed'].dropna(), bins=30, kde=True, color='red')
plt.title('Distribution of Number of Persons Killed')
plt.xlabel('Number of Persons Killed')
plt.ylabel('Frequency')
plt.savefig("killed_distribution.png")
plt.close()
print("Visualizations saved as injured_distribution.png and killed_distribution.png")
print("ETL process completed successfully.")
