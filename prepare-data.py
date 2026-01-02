import pandas as pd

# Read Excel file
df = pd.read_excel(
    "data/input/input_data.xls",
    header=1  # skip the descriptive row
)

# Rename label column
df = df.rename(columns={"default payment next month": "label"})

# Add event_date for Spark partitioning
df["event_date"] = "2024-01-01"

# Save as CSV
df.to_csv("data/input/input_data_clean.csv", index=False)

print("Conversion complete: input_data_clean.csv created")
