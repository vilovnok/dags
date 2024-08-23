import pandas as pd

# Define the URL of the CSV file
url = "https://support.staffbase.com/hc/en-us/article_attachments/360009197031/username.csv"

# Use pandas.read_csv() to directly read the CSV file into a DataFrame
df = pd.read_csv(url)

# Specify the destination filename for the locally saved CSV file
destination_filename = "data.csv"

# Save the DataFrame to a new CSV file using to_csv() method
df.to_csv(destination_filename, index=False)

# Print a message indicating a successful download
print("CSV file successfully downloaded and saved as 'data.csv'.")