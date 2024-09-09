from datetime import datetime

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import numpy as np

# Define the path to the service account credentials JSON file (created dynamically in the workflow)
SERVICE_ACCOUNT_FILE = '../player-data-project-467fdd605960.json'

# Define Google Sheets details
SPREADSHEET_ID = '1fbFxl0tn0zAD8wpCfITfR5w4uBRr9wsu5yIS03FB2z4'  # Replace with your actual spreadsheet ID
SHEET_NAME = 'player-data'  # Sheet/tab name where you want to upload the data

# Define the scope for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Authenticate with Google Sheets API
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# Convert the final DataFrame to Pandas
final_df_pd = pd.read_csv('../data/final/final_df.csv')

# Open the Google Sheet and select the worksheet
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# Clear the existing data in the sheet
sheet.clear()

# Update the sheet with the new data (including headers)
sheet.update([final_df_pd.columns.values.tolist()] + final_df_pd.values.tolist())

print("Data successfully uploaded to Google Sheets.")
