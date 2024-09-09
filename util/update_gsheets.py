import json
import os

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

service_account_info = os.getenv('GSHEETS_SERVICE_ACCOUNT')

if service_account_info:
    service_account_info = json.loads(service_account_info)
else:
    raise Exception("Service account credentials not found in environment variables.")

SPREADSHEET_ID = '1fbFxl0tn0zAD8wpCfITfR5w4uBRr9wsu5yIS03FB2z4'
SHEET_NAME = 'player-data'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = Credentials.from_service_account_file(service_account_info, scopes=SCOPES)
client = gspread.authorize(creds)

final_df_pd = pd.read_csv('../data/final/final_df.csv')

sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

sheet.clear()

sheet.update([final_df_pd.columns.values.tolist()] + final_df_pd.values.tolist())

print("Data successfully uploaded to Google Sheets.")
