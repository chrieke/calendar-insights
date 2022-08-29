import os.path
from typing import List
from datetime import datetime

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pandas as pd


def authenticate_google(scopes: list, oauth2_client_secret_file=None, service_acc_credentials_file=None):
    if service_acc_credentials_file:
        credentials = service_account.Credentials.from_service_account_file(
            'credentials.json')
        creds = credentials.with_scopes(scopes)

    if oauth2_client_secret_file:
        # token.json stores the user's access and refresh tokens.
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(oauth2_client_secret_file, scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

    service = build('calendar', 'v3', credentials=creds)
    return service


def query_google_calendar_api(service, emails: List[str], start_time: datetime, end_time: datetime) -> pd.DataFrame:
    events = []
    for email in emails:
        more_pages = True
        next_page_token=None

        while more_pages:
            query = service.events().list(
               calendarId=email, 
               timeMin=start_time.isoformat() + 'Z',
               timeMax=end_time.isoformat() + 'Z',
               maxResults=1000, 
               singleEvents=True,
               orderBy='startTime',
               pageToken=next_page_token)

            page_results = query.execute()

            page_events = page_results.get('items', [])
            for item in page_events:
                item = item.update({"queried_from": email})
            events.extend(page_events)

            next_page_token = page_results.get('nextPageToken')
            if next_page_token is None:
                break

    df = pd.DataFrame(events)
    return df

