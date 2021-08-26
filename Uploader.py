"""
UPLOADER.py

mangages google calendar and twitter account.
"""
import Manager as M

import datetime, os.path, pytz
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

"""
ACCESS GOOGLE CALENDAR API
"""
def connect_to_gcal():
    # access required
    SCOPES = ['https://www.googleapis.com/auth/calendar.events',"https://www.googleapis.com/auth/calendar.readonly"]
    creds=None

    # credential file exists
    if os.path.exists("token.json"):
        creds=Credentials.from_authorized_user_file("token.json",SCOPES)

    # no valid credentials (may open browser to confirm scopes)
    if (not creds) or (not creds.valid):

        if creds and creds.expired and creds.refresh_token: # refresh credentials
            creds.refresh(Request())
        else: # new credentials
            flow=InstalledAppFlow.from_client_secrets_file("credentials.json",SCOPES)
            creds=flow.run_local_server(port=0)

        # save credentials
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # build api service
    service = build('calendar', 'v3', credentials=creds)

    return service

"""
MAINTAIN CALENDAR
"""
def prepare_calendar_event(details) -> dict:
    local_timezone=pytz.timezone("Europe/London")
    local_dt=local_timezone.localize(details["date"]).astimezone(pytz.utc)
    start_date_str=local_dt.strftime("%Y-%m-%dT%H:%M:00Z")

    end_date_str=(local_dt+datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:00Z")
    event={
        "summary":"Tower Bridge Lift",
        "location":"Tower Bridge Rd, London SE1 2UP",
        "description":"Tower Bridge will lift to allow {} to travel {}.".format(details["vessel_name"],details["direction"]),
        "start":{
            "dateTime":start_date_str,
            "timeZone":"Europe/London"
        },
        "end":{
            "dateTime":end_date_str,
            "timeZone":"Europe/London"
        },
    }
    return event

def get_event_list(calendar_id,service) ->[dict]:
    event_list=service.events().list(calendarId=calendar_id).execute()["items"]
    return event_list

def parse_google_event(event_dict) -> dict:
    # parse event from `get_event_list`
    event={
        "start_dateTime":event_dict["start"]["dateTime"],
        "description":event_dict["description"]
    }

    return event

def add_event(event_dict:dict,calendar_id:str,service) -> str:
    return service.events().insert(calendarId=calendar_id, body=event_dict).execute()

def add_events(new_events:[dict],calendar_id,service,existing_events=None):

    counter=0
    for (i,new_e) in enumerate(new_events):
        print("{}/{} (new={})".format(i,len(new_events),counter),end="\r")
        new_e_dict=prepare_calendar_event(new_e)
        # check if event already exists
        if existing_events is not None:

            already_exist=False
            for e in existing_events:
                if (e["start_dateTime"]==new_e_dict["start"]["dateTime"]) and (e["description"]==new_e_dict["description"]):
                    already_exist=True
                    break

            if not already_exist:
                counter+=1
                add_event(new_e_dict,calendar_id,service)

        print("{}/{} (new={})".format(i,len(new_events),counter),end="\r")

"""
MAIN
"""
def update_calendar():
    # update calendar with events recored locally

    service=connect_to_gcal()

    # get list of calendars
    calendar_list=service.calendarList().list(pageToken=None).execute()["items"]

    # find calendar ID
    calendar_id=None
    for c in calendar_list:
        if c["summary"]=="Tower Bridge Lift Times":
            calendar_id=c["id"]
            break

    if calendar_id is None:
        raise Exception("Calendar cannot be found.")
        return None

    # read events from db
    lift_data=M.load_data("lift_data.csv")
    lift_data=list(lift_data.T.to_dict().values()) # list of dicts

    existing_events=get_event_list(calendar_id,service)
    existing_events=[parse_google_event(e) for e in existing_events]

    add_events(lift_data,calendar_id,service,existing_events)

if __name__=="__main__":
    update_calendar()
