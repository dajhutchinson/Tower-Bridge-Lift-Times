"""
FETCHER.py

fetches and parses lift times data from https://www.towerbridge.org.uk/lift-times.
maintains a database file containing lift times.
"""

from bs4 import BeautifulSoup
from datetime import datetime
import requests

def request_html() -> str:
    r=requests.get("https://www.towerbridge.org.uk/lift-times")
    return r.text

def extract_rows(html_text) -> list:
    # request html
    soup=BeautifulSoup(html_text,"html.parser")

    # isolate table body
    content_divs=soup.find_all("div",{"class":"view-content"})
    if len(content_divs)!=1:
        raise Exception("Could not find content div.")
    else:
        content_div=content_divs[0]
        table_body=content_div.table

    # seperate row data & drop header(s)
    table_rows=table_body.find_all("tr")
    table_rows=[r for r in table_rows if (len(r.find_all("th"))==0)]

    return table_rows

def parse_row(row) -> dict:

    row_data=row.find_all("td")
    row_text=[d.text for d in row_data]

    if len(row_data)!=5: return None

    day=row_text[0].split("\n",1)[0]

    date_str=row_text[1].split("\n",1)[0]
    time_str=row_text[2].split("\n",1)[0]
    datetime_str="{} {}".format(date_str,time_str)
    date=datetime.strptime(datetime_str,"%d %b %Y %H:%M")

    vessel_name=row_text[3].rstrip()
    direction=row_text[4].rstrip()

    row_dict={
        "date":date,
        "vessel_name":vessel_name,
        "direction":direction
    }

    return row_dict

def parse_rows(rows) -> [dict]:
    row_dicts=[]

    for row in rows:
        row_dict=parse_row(row)
        if row_dict: row_dicts.append(row_dict)

    return row_dicts

def fetch_listed_lifts() -> [dict]:
    # fetch and parse all data currently listed on web

    html=request_html() # fetch webpage
    rows=extract_rows(html) # isolate html for rows
    row_dicts=parse_rows(rows) # parse html

    return row_dicts

if __name__=="__main__":
    # EXAMPLE - fetch available data and print
    row_dicts=fetch_listed_lifts()
    for r in row_dicts:
        print(r)
