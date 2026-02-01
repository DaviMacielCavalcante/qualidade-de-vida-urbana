import re 
from datetime import datetime as dt

def extract_most_recent_date(file):
    match = re.search(r"_A_(\d{2}-\d{2}-\d{4})\.CSV", file)
    data_obj = dt.strptime(match.group(1), "%d-%m-%Y")
    return data_obj

def most_recent(files):
    most_recent_file = max(files, key=extract_most_recent_date)
    return most_recent_file