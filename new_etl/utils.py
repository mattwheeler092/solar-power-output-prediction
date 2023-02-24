import calendar

from datetime import datetime, timedelta
from user_definision import DATE_FORMAT, WEATHER_API_KEY


def increment_date(date, num_days):
    """ Function to increase """
    date = datetime.strptime(date, DATE_FORMAT)
    incremented_date = date + timedelta(days=num_days)
    return incremented_date.strftime(DATE_FORMAT)

def month_end_date(date):
    """ """
    date = datetime.strptime(date, DATE_FORMAT)
    # Define year / month / day for month end 
    year = date.year
    month = date.month
    day = calendar.monthrange(year, month)[1]
    # Return month end date in str format
    return datetime(year, month, day).strftime(DATE_FORMAT)

# Define the structure of the visual crossing API query
def generate_api_query(lat, lon, start, end):
    """ Function to generate the api query for a specific 
        lat / lon position and a specific start / end 
        date period. NOTE: start and end are date strings 
        that follow the format "%Y-%m-%d" """
    return f"""https://weather.visualcrossing.com/VisualCrossingWebServices
/rest/services/timeline/{lat}%2C%20{lon}/{start}/{end}?unitGroup=metric&
key={WEATHER_API_KEY}&contentType=json"""


