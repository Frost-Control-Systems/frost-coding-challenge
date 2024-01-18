"""
Thanks for taking the time to work on our coding challenge. We're excited to see what you come up with!

We've put together a sample of code that we'd like you to refactor. The code is meant to be a simplified version of a 
real-world problem we've encountered. 

A few guidelines before you get started:
- Spend between 30 and 45 minutes on this. This question is designed to be exhaustive, and we don't expect it to be 
  fully refactored when you finish. 
- You will be assessed more on your approach than speed. Focus on what will make the code more intelligible and 
  maintainable for the next engineer that works on it.
- As you can see, there is a mix of runnable and pseudo code here. Don't worry about making a solution that is runnable,
  especially if you're not super familiar with Python.
- We appreciate small, incremental commits that show your thought process.
- If you make an assumption about any code that isn't shown, just document it.

"""


from datetime import datetime
from typing import Dict, List

import pyodbc
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel as PydanticModel

cursor = pyodbc.connect("...").cursor()

GET_TRANSMISSIONS = ""  # hypothetical SQL query
GET_FORECASTS = ""  # hypothetical SQL query
GET_LOCATIONS = ""  # hypothetical SQL query
GET_MOST_RECENT_TRANSMISSION_FOR_EACH_ACTIVE_LOCATION = ""  # hypothetical SQL query
GET_FORECASTS = ""  # hypothetical SQL query


class Location(PydanticModel):
    ...


class Transmission(PydanticModel):
    ...


class Forecast(PydanticModel):
    ...


def get_transmissions(params: Dict) -> List[Transmission]:
    return cursor.execute(GET_TRANSMISSIONS, params).bind(Transmission)


def get_forecasts(params: List[str]) -> List[Forecast]:
    return cursor.execute(
        GET_FORECASTS,
        {f"Location{i}": locationID for i, locationID in enumerate(params)},
    ).bind(Forecast)


def get_locations(params: str) -> List[Location]:
    return cursor.execute(GET_LOCATIONS, {"CustomerID": params}).bind(Location)


def get_most_recent_transmission(location: Location) -> Transmission:
    location.MostRecentTransmission = cursor.execute(
        GET_MOST_RECENT_TRANSMISSION_FOR_EACH_ACTIVE_LOCATION,
        {f"LocationID": location.ID},
    ).bind(Transmission)


def get_most_recent_forecasts(params: str) -> List[Forecast]:
    return sorted(cursor.execute(GET_FORECASTS, params).bind(Forecast), "DateTimeUTC")[
        -1
    ]


def main(customer_id):
    locations = get_locations(customer_id)
    for location in locations:
        get_most_recent_transmission(location)

    most_recent_forecasts = get_forecasts(locations)

    locations_forecasts = {}
    for forecast in most_recent_forecasts:
        if forecast.location_id not in locations_forecasts:
            locations_forecasts[forecast.location_id] = []
        else:
            locations_forecasts[forecast.location_id].append(
                Forecast(**forecast.__dict__)
            )

    for key in locations_forecasts.keys():
        locations_forecasts[key].sort("DateTimeUTC")
        locations_idx = [location.ID for location in locations].index(key)
        locations[locations_idx].Forecasts = locations_forecasts[key]
        locations[locations_idx].MostRecentForecasts = get_most_recent_forecasts(
            locations[locations_idx].ID
        )

    transmissions = [
        get_transmissions(
            {
                "LocationID": location.ID,
                "Start": datetime.utcnow() - relativedelta(days=2),
                "End": datetime.utcnow(),
            }
        )
        for location in locations
    ]
    location_id_transmissions = {}
    for transmissions_list in transmissions:
        location_id_transmissions[transmissions_list[0].location_id] = sorted(
            transmissions_list, "DateTimeUTC"
        )
    for id, transmissions in location_id_transmissions.items():
        idx = [location.ID for location in locations].index(id)
        locations[locations[idx]].Transmissions = transmissions
    return locations
