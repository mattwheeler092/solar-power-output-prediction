## What are these files
- `.env` - environmental variables for API keys, GCS, MongoDB authentication
- `user_definition.py` - miscellaneous variables
- `openweather.py` - helper functions for fetching location data from Open Weather
- `visualcrossing.py` - helper functions for fetching historical weather data from Visual Crossing
- `transform_load.py` - read data from GCS into spark DF, some basic processing, join weather data with location data, then load to MongoDB
- `mongodb.py` - helper functions for interacting with MongoDB
- `airflow_dag.py` - yea, the airflow dag file