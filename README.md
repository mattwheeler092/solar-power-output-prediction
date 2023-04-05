# solar-power-output-prediction

# Predicting solar radiation using weather data


## Motivation
Often when people are considering purchasing and installing solar panels, the potential cost benefits can be ambiguous. Someone living in sunny Los Angeles will see more energy savings than someone living in cloudy Seattle. The question that we set out to answer using machine learning was: can we get accurate energy saving estimates for specific locations across California? 


## Dataset and analytic goals
Our intuition was that the leading factors causing variation in solar panel power output would be weather related features. As a result, we used the Visual Crossing API to gather hourly weather data during 2022 for 450 locations within California. The API provided 59 weather related features such as temperature, precipitation, cloud cover, wind speed, etc.


Unfortunately, we were not able to find a usable dataset that provided solar panel power outputs for different locations across California. To overcome this limitation, we chose to use solar irradiance as the target variable instead. Solar irradiance, measured in W/m^2, is the measure of the incident solar energy per unit area, per second. This metric maps nearly linearly to solar panel power output using the following equation,

![My image](images.power_formula.png)

Therefore, by building a machine learning model that can predict solar irradiance, we can directly compute the solar panel power output estimations. Fortunately, the Visual Crossing API also provided solar irradiance as one of its features, which we collected and stored alongside the weather related features.


Our final dataset consisted of 3,941,550 observations, each of which had values for 59 weather features and the target solar irradiance feature. The main analytics goal was to see if this data could be used to accurately predict solar radiation. 


 Overview of Data Engineering Pipeline
The first step of the data engineering pipeline was to determine which locations within California to collect weather data for. We used the US Cities dataset from Kaggle to find the latitude, longitude, and population of the 1200 towns and cities within California. 


Given API limitations, we were restricted to collecting weather information for only 450 locations. We used a variation of the K-Means++ initialisation algorithm to ensure the selected locations weren’t closely packed as the majority of towns / cities were located near San Francisco and Los Angeles. The below graph shows the 450 locations we chose.




The data engineering pipeline involved fetching historical weather data from the Visual Crossing API and storing the results in a Google Cloud Storage bucket, ready for further processing. Next, the raw data was transformed using Pyspark, which included dropping unimportant columns, checking/cleaning the datatype of the data, etc. Finally, the PySpark processed data was loaded into MongoDB Atlas for downstream consumption. 


The volume of data we needed to collect was too large to collect in a single run of the pipeline. As a result, we used Apache Airflow to orchestrate and run separate pipeline runs. We defined a single batch of data to be one month of historical weather data of one latitude / longitude location. We set up the airflow pipeline to collect, process, and store 100 batches of data every 15 minutes. This involved creating two caching mechanisms, the first kept track of the last month of data that had been collected and stored in GCP for each location; once the last month equalled 2022-12, the cache would mark that location as complete and the pipeline would move onto the next location. The second cache would keep track of all GCP files that still needed to be processed by PySpark and stored in the final MongoDB database.


In total there were 5,400 batches of data to collect, process, and store. This was done over 54 separate Airflow jobs that took a total of 13.5 hours. The final database contained over 5.4G of weather data.


 Preprocessing
Cluster Specifications
Time efficiency: 7 minutes per batch (54 batches total)
Total processing time: 6 hours
DataBricks Runtime Version – 7.3 LTS (includes Apache Spark 3.0.1, Scala 2.12)
Worker type: i3.xlarge
Driver type: i3.xlarge
Number of workers: 2-5


EDA
During the EDA and data cleaning process, redundant columns such as ‘feels like’ (redundant with temperature) were removed, as well as columns with little variation (‘snow depth’, for example, consisted of primarily zeros).


Feature Selection and Engineering
PCA: We performed PCA analysis on the latitude / longitude values associated with the 450 locations selected from the US cities dataset. The intuition behind performing a PCA transformation of latitude and longitude was due to a hypothesis that the main directions of variation in the data were distance from the coast, and how far north or south along the coast. After performing PCA on latitude and longitude, two new principal component directions for latitude and longitude were revealed, with the first direction explaining 93% of variance and the second direction explaining 7% of variance. With this information, the new directions were appended as columns to the US Cities data as PC1 and PC2. Then, this new dataframe was joined to the data from the weather API on the column latitude (longitude could have also been used, the choice was arbitrary). 


Date Embeddings: In order for the model to be able to use the observation date as a feature, we needed to numerically quantify it. We chose to do this by encoding the date into a unit circle, where the embedded coordinates were (cos(t), sin(t)), where t is the embedding angle. The diagram below shows how the embedding angle is simply the proportion through the year. By doing this we are able to separate out summer and winter observations.

Time Embeddings: Similar to the date embeddings, we wanted to model to be able to use the time of day as one of its features. We used the same embedding method, except we set the embedding angle to be the fraction of the day that had already passed.

Feature time_sunrise_sunset: To further help the model better utilize the observation time function we combined it with the days sunset and sunrise times to create a new feature called ‘time_sunrise_sunset’.  If the observation time is before the sunrise or after the sunset, the feature value is set to zero. Between sunrise and sunset, the feature value linearly increases to a value of one half way between the two times, before decreasing back to zero. This process is illustrated in the diagram below.





 ML Goals,  Outcomes, and Execution Time Efficiency
The primary ML goal was to fit a supervised learning model using various weather features to predict solar radiation. The features used in the final model were temperature, humidity, dew, precipitation, wind gust, wind speed, pressure, visibility, cloud cover percentage, latitude, longitude, datetime (embedded), time from sunrise or sunset, and 2 principal components of latitude and longitude.


Modeling
A Random Forest Regressor was chosen due to the continuous nature of the feature variables.
After fitting the model on the full data, which had an execution time of 58.7 minutes, metrics such as RMSE and R-squared were calculated. 


Model evaluation
a. RMSE: 67.7854
Given that the target variable (solar radiation) ranges from 0 to 1200 W/m2 , a RMSE of 67.7854 means that on average, the difference between the predicted and actual values is 68. Since the range is 1200, this RMSE is considered “good”, following the rule of thumb that an accurate model will have a RMSE that is 10% of the range of the y values or smaller. At 5.6%, this means that the model performs quite well.


b. R2: 0.9465
An R-squared value of 0.9465 indicates that approximately 95% of the variation in the predicted variable (solar radiation) can be explained by the independent variables (weather features) in the regression model. In other words, the model is able to capture a large portion of the variability in the data and is likely a good fit for the data.


Feature Importance
From the figure, we can see that the feature “time_sunset_sunrise”, which was added during the feature engineering process, had the highest importance. In a random forest model, feature importance is measured by the reduction in prediction error when the feature is included in the model. The higher the importance score, the more predictive power the feature has in the model.



Applications of the model

A simple web application (Demo link) was developed to serve the machine learning model. The frontend was built using ReactJS, providing a user-friendly interface that enables users to select a location. Upon selecting a location, an API request is sent to the Visual Crossing API to fetch the current weather conditions, such as precipitation and humidity, which are then displayed on the frontend. Based on the selected location, the model, which is deployed on a web server using FastAPI, will predict the annual electricity bill saving. 

At the time of writing this report, the application has limited functionalities and the model’s predictions are based on various assumptions on household energy consumption and energy cost, among other things. Additional functionalities could be developed in future work to enhance this application:
Automatically select the city that is closest to the user based on IP address
Allow users to specify the number of solar panels they have and their energy efficiency
Allow users to input their annual energy consumption
Allow users to input their energy cost or have the app directly fetch energy cost data from local energy providers
Optimize UI design for different devices


 Lessons Learned
One lesson learned was the importance of feature selection and feature engineering. The engineered features greatly improved the performance of the model, with the top 2 most important features in the model being a result of the feature engineering stage of the project.


A challenge was faced with the initial API source during the early stage of the project, which was the Open Weather API. The API imposed a daily limit of only 50,000 API calls. Given the time constraints of the project, this would not be enough to collect the required amount of data. As a result, we had to search for an alternative API and eventually found the Visual Crossing API. Not only did it provide richer data but also did not have any API call limits. With this API, we were able to collect all 3M+ data points in just 2 days, which significantly accelerated the project timeline.


 Conclusion
Our group set out to explore whether solar radiation could be predicted using widely available weather data. The dataset contained hourly weather data for 450 locations in California during the year 2022, including solar irradiance, which was used as the target variable. 


The data engineering pipeline involved selecting the locations using a variation of the K-Means++ initialization algorithm, fetching historical weather data from the Visual Crossing API, transforming the raw data using PySpark, and loading the processed data into MongoDB Atlas. The primary modeling goal was to fit a Random Forest model to predict solar radiation. The model had a RMSE of 67.78, which is low relative to the range of y values, which was 1200 W/m2 . 


With the weather data, we were able to predict the solar radiation. From there, solar radiation was converted to solar output using the dimensions and material of a typical solar panel. However, converting this information to solar output depends on the type, size, and material of the solar panel itself. A possible application of this work could be comparing different solar panels to see which yields the highest solar output. With more information about the panels themselves, we can calculate the conversion efficiency of each type of solar panel to get the most accurate solar output prediction. 




