# CHICAGO TRAFFIC ACCIDENTS DATA PROJECT
This project is focused on developing business logic on accidents or roadway crashes in the City of Chicago and data has been extracted from the https://data.cityofchicago.org/

Follow this link to get access to all 3 datasets
- https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if/about_data


The focus of this project was to develop Data LakeHouse consisting of an Ingestion Layer, Storage Layer, and Consumption Layer to provide advantages like the following:
- Real Time analytics 
- Eliminate the use of multiple platforms
- Leverage the benefits of datalakes and data warehouses


### Data LakeHouse Architecture 
![image_1774226924838.png](./image_1774226924838.png "image_1774226924838.png")


### Tools
- Databricks Community Edition : Delta Tables and storage was to build lakehouse 
- Apache Spark : Used for data transformation, normalisation and cleaning
- SQL : Build business logic tables for visualization


### Data Pipeline

Automated my implementation of my notebooks with the Job and Pipelines feature of the Databricks 

![image_1774228314059.png](./image_1774228314059.png "image_1774228314059.png")


### Data Visualization

- From the dim_crashes_location data- I applied aggregrated latitude and longitude data to develop a heat map for hot spots of accidents in the city. Locations like River North and the Loop show accident counts of 10k-15k

![image_1774228966504.png](./image_1774228966504.png "image_1774228966504.png")








