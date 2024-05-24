FinalProject_DAP
This project is to visualize and analayse the factors that led to traffic collision crashes in the US(Monroe County NY and Maryland)

Workflow

Fetch the traffic data JSON or CSV data from the file source(URL request)
View and Explore the characteristics of the dataset
Save the data to MongoDB
Implement the ETL pipeline using Luigi or Apache Airflow
The Resultant clean data is then saved to PostgreSQL database
Use Jupyter Notebook to visualize the cleaned data and draw conclusion and insights from the data.
Tools Used:

Python
Jupyter Notebook
Docker
MongoDB
PostgreSQL
Luigi
Apache Airflow
Dataset URL:

https://catalog.data.gov/dataset/crash-reporting-non-motorists-data [ https://data.montgomerycountymd.gov/api/views/n7fk-dce5/rows.json?accessType=DOWNLOAD ] - JSON
https://catalog.data.gov/dataset/crash-reporting-drivers-data [ https://data.montgomerycountymd.gov/api/views/mmzv-x632/rows.json?accessType=DOWNLOAD ] - JSON
https://data.world/city-of-bloomington/117733fb-31cb-480a-8b30-fbf425a690cd [crash-data-monroe-county-2019-3.csv] - CSV
