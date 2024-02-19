# Realtime Streaming with Apache Spark

Data Engineering & visualization of a Smart City

##### Background Use Case
A driver moving from central London to bemingham. He wants to have an IOT device that collects information about: GPS location, Weather info, road accident info e.t.c. All these devices are being gathered by the IOT device & sent to the system. The goal is to analyse the data in realtime & working on it as well in real time. As the driver is moving from point to point the data is being ollected in real time. If there is a delay on the road we can access them in real time. 

Steps
- Data is first collected from the various sources
- This data is streamed through Apache Kafka topics across brokers managed by Zookeeper
- Then a consumer created with Apache Spark will be listening to events coming into Apache Kafka
- This listened data is then consumed & streamed to AWS s3 bucket
- Then a condition is set using AWS glue to extract the data from the raw storage using Data Catalog
- The automated conditions can now be accessed through AWS Athena or Redshift 
- All the AWS processes will be managed using AWS IAM for security purposes
- Finally a visualization dashboard of the data can now be created using visualization tools e.g. Power BI, Tableau, Looker Studio or even AWS Athena

- To watch
https://www.youtube.com/watch?v=egA9wPXSZSQ
https://youtu.be/C42DIddev10

- To download
https://www.youtube.com/watch?v=e1w7R1hEvCs&t=9s

ruok - "R U OK"

### Packages to install
- pip install confluent_kafka
- pip install simplejson
- pip install pyspark
pip freeze > requirements.txt [Get all installed required packages]