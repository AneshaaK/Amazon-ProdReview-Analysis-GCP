<h1 align="left">Data Analytics Pipeline in Google Cloud Platform <br> for Consumer Reviews of Amazon Products</h1>

This project objective is to build a scalable and efficient data analytics pipeline architecture in Google Cloud Platform on amazon product reviews dataset to achieve the following goals, for End-Users such as Marketing & Sales Department along with Sellers:
- Most reviewed products
- Most rated products
- Popular category based on gender 
- The popularity of free shipping products

### Project Architecture
<img width="1000" alt="image" src="https://user-images.githubusercontent.com/131211098/235416278-838a923b-c8f1-4511-b7f9-af57a8618c9d.png">

### Big Data Life cycle
- Data Ingestion
  - create a pub/sub topic to send notifications whenever a batch file is uploaded into a specific bucket
  - create a pub/sub topic to publish streaming data every 0.5 secs on specific GCS bucket
- Data Preparation - cleaning, filtering and formatting
  - Dataflow subscribes to the topic that publishes the metadata of the batch file
  - Dataflow subscribes to the streaming topic in a fixed time window
- Data Analytics
  - Data stored in Bigquery where we wrote sql queries to create views
- Data Visualization
  - created dashboard using Looker Studio by connecting Bigquery 
 
### Contributors
Aneshaa Kasula, Viritha Vanama and Mohini Patil
