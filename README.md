# MapReduce and Apache Spark
In this we are comparing MapReduce and Spark performance by running several tasks simultaneously and measuring their job execution time. We are using Amazon EMR cluster with Hive and PIG as our Hadoop cluster. In order to measure the execution time, we deploy Hive scripts because hive use MapReduce. First, upload all the script and input files to S3 bucket. Then go to EMR cluster.

Following is my EMR cluster details 
![EMR_Cluster](EMR_Cluster_Configuration_details.png)

Then go to the step menu:
![Step_Menu](Step_Menu_of_EMR.png)

Then use following method to add steps
## Hive
![Hive_Step](HiveQL/Screen_Shots/2-HQL_add_step.png)

## Spark
![Spark_Step](Spark/Screen_Shots/2-Spark_add_step.png)

# Dataset
Detail descrition of the dataset can be found from [here](https://www.kaggle.com/code/adveros/flight-delay-eda-exploratory-data-analysis/notebook).


