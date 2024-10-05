# What is the project about?

The project uses the Xetra dataset containing public dataset on GitHub of the Deutsche Boerse, the German marketplace organizer for trading of shares and other securities. 0.The meta_file acts as an input to the data pipeline.It contains the dates for which the data is read from the source S3 bucket. 1.The project intends to read data from the source files available in the source S3 bucket. 2.Process the data and generate KPI's like min price, max price, daily traded volume, % change from previous closing price etc. 3.The target report is then generated and stored in target S3 bucket as parquet files.

# What does the source data look like?

The source data is available as csv files.The filename contains prefix of the date for which the data is available in the file. The source file contains data as below:

![image](https://github.com/user-attachments/assets/b30c0246-b202-49e5-bd96-c62c99def0d3)

What are topics explored by me in the project?

Reusing the concept of the python project in the xetra_1234 repository, modifying the functions to be modularised and triggered as tasks in a dag, exploring ways to access data in different tasks.

# What are the tools used in the project?

Python 3.9,Github,Visual Studio Code,Docker,Python packages Pandas,boto3,pyyaml,awscli,airflow.

# How to setup the project?

1.Create a source S3 bucket and update the 'src_bucket' tag in the xetra_report1_config.yml file.

2.Create a target S3 bucket and update the 'trg_bucket' tag in the xetra_report1_config.yml file.

3.Create an AWS user with permissions to read/write from/to S3 bucket.

4.Add the AWS_ACCESS_KEY_ID of the user created in the earlier step to the 'access_key' tag in the xetra_report1_config.yml file.

5.Add the AWS_SECRET_ACCESS_KEY of the user created in the earlier step to the 'secret_key' tag in the xetra_report1_config.yml file.

6.Add a postgres connection using the airflow web UI.

7.Open a new terminal in visual studio code and start docker : docker-compose up

8.From the airflow UI trigger the 'Xetra-data-pipeline-1' dag

9.Upon completion a parquet file with extracted data will be available in the target S3 folder provided in step 2.

# Future enhancements to the data pipeline
1.Support of different types of source files.

2.Add logging for each functionality of the data pipeline

3.Add audit information at each stage for torubleshooting

4.Make the solution deployable to AWS cloud.
