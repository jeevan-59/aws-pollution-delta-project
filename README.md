# Pollution Delta Project (AWS project)
> AWS Glue 3.0 and later supports the Linux Foundation Delta Lake framework. Delta Lake is an open-source data lake storage framework that helps you perform ACID transactions, scale metadata handling, and unify streaming and batch data processing.

Source data : https://waqi.info OR  https://api.waqi.info

Data flow : Landing --> bronze --> silver --> gold

## AWS Resources
* S3
* Lambda
* Step Functions
* Glue
* CloudFormation
* ECR
* Athena

## On-Premise
* docker

## Prerequisites
* replace all parameters with yours in files - PlltnDeltaProjectTemplate.yaml & states-execution-detail.json 
* create config bucket for storing scripts on top level- states-execution-detail.json , glue-job-bronze-generic-waqi.py, delta-core_2.12-1.0.1.jar
* build locally container-lambda-py-area-sofia , container-lambda-py-init-web-scraping and then push to ECR
* create cloudformation with PlltnDeltaProjectTemplate.yaml
* manual config of Athena service


## Development setup
run cloudformation script - PlltnDeltaProjectTemplate.yaml

## Usage example
run Step Functions - orchestration - Land2Bronze and then check delta files through Athena in bronze layer

## Diagrams
![awsdesign](https://user-images.githubusercontent.com/14351765/232714710-3899d356-500e-4a90-86a8-7bfee5928b88.png)

## TO DO 
* Orchestrating for bronze2Silver and Silver2Gold
* Developing Silver layer (filters,cleans)
* Developing Gold layer (business-level,aggregations)
* Creating CI/CD processes
* Implementing QuickSight
