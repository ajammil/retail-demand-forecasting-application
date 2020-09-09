# Retail Demand Forecasting Application

![Powered by AWS](https://d0.awsstatic.com/logos/powered-by-aws.png "Powered by AWS")

This application allows retail companies to predict future products demand at different locations using AWS Forecast, using the following data parameters:
- Target
  - Historical sales data
  - Location
- Related
  - Prices
  - Weather
  - Promotions
- Item metadata
  - Color
  - Brand
  - Category

## How to use
- Create a programmatic access key to use for logging in with the specified permissions
- Create a policy using this JSON policy
 ```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "retail-role-creating-policy",
            "Effect": "Allow",
            "Action": [
                "iam:CreatePolicy",
                "iam:ListPolicies",
                "iam:PassRole",
                "iam:ListRoles",
                "iam:CreateRole",
                "iam:AttachRolePolicy"
            ],
            "Resource": "*"
        }
    ]
}
```
- Attach the policy you just created, along with the following AWS Managed policies to the user you just created.
    - AmazonForecastFullAccess
    - AmazonS3FullAccess
    - AmazonAthenaFullAccess
    - AWSGlueConsoleFullAccess
    - AmazonSSMReadOnlyAccess 
- Login to the application using the Access/Secret keys
- Select a region
- Create a dataset group (Dataset groups corresponds to an individual project)
- Click Upload on the target dataset, and select the CSV file that contains your data then click Next, choose a name and then click Create.
- (Optional) repeat the previous step with the related and metadata datasets for better accuracy
- When all of your datasets are in active state, click on Create Forecast
- Select the frequency at which you'd like to generate forecasts, and select the horizon of the forecast. For example, if you want to do weekly forecasts for 20 weeks, select week as the frequency and 20 as the forecast horizon. Please note that:
    - Forecast frequency must be equal to or more than the frequency of the selected datasets.
    - Foracasted duration should not be more 1/3 of the provided history, for example to do forecasts for 1 year in the future. You have to provide at least 3 Years of historical data.
- Click Create, and wait until the model trains, this should take hours several hours depending on the size of your datasets.
- When creation is completed, click view forecasts, and select the forecast you just created.
- Look up your products' predictions!

## How it works
### Components used
#### Amazon Forecast
Amazon Forecast is a fully managed service that uses machine learning to deliver highly accurate forecasts
- Serverless
- Scalable
- Cost Effective
- Easy to use
- Create perdictions in hours instead of months
- Same technology used by Amazon.com
#### Amazon S3
Amazon S3 is an object storage service built to store and retrieve any amount of data from anywhere on the Internet.
- Scalable
- Highly Available (99.99%)
- Highly Durable (11 9's) of durability
- Cost Effective
#### AWS Glue
AWS Glue is a serverless fully managed extract, transform, and load (ETL) service.
- Highly Available
- Scalable
- Cost Effective
#### Amazon Athena
Amazon Athena is a serverless query service that makes it easy to analyze data in Amazon S3 using standard SQL. 
- Highly Available
- Scalable
- Cost Effective
- Pay per query
# Arcitecture
![Architecture](/media/architecture.png)
