
## Generate Report from Data using AWS Cli + S3 + Athena 

### Arch:
![Arch](https://github.com/AODBA/blg/blob/master/blg.PNG)
#### Note:
- you need to have an aws account and need to setup a profile on your local aws cli 

### Setup:
- clone the repo to you box.

```
cd /opt/
git clone https://github.com/AODBA/blg.git
```


### 1 - We start by setting some variables, this values will be used in the scripts to follow
```
export today=`date +%F`
export crawler_name='my-blg-crawler'
export aws_profile_name='personal'
export aws_region_name='ap-southeast-2'
export my_bucket_name='my-bkt-blg'
```

### 2 - Create an s3 bucket
```
aws s3 mb s3://$my_bucket_name --profile $aws_profile_name --region $aws_region_name
```

### 3 - Edit the policy docs to reflect you bucket name
```
cp -r /opt/blg/code/policy /opt/blg/code/policy_$today
cd /opt/blg/code/policy_$today
sed -i s/REPLACE_BKT_NAME/$my_bucket_name/g *.json
```

### 4 - Create an IAM policy
```
aws iam create-policy --policy-name $crawler_name-policy --policy-document file:///opt/blg/code/policy_$today/policy-document.json --profile $aws_profile_name --region $aws_region_name
```
### 5 - Create an IAM Role
```
aws iam create-role --role-name $crawler_name-role --assume-role-policy-document file:///opt/blg/code/policy_$today/role-policy.json --profile $aws_profile_name --region $aws_region_name
```
### 6 Attach Role Policy
```
export arn=$(aws iam list-policies --query 'Policies[?PolicyName==`'$crawler_name-policy'`].Arn' --profile $aws_profile_name --region $aws_region_name --output text)
aws iam attach-role-policy --role-name $crawler_name-role --policy-arn $arn --profile $aws_profile_name --region $aws_region_name
export service_arn=$(aws iam list-policies --query 'Policies[?PolicyName==`AWSGlueServiceRole`].Arn' --profile $aws_profile_name --region $aws_region_name --output text)
aws iam attach-role-policy --role-name $crawler_name-role --policy-arn $service_arn --profile $aws_profile_name --region $aws_region_name
```

### 7 Create AWS Glue Crawler
```
aws glue create-crawler --name $crawler_name --role $crawler_name-role --database-name $crawler_name-db --targets '{"S3Targets": [{"Path": "s3://'$my_bucket_name'/","Exclusions": []}],"JdbcTargets": [],"DynamoDBTargets": [],"CatalogTargets": []}'  --description 'my '$crawler_name' desc' --table-prefix tbl_prefix__ --profile $aws_profile_name --region $aws_region_name
```

### 8 Download / Upload data to S3
```
 wget https://data.melbourne.vic.gov.au/api/views/b2ak-trbp/rows.csv?accessType=DOWNLOAD -O PedestrianCountingSystem-2009toPresent.csv && aws s3 cp PedestrianCountingSystem-2009toPresent.csv s3://$my_bucket_name/History/ --profile $aws_profile_name --region $aws_region_name
wget https://data.melbourne.vic.gov.au/api/views/h57g-5234/rows.csv?accessType=DOWNLOAD -O PedestrianCountingSystem-SensorLocations.csv && aws s3 cp PedestrianCountingSystem-SensorLocations.csv s3://$my_bucket_name/SensorLocations/ --profile $aws_profile_name --region $aws_region_name
```


### 9 - Run AWS Glue Crawler 
```
aws glue start-crawler --name $crawler_name --output json --profile $aws_profile_name --region $aws_region_name
```

- get crawl status 
```
aws glue get-crawler --name $crawler_name --profile $aws_profile_name --region $aws_region_name
```

### 10 - Run Athena Query Top 10 by Day 
```
export top10day=`aws athena start-query-execution --query-string '
SELECT sum(a.hourly_counts) AS Day_Count,
         a.sensor_id,
         b.sensor_name,
         b.sensor_description,
         a.year,
         a.month,
         a.mdate
FROM "'$crawler_name'-db"."tbl_prefix_history" a
JOIN "'$crawler_name'-db"."tbl_prefix_sensorlocations" b
    ON a.sensor_id = b.sensor_id
GROUP BY  a.sensor_id, b.sensor_name, b.sensor_description,a.year,a.month,a.mdate
ORDER BY  Day_Count DESC limit 10;
' --result-configuration OutputLocation='s3://athena-output-ao/top10-day/' --profile $aws_profile_name --region $aws_region_name --output text`
sleep 10
# Download the ResultSet from S3
aws s3 cp s3://athena-output-ao/top10-day/$top10day.csv top10day.csv  --profile $aws_profile_name --region $aws_region_name
# View the Result
cat top10day.csv
```

### 10 - Run Athena Query Top 10 by Month
```
export top10month=`aws athena start-query-execution --query-string '
SELECT sum(a.hourly_counts) AS Month_Count,
         a.sensor_id,
         b.sensor_name,
         b.sensor_description,
         a.year,
         a.month
FROM "'$crawler_name'-db"."tbl_prefix_history" a
JOIN "'$crawler_name'-db"."tbl_prefix_sensorlocations" b
    ON a.sensor_id = b.sensor_id
GROUP BY  a.sensor_id, b.sensor_name, b.sensor_description,a.year,a.month
ORDER BY  Month_Count DESC limit 10;
' --result-configuration OutputLocation='s3://athena-output-ao/top10-month/' --profile $aws_profile_name --region $aws_region_name --output text`
sleep 10
aws s3 cp s3://athena-output-ao/top10-month/$top10month.csv top10month.csv --profile $aws_profile_name --region $aws_region_name

cat top10month.csv

```

