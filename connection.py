import boto3
from Framework.Creds.CBCredV2 import CBCredV2, Target
from pyspark.sql import SparkSession
import json
import sys


jobapplmonth = sys.argv[1]
numPartitions = int(sys.argv[2])

def generate_message(message):
    m = {'email': message.applicantemail,
         'job_id': message.did}
    return json.dumps(m)


def publish(messages):
    cbcred = CBCredV2()
    credentials = cbcred.get(Target.cred, "aws_rw")
    sns = boto3.client(
        'sns',
        aws_access_key_id=credentials.user.username,
        aws_secret_access_key=credentials.user.password,
        region_name='us-east-1')
    topic_arn = 'arn:aws:sns:us-east-1:276990272009:JobApplication-Backup'
    for message in messages:
        sns.publish(
            TopicArn=topic_arn,
            Subject='Message Published',
            Message=generate_message(message)
        )


spark = SparkSession.builder.appName("Job Applications Refeed").getOrCreate()
spark.sql("SELECT did,applicantemail FROM jobappl.jobapplication WHERE jobapplmonth='{}' AND statuscode = 'Active' AND applicantemail IS NOT NULL AND applicantemail != ''".format(jobapplmonth)) \
     .repartition(numPartitions).rdd \
     .foreachPartition(lambda row: publish(row))