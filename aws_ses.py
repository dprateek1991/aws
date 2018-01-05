#####################################################################################
# Script Name   : ses
# Author        : Prateek Dubey
# Purpose       : Script to send email using AWS SES
# Creation Date : 05-JAN-2018
#####################################################################################

import boto3
from botocore.exceptions import ClientError

SENDER = "prateekdubey12@gmail.com"
RECIPIENT = "prateekdubey12@gmail.com"

AWS_REGION = "us-east-1"

SUBJECT = "EMR Cluster Details"

BODY_TEXT = "Cluster Details"

BODY_HTML = """<html>
<head></head>
<body>
<p><br><br><h1>{code}</h1><br></p>
<table border="1" bordercolor="black" style="width:100%">
  <tr>
    <th>Active Clusters</th>
    <th>Cluster Details</th> 
  </tr>
</table>
</body>
</html>
""".format(code=BODY_TEXT)

CHARSET = "UTF-8"

client = boto3.client('ses', region_name=AWS_REGION, aws_access_key_id='xxxx',aws_secret_access_key='xxxx')

try:
    response = client.send_email(
        Destination={
            'ToAddresses': [
                RECIPIENT,
            ],
        },
        Message={
            'Body': {
				'Html': {
                    'Charset': CHARSET,
                    'Data': BODY_HTML,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY_TEXT,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
        Source=SENDER,
    )
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print("Email sent! Message ID:"),
    print(response['ResponseMetadata']['RequestId'])
