import boto3
import json

#1. Make a Lambda function and use to_channel.py logic
#2. Subscribe the Lambda to the topic(target_arn) in AWS SNS
#3. Change message to match with the input
target_arn = "arn:aws:sns:region:account_id:sns-topic"
env = "tst"
def sns_push(arn, env, status="success"):
    state_switcher = {
        "success": "OK",
        "failure": "Alarm"
    }
    description_switcher = {
        "success": "Succeeded",
        "failure": "Failed"
    }

    message = {"Trigger": {"MetricName": "a metric"}, "NewStateReason": "Workflow {}".format(status),
               "AlarmName": "DEMonthly",
               "AlarmDescription": "Workflow {}: https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#".format(
                   description_switcher.get(status, "Unknown")),
               "Region": "US West (Oregon)", "NewStateValue": state_switcher.get(status, "N/A")}
    client = boto3.client("sns")
    response = client.publish(
        Subject="DE Monthly Job Status",
        TargetArn=arn,
        Message=json.dumps({"default": json.dumps(message)}),
        MessageStructure="json"
    )
    print(response)

sns_push(target_arn, env, status="failure")
