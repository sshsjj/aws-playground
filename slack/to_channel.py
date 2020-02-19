import os
import logging
import json
# We use urllib2 instead of requests just to make the demo easier
# In a production environment, consider using the requests library
import requests
import time

current_milli_time = lambda: int(round(time.time() * 1000))
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)
env = "stg"
dt = "2019-01-01"

slack_url = "YOUR_SLACK_WEBHOOK"


def sns_alert(dt, env, slack_url, job_status="Succeeded"):
    failed_reason = "Hard Bounces Generation Failed on Partition{}".format(dt)
    attachment_text = "Reason: {}".format(failed_reason) if job_status == "Failed" else ""
    aws_glue_link = "https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#"

    job_status_color_switcher = {
        "Succeeded": "#14FF14",
        "Failed": "#B22222"
    }

    priority_switcher = {
        "prd": "High",
        "stg": "Medium",
        "tst": "Low"
    }
    optional_fields = {
        "title": "Priority",
        "value": priority_switcher.get(env, "N/A"),
        "short": False
    }
    status_icon = {
        "Succeeded": ":white_check_mark:",
        "Failed": ":x:"
    }

    postData = {
        "channel": "#de-monthly-wf-tst",
        "username": "Alert",
        "text": ":{} *Error occured in {}*".format(status_icon.get(job_status, ":grey_question:"), env),
        "attachments": [{
            "color": job_status_color_switcher.get(job_status, "#FFFFFF"),
            "region": "us-west-2",
            "author_name": "DE Monthly Workflow",
            "title": "Glue Job(rv-pdab) {}".format(job_status),
            "title_link": aws_glue_link,
            "fields": [
                optional_fields if job_status == "Failed" else {}
            ],
            "text": attachment_text,
            "footer": "AWS Glue",
            "footer_icon": "https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2020/01/data-lake-glue.png",
            "ts": current_milli_time()
        }],
        "icon_emoji": ":biohazard_sign:"
    }

    r = requests.post(slack_url, data=json.dumps(postData))
    print(r.status_code)
    LOGGER.info('Message posted')


# sns_alert(dt, env, slack_url, job_status="Failed")
sns_alert(dt, env, slack_url)

