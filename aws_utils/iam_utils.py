import json

from botocore.exceptions import ClientError
from gui.error_display_functions import worker_error_page


def get_s3_role_arn(iam_client):
    policy_arn = None
    role_arn = None
    role_name = 'retail-demand-forecasting-role'
    policy_name = 'retail-demand-forecasting-policy'
    path = '/retail_forecasting-application/'
    try:
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "forecast.amazonaws.com",
                            "glue.amazonaws.com"
                        ]
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Path=path,
            Description='Role for AWS Forecast created by Retail Demand Forecasting Application'
        )
        role = response['Role']
        role_arn = role['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        try:
            response = iam_client.list_roles(
                PathPrefix=path,
            )
            roles_list = response['Roles']
            for role in roles_list:
                if role['RoleName'] == role_name:
                    role_arn = role['Arn']
                    break
        except ClientError as e:
            worker_error_page('Access Denied!', str(e))
    except ClientError as e:
        worker_error_page('Access Denied!', str(e))
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::*"
                    ]
                }
            ]
        }
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy),
            Path=path,
            Description='Policy for AWS Forecast created by Retail Demand Forecasting Application'
        )
        policy = response['Policy']
        policy_arn = policy['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        response = iam_client.list_policies(
            Scope='Local',
            PathPrefix=path,
        )
        policy_list = response['Policies']
        for policy in policy_list:
            if policy['PolicyName'] == policy_name:
                policy_arn = policy['Arn']
                break
    except ClientError as e:
        worker_error_page('Access Denied!', str(e))
    try:
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )
    except ClientError as e:
        worker_error_page('Access Denied!', str(e))
    return role_arn
