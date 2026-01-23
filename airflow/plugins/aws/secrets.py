import boto3

ssm = boto3.client(
        "ssm"
    )

def get_parameter(name):
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    return response["Parameter"].get("Value")