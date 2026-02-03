import os
import boto3


def _get_uc_session(credential_name: str) -> boto3.Session:
    return boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            credential_name
        )
    )


# Gets the requested token stored in AWS SSM.
def get_ssm_parameter(ssm_client: boto3.Session, name: str) -> str:
    res = ssm_client.get_parameter(Name=name, WithDecryption=True)
    return res["Parameter"]["Value"]


def get_ssm_client(
    credential_name: str, region: str = None, use_region: bool = True
) -> boto3.client:
    """
    Creates and returns an AWS SSM client. If Unity Catalog is enabled, uses dbutils credentials provider
    with the given credential name. Otherwise creates a standard boto3 client.

    Args:
        credential_name (str): Name of the credential in the CloudCredentialRegistry that grants access to the desired SSM parameter.
        region (str, optional): Name of the AWS region to use. If None, uses the region of the current session. Defaults to None.
        use_region (bool, optional): Whether to specify the region when creating the client. Defaults to True.
    """
    # Checks whether UC is enabled by checking the DATA_SECURITY_MODE.
    if os.environ.get("DATA_SECURITY_MODE") in ["SINGLE_USER", "USER_ISOLATION"]:
        boto_session = _get_uc_session(credential_name)
        if use_region:
            region_name = region if region else boto_session.region_name
            return boto_session.client("ssm", region_name=region_name)
        return boto_session.client("ssm")

    if use_region:
        region_name = region if region else boto3.session.Session().region_name
        return boto3.client("ssm", region_name=region_name)
    return boto3.client("ssm")


def get_s3_client(credential_name: str) -> boto3.client:
    if os.environ.get("DATA_SECURITY_MODE") in ["SINGLE_USER", "USER_ISOLATION"]:
        return _get_uc_session(credential_name).client("s3")
    return boto3.client("s3")


def get_s3_resource(credential_name: str) -> boto3.resource:
    if os.environ.get("DATA_SECURITY_MODE") in ["SINGLE_USER", "USER_ISOLATION"]:
        return _get_uc_session(credential_name).resource("s3")
    return boto3.resource("s3")


def get_sqs_client(credential_name: str) -> boto3.client:
    if os.environ.get("DATA_SECURITY_MODE") in ["SINGLE_USER", "USER_ISOLATION"]:
        boto_session = _get_uc_session(credential_name)
        return boto_session.client("sqs", region_name=boto_session.region_name)
    session = boto3.session.Session()
    return session.client("sqs", region_name=session.region_name)


def get_ses_client(region: str) -> boto3.client:
    if os.environ.get("DATA_SECURITY_MODE") in ["SINGLE_USER", "USER_ISOLATION"]:
        boto_session = _get_uc_session("ses-send-email")
        return boto_session.client("ses", region_name=region)
    return boto3.client("ses", region_name=region)
