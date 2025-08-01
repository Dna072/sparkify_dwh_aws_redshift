import time

import pandas as pd
import boto3
import psycopg2
import json
import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY_P')
SECRET = config.get('AWS', 'SECRET_P')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DB_NAME")
DWH_DB_USER = config.get("DWH", "DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DB_PASSWORD")
DWH_PORT = config.get("DWH", "DB_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)
# Display cluster properties
pd.DataFrame({
    "Param": ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER",
              "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
    "Value": [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER,
              DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
})

# Create clients and resources for EC2, IAM, S3 and Redshift
ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

iam = boto3.client('iam',
                   region_name="us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)



def create_redshift_iam_role():
    """
        Create or retrieve an IAM role that allows Redshift to access S3.

        If the role already exists, it is returned. Otherwise, a new role is created
        with the AmazonS3ReadOnlyAccess policy attached.

        Returns:
            str: ARN of the IAM role.
    """
    try:
        # Try to get existing role
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(f"✅ IAM Role ARN: {role_arn}")
        return role_arn

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('✅ Creating a new IAM Role')

            try:
                dwh_role = iam.create_role(
                    Path='/',
                    RoleName=DWH_IAM_ROLE_NAME,
                    Description="Allows Redshift clusters to call AWS services on your behalf.",
                    AssumeRolePolicyDocument=json.dumps(
                        {
                            'Statement': [{
                                'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}
                            }],
                            'Version': '2012-10-17'
                        }
                    )
                )

                print("✅ Attaching S3ReadOnlyAccess Policy...")
                response = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                                                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                                  )['ResponseMetadata']['HTTPStatusCode']
                print(f"✅ IAM role created! ARN: {iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']}")
            except ClientError as ce:
                print("❌Failed to create role: ", ce)
        else:
            print("❌ Error retrieving role: ", e)


def create_redshift_cluster():
    """
        Create a Redshift cluster with the provided configuration from dwh.cfg.

        Uses the IAM role for S3 access, and sets the cluster to be publicly accessible
        for demo purposes.
    """
    try:
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        response = redshift.create_cluster(
            # parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # parameter for role (to allow s3 access)
            IamRoles=[role_arn],

            # publicly accessible for demo
            PubliclyAccessible=True
        )
    except Exception as e:
        print("❌ Failed to create Redshift cluster: ", e)


def wait_for_cluster_available(timeout=1800, interval=30):
    """
        Poll until the Redshift cluster becomes available or until timeout.

        Args:
            timeout (int): Maximum time in seconds to wait.
            interval (int): Interval in seconds between status checks.

        Returns:
            dict: Properties of the available cluster.

        Raises:
            TimeoutError: If the cluster doesn't become available within the timeout.
    """
    print("⏳  Waiting for Redshift cluster to become available...")
    elapsed = 0
    while elapsed < timeout:
        try:
            props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            status = props['ClusterStatus']
            print(f" → Current status: {status}")
            if status == 'available':
                print("✅ Cluster is available!")
                print(f"🔗 Cluster endpoint: {props['Endpoint']['Address']}")
                return props
        except Exception as e:
            print("❌ Error while checking status:", e)

        time.sleep(interval)
        elapsed += interval

    raise TimeoutError("🛑 Timed out waiting for the cluster to become available")


def wait_for_cluster_deletion(timeout=900, interval=20):
    """
      Wait for the Redshift cluster to be fully deleted.

      Args:
          timeout (int): Maximum time in seconds to wait.
          interval (int): Interval in seconds between status checks.

      Returns:
          None

      Raises:
          TimeoutError: If the cluster is not deleted within the timeout.
    """
    print("⏳  Waiting for Redshift cluster to be deleted...")
    elapsed = 0

    while elapsed < timeout:
        try:
            redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
            print("➡ Cluster deletion in progress...")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print("✅ Cluster successfully deleted.")
                return
            else:
                print("❌ Error while checking deletion status (most likely deleted already): ", e)

        time.sleep(interval)
        elapsed += interval

    raise TimeoutError("🛑 Timed out waiting for the cluster to be deleted.")


def pretty_redshift_props(props=None):
    """
      Print and return a DataFrame of key Redshift cluster properties.

      Args:
          props (dict, optional): Cluster properties. If not provided, fetched via API.

      Returns:
          pd.DataFrame: DataFrame containing selected cluster properties.
    """
    if props is None:
        props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    pd.set_option('display.max_colwidth', None)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
                    "DBName", "Endpoint", "NumberOfNodes", "VpcId"]
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    df = pd.DataFrame(data=x, columns=["Key", "Value"])
    print(df.to_string())
    return df


# Function to update the default security group to allow ingress to port DWH_PORT
def open_port_to_redshift(my_cluster_props):
    """
        Open TCP port in the default security group to allow Redshift access.

        Args:
            my_cluster_props (dict): Redshift cluster properties containing VPC ID.

        Returns:
            None
    """
    try:
        vpc = ec2.Vpc(id=my_cluster_props['VpcId'])
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)

        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )

        print(f"✅ Port {DWH_PORT} opened for Redshift.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print(f"⚠️ Port {DWH_PORT} already open - skipping...")
        else:
            print("❌ Failed to open port:", e)


# Test connection to cluster
def test_cluster_connection(my_cluster_props=None):
    """
        Attempt to connect to the Redshift cluster using psycopg2.

        Args:
            my_cluster_props (dict, optional): Redshift cluster properties. Fetched if None.

        Returns:
            None
    """
    print("🧪 Testing connection...")
    if my_cluster_props is None:
        my_cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        conn = psycopg2.connect(
            dbname=DWH_DB,
            user=DWH_DB_USER,
            password=DWH_DB_PASSWORD,
            host=my_cluster_props['Endpoint']['Address'],
            port=DWH_PORT
        )
        print('✅ Connection successful!')
        conn.close()
    except Exception as e:
        print("❌ Connection failed.")
        print(e)


def delete_redshift_cluster():
    """
       Delete the Redshift cluster and wait for confirmation of deletion.

       Returns:
           None
    """
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True
        )
        my_cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        print('🗑️ Cluster deletion initiated.')
        wait_for_cluster_deletion()
    except Exception as e:
        print('❌ Could not delete the redshift cluster:', e)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--create", action="store_true", help="Create cluster and wait")
    parser.add_argument("--delete", action="store_true", help="Delete Redshift cluster")
    parser.add_argument("--test", action="store_true", help="Test connection to cluster")
    args = parser.parse_args()

    if args.create:
        create_redshift_iam_role()
        create_redshift_cluster()
        props = wait_for_cluster_available()
        open_port_to_redshift(props)

        print("⌛️ Allowing 20 seconds to pass before testing connection")
        time.sleep(20)
        test_cluster_connection(props)

    elif args.delete:
        delete_redshift_cluster()

    elif args.test:
        test_cluster_connection()

    else:
        props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        pretty_redshift_props(props)
        test_cluster_connection(props)
