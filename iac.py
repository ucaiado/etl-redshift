#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
'Infrastructure as Code'


@author: udacity, ucaiado

Created on 04/25/2020
"""

# import libraries
import argparse
import textwrap
import boto3
import json
import configparser
import pandas as pd


'''
Begin help functions and variables
'''

config = configparser.ConfigParser()
config.read_file(open('confs/dwh.cfg'))


KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_HOST = config.get("CLUSTER", "HOST")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH",  "DWH_IAM_ROLE_NAME")
IAM_ROLE_ARN = config.get("IAM_ROLE",  "ARN")


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes",
                  'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

'''
End help functions and variables
'''


if __name__ == '__main__':
    s_txt = '''\
            Infrastructure as code
            --------------------------------
            Create Amazon Readshift cluster
            '''
    # include and parse variables
    obj_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=obj_formatter, description=textwrap.dedent(s_txt))

    s_help = 'Create IAM role'
    parser.add_argument('-i', '--iam', action='store_true', help=s_help)

    s_help = 'Create a Readshift cluster'
    parser.add_argument('-r', '--redshift', action='store_true', help=s_help)

    s_help = 'Check Readshift cluster status'
    parser.add_argument('-s', '--status', action='store_true', help=s_help)

    s_help = 'Open incoming TCP port'
    parser.add_argument('-t', '--tcp', action='store_true', help=s_help)

    s_help = 'Clean up your resources'
    parser.add_argument('-d', '--delete', action='store_true', help=s_help)

    # check what should do
    args = parser.parse_args()
    b_create_iam = args.iam
    b_create_redshift = args.redshift
    b_check_status = args.status
    b_open_tcp = args.tcp
    b_delete = args.delete

    s_err = 'Please select only one option from -h menu'
    b_test_all = (b_create_iam or b_create_redshift or b_check_status or
                  b_open_tcp or b_delete)
    assert b_test_all, s_err
    if b_create_redshift or b_open_tcp:
        assert len(IAM_ROLE_ARN) > 2, 'Please run --iam flag before this step'
    if b_open_tcp:
        s_err = ('Please run --iam and --redshift flags and wait for the '
                 'cluster be ready before proceed to this step')
        assert len(DWH_HOST) > 2, s_err

    # create clients
    print('...create clients for EC2, S3, IAM, and Redshift')
    ec2 = boto3.resource(
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    s3 = boto3.resource(
        's3',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    if b_create_iam:
        # Create the IAM role
        try:
            dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description=('Allows Redishift clusters to call AWS '
                             'services on your behalf'),
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'}
                ))
            print('...create a new IAM Role')
        except Exception as e:
            print(e)

        # Attaching Policy
        print('...attach policy')
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        # Get and print the IAM role ARN
        print('...get the IAM role ARN')
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print('   !! fill in the IAM_ROLE ARN field in dwh.cfg file with the '
              'following string:')
        print(role_arn)
    elif b_create_redshift:
        try:
            response = redshift.create_cluster(
                # add parameters for hardware
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),

                # add parameters for identifiers & credentials
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,

                # add parameter for role (to allow s3 access)
                IamRoles=[IAM_ROLE_ARN])
            print('...create redshift cluster')
        except Exception as e:
            print(e)
    elif b_check_status:
        print('...check cluster status')
        try:
            my_cluster_prop = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            df = prettyRedshiftProps(my_cluster_prop).set_index('Key')
            print(df.to_string())
            print('\ntry to recover endpoint string')
            if my_cluster_prop['ClusterStatus'] == 'available':
                DWH_ENDPOINT = my_cluster_prop['Endpoint']['Address']
                DWH_ROLE_ARN = my_cluster_prop['IamRoles'][0]['IamRoleArn']
                endpoint = my_cluster_prop['Endpoint']['Address']
                print('   !! fill in the HOST CLUSTER field in dwh.cfg file '
                      'with the following string:')
                print("HOST :: ", endpoint)
            else:
                print('   !!cluster status is not "available" yet')
        except Exception as e:
            print(e)
    elif b_open_tcp:
        print('...open an incoming TCP port to access the cluster ednpoint')
        my_cluster_prop = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        try:
            vpc = ec2.Vpc(id=my_cluster_prop['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]

            defaultSg.authorize_ingress(
                GroupName='default',
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
        except Exception as e:
            print(e)

    elif b_delete:
        print('...clean up your resources')
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True)

        iam.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

        print('...check cluster status')
        my_cluster_prop = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = prettyRedshiftProps(my_cluster_prop).set_index('Key')
        print(df.to_string())

