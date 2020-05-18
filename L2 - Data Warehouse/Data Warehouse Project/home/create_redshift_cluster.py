import pandas as pd
import boto3
import json
import configparser
import psycopg2

def main():
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    #STEP 1 => Load all the configurations required creating the redshift cluster
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    '''
    df = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })

    print(df) '''
    
    #STEP 2 => Instantiate and create the resources and client required
    
    ec2 = boto3.resource('ec2', region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
    s3 = boto3.resource('s3', region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
    iam = boto3.client('iam', region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
    redshift = boto3.client('redshift', region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)

    #Step 3 => Create New IAM Role for Redshift and attach Polict(Ex S3), so that Redshift would be able to access S3 data
    
    try:
        print("Create a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
    
    try:
        print("Attach Policy to IAM Role")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)
        
    print("Fetch Arn Of the IAM Role")
    
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    print(roleArn)
    
    #Step 3 => Create Redshift Cluser based on the configurations provided in the dwh.cfg file and display the cluster properties
    
    print("Creating Redshift Role")
    try:
        response = redshift.create_cluster(
            #Hardware
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),
            
            #Identifiers & Credentials
            DBName = DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,
            
            #Role With the S3 Access
            IamRoles = [roleArn]
            
        )
    except Exception as e:
        print(e)
    
    print("Retrieving Cluster Properties")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in myClusterProps.items() if k in keysToShow]
    pd.DataFrame(data=x, columns=["Key", "Value"])

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    
    #Step 4 => Assisgn the Cluster a VPC and Default Security group based on your access (Check for the list of secu)
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        print(list(vpc.security_groups.all())[6])
        defaultSg = list(vpc.security_groups.all())[6]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
    
    print('Connecting to the cluster')
    print(config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Connected')

    

        

if __name__ == "__main__":
    main()