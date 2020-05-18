import boto3
import configparser


def main():
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    #STEP 1 => Load all the configurations required creating the redshift cluster
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    redshift = boto3.client('redshift', region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
    
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    

        

if __name__ == "__main__":
    main()