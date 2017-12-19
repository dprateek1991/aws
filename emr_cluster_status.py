#####################################################################################
# Script Name   : emr_cluster_status
# Author        : Prateek Dubey
# Purpose       : Script to check Active EMR Clusters
# Creation Date : 19-DEC-2017
#####################################################################################

import boto3

class ActiveEMRClusterChecker(object):

    def __init__(self):
        self.emr_client = None
        self.active_cluster_ids = None
        self.cluster_id = None
        self.description = None

    def run(self):
        self.set_emr_client()
        self.list_active_clusters()
        self.number_of_active_clusters()
        self.describe_cluster(self.cluster_id)

    def set_emr_client(self):
        session = boto3.Session()
        self.emr_client = session.client('emr',region_name= 'us-east-1',aws_access_key_id='xxxx',aws_secret_access_key='xxxx')

    def list_active_clusters(self):
        active_cluster_states = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        response = self.emr_client.list_clusters(ClusterStates=active_cluster_states)
        self.active_cluster_ids = [cluster["Id"] for cluster in response["Clusters"]]
        self.cluster_id = ",".join(self.active_cluster_ids)
        print("Active Cluster ID: {}".format(self.cluster_id))

    def number_of_active_clusters(self):
        if not self.active_cluster_ids:
            print("No active clusters presently")
        else:
            print("Found {} active clusters".format(len(self.active_cluster_ids)))

    def describe_cluster(self,cluster_id):
        description = self.emr_client.describe_cluster(ClusterId=cluster_id)
        state = description['Cluster']['Status']['State']
        name = description['Cluster']['Name']
        keypair = description['Cluster']['Ec2InstanceAttributes']['Ec2KeyName']
        description = {'state': state, 'name': name, 'keypair': keypair}
        print("Description: {}".format(description))
        return description

def lambda_handler(event, context):
    ActiveEMRClusterChecker().run()
