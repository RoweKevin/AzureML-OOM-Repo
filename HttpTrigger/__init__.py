import logging
import os
from azureml.core import Workspace, Experiment, Dataset
from azureml.core.compute import AmlCompute
from azureml.core.compute import ComputeTarget

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    subscription_id = os.getenv("SUBSCRIPTION_ID", default="YOUR_SUB_ID")
    resource_group = os.getenv("RESOURCE_GROUP", default="YOUR_RG_NAME")
    workspace_name = os.getenv("WORKSPACE_NAME", default="WORKSPACE_NAME")
    workspace_region = os.getenv("WORKSPACE_REGION", default="THE_REGION_WHERE_YOUR_WORSPACE_IS ie:eastus")

    ws = Workspace.from_config()
    print(ws.name, ws.resource_group, ws.location, ws.subscription_id, sep='\n')

    experiment_name = 'train-with-datasets'
    exp = Experiment(workspace=ws, name=experiment_name)

    # choose a name for your cluster
    compute_name = os.environ.get('AML_COMPUTE_CLUSTER_NAME', 'cpu-cluster')
    compute_min_nodes = os.environ.get('AML_COMPUTE_CLUSTER_MIN_NODES', 0)
    compute_max_nodes = os.environ.get('AML_COMPUTE_CLUSTER_MAX_NODES', 4)

    # This example uses CPU VM. For using GPU VM, set SKU to STANDARD_NC6
    vm_size = os.environ.get('AML_COMPUTE_CLUSTER_SKU', 'STANDARD_D2_V2')


    if compute_name in ws.compute_targets:
        compute_target = ws.compute_targets[compute_name]
        if compute_target and type(compute_target) is AmlCompute:
            print('found compute target. just use it. ' + compute_name)
    else:
        print('creating a new compute target...')
        provisioning_config = AmlCompute.provisioning_configuration(vm_size=vm_size,
                                                                    min_nodes=compute_min_nodes, 
                                                                    max_nodes=compute_max_nodes)

        # create the cluster
        compute_target = ComputeTarget.create(ws, compute_name, provisioning_config)
        
        # can poll for a minimum number of nodes and for a specific timeout. 
        # if no min node count is provided it will use the scale settings for the cluster
        compute_target.wait_for_completion(show_output=True, min_node_count=None, timeout_in_minutes=20)
        
        # For a more detailed view of current AmlCompute status, use get_status()
        print(compute_target.get_status().serialize())

    datastore = ws.get_default_datastore()
    datastore.upload_files(files = ['./train-dataset/iris.csv'],
                        target_path = 'train-dataset/tabular/',
                        overwrite = True,
                        show_progress = True)
                        
    dataset = Dataset.Tabular.from_delimited_files(path = [(datastore, 'train-dataset/tabular/iris.csv')])

    # preview the first 3 rows of the dataset
    dataset.take(3).to_pandas_dataframe()

    return func.HttpResponse(
             "This HTTP triggered function executed successfully.",
             status_code=200
    )
