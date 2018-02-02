import os
import datetime
import azure.batch.batch_auth as batchauth
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels
import azure.storage.blob as azureblob


VERSION = 'v4'
RUN_FILE = 'hello_world.py'
TASK_FILES = [RUN_FILE]

BATCH_ACCOUNT_NAME = 'rts0batch'
BATCH_ACCOUNT_KEY = 'WERc3Oj7y0MjSme8uaEHbV6xl50AF/zCjJLma4V0/Rrxqd3eFcDsQSm6JMnjFeND5XUsGSg8MayU5kPrRVpMxA=='
BATCH_ACCOUNT_URL = 'https://rts0batch.westeurope.batch.azure.com'

STORAGE_ACCOUNT_NAME = 'rts0storage'
STORAGE_ACCOUNT_KEY = 'UAmeIlGVIYulCB3G9d4QBx/oQMujzhvp0Vd5RElfbL2xRR6oBRUIaAe1XD2nV5npjPGZn0Z6VuZt6zCEKNjqKg=='

JOB_ID = 'job_{}'

POOL_ID = 'pool_{}'
POOL_NODE_COUNT = 1
POOL_VM_SIZE = 'Basic_A1'

NODE_OS_PUBLISHER = 'microsoft-ads'
NODE_OS_OFFER = 'linux-data-science-vm'
NODE_OS_SKU = 'linuxdsvm'
NODE_AGENT_SKU = 'batch.node.centos 7'

job_id = JOB_ID.format(VERSION)
pool_id = POOL_ID.format(VERSION)
app_container_name = 'application'


###############################################################################
# Helpers
###############################################################################
def wrap_commands_in_shell(ostype, commands):
    if ostype.lower() == 'linux':
        return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
            ';'.join(commands))
    elif ostype.lower() == 'windows':
        return 'cmd.exe /c "{}"'.format('&'.join(commands))
    else:
        raise ValueError('unknown ostype: {}'.format(ostype))


def upload_file_to_container(block_blob_client, container_name, file_path):
    blob_name = os.path.basename(file_path)

    print('Uploading file {} to container [{}]...'.format(file_path, container_name))

    block_blob_client.create_blob_from_path(container_name, blob_name, file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name, blob_name, permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name, blob_name, sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name, blob_source=sas_url)

###############################################################################
# Storage and Batch connect 
###############################################################################
blob_client = azureblob.BlockBlobService(
    account_name=STORAGE_ACCOUNT_NAME, account_key=STORAGE_ACCOUNT_KEY)

blob_client.create_container(app_container_name, fail_on_exist=False)

resource_files = [
    upload_file_to_container(blob_client, app_container_name, full_path)
    for full_path in map(os.path.realpath, TASK_FILES)]

credentials = batchauth.SharedKeyCredentials(BATCH_ACCOUNT_NAME, BATCH_ACCOUNT_KEY)
batch_service_client = batch.BatchServiceClient(
                        credentials, base_url=BATCH_ACCOUNT_URL)

###############################################################################
# Make Pool and Node
###############################################################################
pool_commands = [
    'cp -p {} $AZ_BATCH_NODE_SHARED_DIR'.format(f) for f in TASK_FILES
]

img_ref = batchmodels.ImageReference(NODE_OS_PUBLISHER, NODE_OS_OFFER, NODE_OS_SKU)
vm_conf = batchmodels.VirtualMachineConfiguration(image_reference=img_ref,
                                                  node_agent_sku_id=NODE_AGENT_SKU)
start_task = batch.models.StartTask(resource_files=resource_files, wait_for_success=True,
                                    command_line=wrap_commands_in_shell('linux', pool_commands))

pool = batch.models.PoolAddParameter(id=pool_id, virtual_machine_configuration=vm_conf,
                                     vm_size=POOL_VM_SIZE, start_task=start_task,
                                     target_dedicated_nodes=POOL_NODE_COUNT)

batch_service_client.pool.add(pool)

###############################################################################
# Make Job and Task
###############################################################################
job = batch.models.JobAddParameter(
    job_id, batch.models.PoolInformation(pool_id=pool_id))

batch_service_client.job.add(job)

task_commands = [
    'python3 $AZ_BATCH_NODE_SHARED_DIR/{} --key={}'.format(RUN_FILE, 'value'),
]

task = batch.models.TaskAddParameter(
    'task_{}'.format(job_id), wrap_commands_in_shell('linux', task_commands))

batch_service_client.task.add(job_id, task)

