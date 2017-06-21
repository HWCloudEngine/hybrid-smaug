# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


from oslo_config import cfg
from oslo_log import log as logging

from karbor.common import constants
from karbor.services.protection import resource_flow
from karbor.services.protection import restore_reference
from taskflow import task

sync_status_opts = [
    cfg.IntOpt('sync_status_interval',
               default=20,
               help='update protection status interval')
]

CONF = cfg.CONF
CONF.register_opts(sync_status_opts)

LOG = logging.getLogger(__name__)


class InitiateRestoreTask(task.Task):
    def execute(self, restore, *args, **kwargs):
        LOG.debug("Initiate restore restore_id: %s", restore.id)
        restore['status'] = constants.RESTORE_STATUS_IN_PROGRESS
        restore.save()

    def revert(self, restore, *args, **kwargs):
        LOG.debug("Failed to restore restore_id: %s", restore.id)
        restore['status'] = constants.RESTORE_STATUS_FAILURE
        restore.save()


class CompleteRestoreTask(task.Task):
    def execute(self, restore, *args, **kwargs):
        LOG.debug("Complete restore restore_id: %s", restore.id)
        restore['status'] = constants.RESTORE_STATUS_SUCCESS
        restore.save()


class CreateReferenceTask(task.Task):
    default_provides = 'restore_reference'

    def execute(self):
        LOG.info('Creating Restore Reference.')
        reference = restore_reference.RestoreReference()
        return reference


def get_flow(context, workflow_engine, checkpoint, provider, restore,
             restore_auth):
    target = restore.get('restore_target', None)

    heat_conf = {}
    if target is not None:
        heat_conf["auth_url"] = target
        if restore_auth is not None:
            auth_type = restore_auth.get("type", None)
            if auth_type == "password":
                heat_conf["username"] = restore_auth["username"]
                heat_conf["password"] = restore_auth["password"]

    resource_graph = checkpoint.resource_graph
    parameters = restore.parameters
    flow_name = "Restore_" + checkpoint.id
    restore_flow = workflow_engine.build_flow(flow_name, 'linear')
    plugins = provider.load_plugins()
    resources_task_flow = resource_flow.build_resource_flow(
        operation_type=constants.OPERATION_RESTORE,
        context=context,
        workflow_engine=workflow_engine,
        resource_graph=resource_graph,
        plugins=plugins,
        parameters=parameters
    )

    workflow_engine.add_tasks(
        restore_flow,
        InitiateRestoreTask(),
        CreateReferenceTask(),
        resources_task_flow,
        CompleteRestoreTask()
    )
    flow_engine = workflow_engine.get_engine(restore_flow,
                                             store={'checkpoint': checkpoint,
                                                    'restore': restore})
    return flow_engine
