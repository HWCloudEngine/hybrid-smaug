# Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from karbor.common import constants
from karbor.services.protection.protection_plugins.base_protection_plugin \
    import BaseProtectionPlugin


class ProjectProtectionPlugin(BaseProtectionPlugin):
    def __init__(self, config=None):
        super(ProjectProtectionPlugin, self).__init__(config)

    def get_options_schema(self, resources_type):
        pass

    def get_restore_schema(self, resources_type):
        pass

    def get_saved_info_schema(self, resources_type):
        pass

    def get_resource_stats(self, checkpoint, resource_id):
        # Get the status of this resource
        bank_section = checkpoint.get_resource_bank_section(resource_id)
        try:
            status = bank_section.get_object("status")
            return status
        except Exception:
            return constants.RESOURCE_STATUS_UNDEFINED

    def create_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        project_id = resource_node.value.id
        bank_section = checkpoint.get_resource_bank_section(project_id)
        bank_section.create_object("status",
                                   constants.RESOURCE_STATUS_AVAILABLE)
        return

    def restore_backup(self, cntxt, checkpoint, **kwargs):
        return

    def delete_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        project_id = resource_node.value.id
        bank_section = checkpoint.get_resource_bank_section(project_id)
        bank_section.create_object("status",
                                   constants.RESOURCE_STATUS_DELETED)
        return

    def get_saved_info(self, metadata_store, resource):
        pass

    def get_supported_resources_types(self):
        return [constants.PROJECT_RESOURCE_TYPE]

