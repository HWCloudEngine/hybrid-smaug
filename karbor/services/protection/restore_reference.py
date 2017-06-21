#    Licensed under the Apache License, Version 2.0 (the "License"); you may
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

from karbor.exception import InvalidOriginalId
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class RestoreReference(object):
    def __init__(self):
        super(RestoreReference, self).__init__()
        self._resources = []
        self._original_id_resource_map = {}

    def put_resource(self, original_id, restore_resource):
        self._resources.append(restore_resource)
        self._original_id_resource_map[original_id] = restore_resource

    def get_resource_reference(self, original_id):
        if original_id in self._original_id_resource_map:
            return self._original_id_resource_map[original_id]
        else:
            LOG.error("The reference is not found, original_id:%s",
                      original_id)
            raise InvalidOriginalId

    def len(self):
        return len(self._resources)

    def to_dict(self):
        resources_dict = {}
        for resource in self._resources:
            resource_id = resource.resource_id
            resource_dict = resource.to_dict()
            resources_dict[resource_id] = resource_dict[resource_id]
        return resources_dict
