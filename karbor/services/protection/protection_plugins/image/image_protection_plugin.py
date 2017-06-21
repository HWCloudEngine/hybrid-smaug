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

from functools import partial

from karbor.common import constants
from karbor import exception
from karbor.services.protection.client_factory import ClientFactory
from karbor.services.protection import protection_plugin
from karbor.services.protection.protection_plugins.image \
    import image_plugin_schemas as image_schemas
from karbor.services.protection.protection_plugins import utils
from oslo_config import cfg
from oslo_log import log as logging


image_backup_opts = [
    cfg.IntOpt('poll_interval', default=10,
               help='Poll interval for image status'),
]

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def get_image_status(glance_client, image_id):
    LOG.debug('Polling image (image_id: %s)', image_id)
    try:
        status = glance_client.images.get(image_id)
    except exception.NotFound:
        status = 'not-found'
    LOG.debug('Polled image (image_id: %s) status: %s',
              image_id, status)
    return status


class ProtectOperation(protection_plugin.Operation):
    def __init__(self, poll_interval):
        super(ProtectOperation, self).__init__()
        self._interval = poll_interval

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        image_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(image_id)

        resource_definition = {"resource_id": image_id}
        glance_client = ClientFactory.create_client('glance', context)
        LOG.info("Creating image backup, image_id: %s.", image_id)
        try:
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_PROTECTING)
            image_info = glance_client.images.get(image_id)
            if image_info.status != "active":
                is_success = utils.status_poll(
                    partial(get_image_status, glance_client, image_info.id),
                    interval=self._interval, success_statuses={'active'},
                    ignore_statuses={'queued', 'saving'},
                    failure_statuses={'killed', 'deleted', 'pending_delete',
                                      'deactivated', 'NotFound'}
                )
                if is_success is not True:
                    LOG.error("The status of image (id: %s) is invalid.",
                              image_id)
                    raise exception.CreateBackupFailed(
                        reason="The status of image is invalid.",
                        resource_id=image_id,
                        resource_type=constants.IMAGE_RESOURCE_TYPE)

            hyper_image = None
            if image_info.container_format == "hypercontainer":
                original_image = image_info['__original_image']
                hyper_image = image_id
            else:
                original_image = image_id
                images = glance_client.images.list()
                for image in images.next():
                    if image.container_format == "hypercontainer":
                        if image['__original_image'] == image_id:
                            hyper_image = image['id']
                            break

            image_metadata = {
                "original_image": original_image,
                "hyper_image": hyper_image
            }
            resource_definition["image_metadata"] = image_metadata
            bank_section.update_object("metadata", resource_definition)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_AVAILABLE)
            LOG.info('Protecting image (id: %s) to bank completed '
                     'successfully', image_id)
        except Exception as err:
            LOG.error("Create image backup failed, image_id: %s.", image_id)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.CreateBackupFailed(
                reason=err,
                resource_id=image_id,
                resource_type=constants.IMAGE_RESOURCE_TYPE)


class DeleteOperation(protection_plugin.Operation):
    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        image_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(image_id)

        LOG.info("Deleting image backup failed, image_id: %s.", image_id)
        try:
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_DELETING)
            objects = bank_section.list_objects()
            for obj in objects:
                if obj == "status":
                    continue
                bank_section.delete_object(obj)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_DELETED)
        except Exception as err:
            LOG.error("delete image backup failed, image_id: %s.", image_id)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.DeleteBackupFailed(
                reason=err,
                resource_id=image_id,
                resource_type=constants.IMAGE_RESOURCE_TYPE)


class RestoreOperation(protection_plugin.Operation):
    def __init__(self, poll_interval):
        super(RestoreOperation, self).__init__()
        self._interval = poll_interval

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        original_image_id = resource.id
        restore_reference = kwargs.get("restore_reference")
        LOG.info("Restoring image backup, image_id: %s.", original_image_id)

        bank_section = checkpoint.get_resource_bank_section(original_image_id)
        resource_definition = bank_section.get_object('metadata')
        availability_zone = resource_definition.get('availability_zone', None)
        image_metadata = resource_definition['image_metadata']
        if availability_zone not in CONF.public_availability_zones:
            restore_reference.put_resource(original_image_id,
                                           image_metadata['original_image'])
        else:
            restore_reference.put_resource(original_image_id,
                                           image_metadata['hyper_image'])
        LOG.info("Finish restoring image backup, image_id: %s.",
                 original_image_id)


class GlanceProtectionPlugin(protection_plugin.ProtectionPlugin):
    _SUPPORT_RESOURCE_TYPES = [constants.IMAGE_RESOURCE_TYPE]

    def __init__(self, config=None):
        super(GlanceProtectionPlugin, self).__init__(config)
        self._config.register_opts(image_backup_opts,
                                   'image_backup_plugin')
        self._plugin_config = self._config.image_backup_plugin
        self._poll_interval = self._plugin_config.poll_interval

    @classmethod
    def get_supported_resources_types(cls):
        return cls._SUPPORT_RESOURCE_TYPES

    @classmethod
    def get_options_schema(cls, resources_type):
        return image_schemas.OPTIONS_SCHEMA

    @classmethod
    def get_restore_schema(cls, resources_type):
        return image_schemas.RESTORE_SCHEMA

    @classmethod
    def get_saved_info_schema(cls, resources_type):
        return image_schemas.SAVED_INFO_SCHEMA

    @classmethod
    def get_saved_info(cls, metadata_store, resource):
        pass

    def get_protect_operation(self, resource):
        return ProtectOperation(self._poll_interval)

    def get_restore_operation(self, resource):
        return RestoreOperation(self._poll_interval)

    def get_delete_operation(self, resource):
        return DeleteOperation()
