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

import six
from uuid import uuid4

from cinderclient.exceptions import NotFound
from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import loopingcall
from karbor.common import constants
from karbor import exception
from karbor.i18n import _, _LE, _LI
from karbor.services.protection.client_factory import ClientFactory
from karbor.services.protection.protection_plugins.base_protection_plugin \
    import BaseProtectionPlugin
from karbor.services.protection.protection_plugins.volume \
    import volume_plugin_cinder_schemas as cinder_schemas
from karbor.services.protection.restore_heat import HeatResource

protection_opts = [
    cfg.IntOpt('protection_sync_interval',
               default=60,
               help='update protection status interval')
]
CONF = cfg.CONF
CONF.register_opts(protection_opts)

LOG = logging.getLogger(__name__)


class CinderProtectionPlugin(BaseProtectionPlugin):
    _SUPPORT_RESOURCE_TYPES = [constants.VOLUME_RESOURCE_TYPE]

    def __init__(self, config):
        super(CinderProtectionPlugin, self).__init__(config)
        # TODO(hurong): add create admin_cinder_client
        # self.admin_cinder_client = None
        self.protection_resource_map = {}
        self.protection_sync_interval = CONF.protection_sync_interval

        sync_status_loop = loopingcall.FixedIntervalLoopingCall(
            self.sync_status)
        sync_status_loop.start(interval=self.protection_sync_interval,
                               initial_delay=self.protection_sync_interval)

    def get_supported_resources_types(self):
        return self._SUPPORT_RESOURCE_TYPES

    def get_options_schema(self, resources_type):
        return cinder_schemas.OPTIONS_SCHEMA

    def get_restore_schema(self, resources_type):
        return cinder_schemas.RESTORE_SCHEMA

    def get_saved_info_schema(self, resources_type):
        return cinder_schemas.SAVED_INFO_SCHEMA

    def get_saved_info(self, metadata_store, resource):
        # TODO(hurong)
        pass

    def _cinder_client(self, cntxt):
        return ClientFactory.create_client("cinder", cntxt)

    def create_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        # backup_name = kwargs.get("backup_name", "karbor_backup_%s" %
        # resource.id)
        resource = resource_node.value
        volume_id = resource.id
        backup_name = kwargs.get("backup_name", "karbor_backup_%s" % volume_id)

        bank_section = checkpoint.get_resource_bank_section(volume_id)

        resource_definition = {"volume_id": volume_id}
        cinder_client = self._cinder_client(cntxt)

        # TODO: add by wangliuan, saved as per resource context
        # self.admin_cinder_client = cinder_client

        LOG.info(_("creating volume backup, volume_id: %s."), volume_id)
        try:
            bank_section.create_object("status",
                                       constants.RESOURCE_STATUS_PROTECTING)

            # get backup-az and backup-type for hybrid-cloud
            volume = cinder_client.volumes.get(volume_id)
            meta = getattr(volume, "os-vol-host-attr:host")
            volume_type = getattr(volume, "volume_type")
            backup_az = meta.split(":")[-1]
            backup_type = "%s@%s" % (volume_type.split("@")[0], backup_az)
            size = volume.size

            backup = cinder_client.backups.create(volume_id=volume_id,
                                                  name=backup_name,
                                                  force=True)

            resource_definition["backup_id"] = backup.id
            resource_definition["backup_az"] = backup_az
            resource_definition["backup_type"] = backup_type
            resource_definition["size"] = size

            bank_section.create_object("metadata", resource_definition)
            self.protection_resource_map[volume_id] = {
                "bank_section": bank_section,
                "backup_id": backup.id,
                "operation": "create",
                "cinder_client": cinder_client,
            }
        except Exception as e:
            LOG.error(_LE("create volume backup failed, volume_id: %s."),
                      volume_id)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.CreateBackupFailed(
                reason=six.text_type(e),
                resource_id=volume_id,
                resource_type=constants.VOLUME_RESOURCE_TYPE
            )

    def delete_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        resource_id = resource_node.value.id

        bank_section = checkpoint.get_resource_bank_section(resource_id)
        cinder_client = self._cinder_client(cntxt)

        LOG.info(_("deleting volume backup, volume_id: %s."), resource_id)
        try:
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_DELETING)
            resource_definition = bank_section.get_object("metadata")
            backup_id = resource_definition["backup_id"]
            cinder_client.backups.delete(backup_id)
            bank_section.delete_object("metadata")
            self.protection_resource_map[resource_id] = {
                "bank_section": bank_section,
                "backup_id": backup_id,
                "operation": "delete",
                "cinder_client": cinder_client
            }
        except Exception as e:
            LOG.error(_LE("delete volume backup failed, volume_id: %s."),
                      resource_id)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_ERROR)

            raise exception.DeleteBackupFailed(
                reason=six.text_type(e),
                resource_id=resource_id,
                resource_type=constants.VOLUME_RESOURCE_TYPE
            )

    def restore_backup(self, cntxt, checkpoint, **kwargs):
        # TODO(hurong):
        resource_node = kwargs.get("node")
        resource_id = resource_node.value.id
        heat_template = kwargs.get("heat_template")

        name = kwargs.get("restore_name",
                          "%s@%s" % (checkpoint.id, resource_id))
        description = kwargs.get("restore_description")

        LOG.info(_("restoring volume backup, volume_id: %s."),
                 resource_id)
        heat_resource_id = str(uuid4())
        heat_resource = HeatResource(heat_resource_id,
                                     constants.VOLUME_RESOURCE_TYPE)

        bank_section = checkpoint.get_resource_bank_section(resource_id)
        try:
            resource_definition = bank_section.get_object("metadata")
            backup_id = resource_definition["backup_id"]

            # TODO(luobin): get backup-az and backup-type for hybrid-cloud
            backup_az = resource_definition["backup_az"]
            backup_type = resource_definition["backup_type"]
            size = resource_definition["size"]
            properties = {"backup_id": backup_id,
                          "availability_zone": backup_az,
                          "volume_type": backup_type,
                          "size": size}

            if name is not None:
                properties["name"] = name
            if description is not None:
                properties["description"] = description

            for key, value in properties.items():
                heat_resource.set_property(key, value)

            heat_template.put_resource(resource_id, heat_resource)
        except Exception as e:
            LOG.error(_LE("restore volume backup failed, volume_id: %s."),
                      resource_id)
            raise exception.RestoreBackupFailed(
                reason=six.text_type(e),
                resource_id=resource_id,
                resource_type=constants.VOLUME_RESOURCE_TYPE
            )

    def sync_status(self):
        for resource_id, resource_info in self.protection_resource_map.items():
            backup_id = resource_info["backup_id"]
            bank_section = resource_info["bank_section"]
            cinder_client = resource_info["cinder_client"]
            operation = resource_info["operation"]
            try:
                backup = cinder_client.backups.get(backup_id)
                if backup.status == "available":
                    bank_section.update_object(
                        "status", constants.RESOURCE_STATUS_AVAILABLE)
                    self.protection_resource_map.pop(resource_id)
                elif backup.status in ["error", "error-deleting"]:
                    bank_section.update_object(
                        "status", constants.RESOURCE_STATUS_ERROR)
                    self.protection_resource_map.pop(resource_id)
                else:
                    continue
            except Exception as exc:
                if operation == "delete" and type(exc) == NotFound:
                    bank_section.update_object(
                        "status",
                        constants.RESOURCE_STATUS_DELETED)
                    LOG.info(_LI("deleting volume backup finished, "
                                 "backup id: %s"), backup_id)
                else:
                    LOG.error(_LE("deleting volume backup error.exc: %s"),
                              six.text_type(exc))
                self.protection_resource_map.pop(resource_id)
