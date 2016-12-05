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

import eventlet
from uuid import uuid4

from oslo_config import cfg
from oslo_log import log as logging
from karbor.common import constants
from karbor import exception
from karbor.i18n import _, _LE
from karbor.services.protection.client_factory import ClientFactory
from karbor.services.protection.protection_plugins.base_protection_plugin \
    import BaseProtectionPlugin
from karbor.services.protection.protection_plugins.server \
    import server_plugin_schemas
from karbor.services.protection.restore_heat import HeatResource

protection_opts = [
    cfg.IntOpt('backup_image_object_size',
               default=52428800,
               help='The size in bytes of instance image objects')
]

CONF = cfg.CONF
CONF.register_opts(protection_opts)
LOG = logging.getLogger(__name__)

VOLUME_ATTACHMENT_RESOURCE = 'OS::Cinder::VolumeAttachment'
FLOATING_IP_ASSOCIATION = 'OS::Nova::FloatingIPAssociation'


class NovaProtectionPlugin(BaseProtectionPlugin):
    _SUPPORT_RESOURCE_TYPES = [constants.SERVER_RESOURCE_TYPE]

    def __init__(self, config=None):
        super(NovaProtectionPlugin, self).__init__(config)
        self._tp = eventlet.GreenPool()
        self.image_object_size = CONF.backup_image_object_size

    def _add_to_threadpool(self, func, *args, **kwargs):
        self._tp.spawn_n(func, *args, **kwargs)

    def get_options_schema(self, resource_type):
        return server_plugin_schemas.OPTIONS_SCHEMA

    def get_restore_schema(self, resource_type):
        return server_plugin_schemas.RESTORE_SCHEMA

    def get_saved_info_schema(self, resource_type):
        return server_plugin_schemas.SAVED_INFO_SCHEMA

    def get_saved_info(self, metadata_store, resource):
        # TODO(luobin)
        pass

    def _glance_client(self, cntxt):
        return ClientFactory.create_client("glance", cntxt)

    def _nova_client(self, cntxt):
        return ClientFactory.create_client("nova", cntxt)

    def _cinder_client(self, cntxt):
        return ClientFactory.create_client("cinder", cntxt)

    def _neutron_client(self, cntxt):
        return ClientFactory.create_client("neutron", cntxt)

    def create_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        server_id = resource_node.value.id

        bank_section = checkpoint.get_resource_bank_section(server_id)

        nova_client = self._nova_client(cntxt)
        # glance_client = self._glance_client(cntxt)
        neutron_client = self._neutron_client(cntxt)
        cinder_client = self._cinder_client(cntxt)

        resource_definition = {"resource_id": server_id}
        child_nodes = resource_node.child_nodes
        attach_metadata = {}
        backup_az = ""

        LOG.info(_LI("creating server backup, server_id: %s."), server_id)

        try:
            bank_section.create_object("status",
                                       constants.RESOURCE_STATUS_PROTECTING)
            for child_node in child_nodes:
                child_resource = child_node.value
                if child_resource.type == constants.VOLUME_RESOURCE_TYPE:
                    volume = cinder_client.volumes.get(child_resource.id)
                    meta = getattr(volume, "os-vol-host-attr:host")
                    backup_az = meta.split(":")[-1]
                    attachments = getattr(volume, "attachments")
                    for attachment in attachments:
                        if attachment["server_id"] == server_id:
                            attach_metadata[child_resource.id] = attachment[
                                "device"]
            resource_definition["attach_metadata"] = attach_metadata

            server = nova_client.servers.get(server_id)
            availability_zone = getattr(server, "OS-EXT-AZ:availability_zone")
            if backup_az == "":
                resource_definition["backup_az"] = availability_zone
            else:
                resource_definition["backup_az"] = backup_az

            addresses = getattr(server, "addresses")
            networks = []
            floating_ips = []
            for network_infos in addresses.values():
                for network_info in network_infos:
                    addr = network_info.get("addr")
                    mac = network_info.get("OS-EXT-IPS-MAC:mac_addr")
                    type = network_info.get("OS-EXT-IPS:type")
                    if type == 'fixed':
                        port = \
                        neutron_client.list_ports(mac_address=mac)["ports"][0]
                        if port["network_id"] not in networks:
                            networks.append(port["network_id"])
                    elif type == "floating":
                        floating_ips.append(addr)
            flavor = getattr(server, "flavor")["id"]
            image = getattr(server, "image")["id"]
            key_name = getattr(server, "key_name")
            security_groups = getattr(server, "security_groups")

            # TODO(luobin): server-metadata
            server_metadata = {"networks": networks,
                               "floating_ips": floating_ips,
                               "flavor": flavor,
                               "image": image,
                               "key_name": key_name,
                               "security_groups": security_groups
                               }
            resource_definition["server_metadata"] = server_metadata

            bank_section.create_object("metadata", resource_definition)

            bank_section.create_object("status",
                                       constants.RESOURCE_STATUS_AVAILABLE)
        except Exception as err:
            # update resource_definition backup_status
            LOG.error(_LE("create backup failed, server_id: %s, err: %s"),
                      server_id, err)
            bank_section.create_object("status",
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.CreateBackupFailed(
                reason=err,
                resource_id=server_id,
                resource_type=constants.SERVER_RESOURCE_TYPE)

    def restore_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        original_server_id = resource_node.value.id
        heat_template = kwargs.get("heat_template")

        name = kwargs.get("restore_name", "karbor-restore-server")

        LOG.info(_("restoring server backup, server_id: %s."),
                 original_server_id)

        heat_resource_id = str(uuid4())
        heat_server_resource = HeatResource(heat_resource_id,
                                            constants.SERVER_RESOURCE_TYPE)
        bank_section = checkpoint.get_resource_bank_section(original_server_id)
        try:
            resource_definition = bank_section.get_object("metadata")
            server_metadata = resource_definition["server_metadata"]
            attach_metadata = resource_definition["attach_metadata"]
            backup_az = resource_definition["backup_az"]
            properties = {
                "availability_zone": backup_az,
                "flavor": server_metadata["flavor"],
                "image": server_metadata["image"],
                "name": name,
            }

            if server_metadata["key_name"] is not None:
                properties["key_name"] = server_metadata["key_name"]

            security_groups = []
            for security_group in server_metadata["security_groups"]:
                security_groups.append(security_group["name"])
            properties["security_groups"] = security_groups

            networks = []
            for network in server_metadata["networks"]:
                networks.append({"network": network})
            properties["networks"] = networks

            for key, value in properties.items():
                heat_server_resource.set_property(key, value)

            heat_template.put_resource(original_server_id, heat_server_resource)

            # volume attachment
            for original_volume_id, device in attach_metadata.items():
                heat_resource_id = str(uuid4())
                heat_attachment_resource = HeatResource(
                    heat_resource_id,
                    VOLUME_ATTACHMENT_RESOURCE)
                instance_uuid = heat_template.get_resource_reference(
                    original_server_id)
                volume_id = heat_template.get_resource_reference(
                    original_volume_id)
                properties = {"mountpoint": device,
                              "instance_uuid": instance_uuid,
                              "volume_id": volume_id}
                for key, value in properties.items():
                    heat_attachment_resource.set_property(key, value)
                heat_template.put_resource(
                    "%s_%s" % (original_server_id, original_volume_id),
                    heat_attachment_resource)

            # floating ip association
            for floating_ip in server_metadata["floating_ips"]:
                heat_resource_id = str(uuid4())
                heat_floating_resource = HeatResource(
                    heat_resource_id,
                    FLOATING_IP_ASSOCIATION)
                instance_uuid = heat_template.get_resource_reference(
                    original_server_id)
                properties = {"instance_uuid": instance_uuid,
                              "floating_ip": floating_ip}
                for key, value in properties.items():
                    heat_floating_resource.set_property(key, value)
                heat_template.put_resource(
                    "%s_%s" % (original_server_id, floating_ip),
                    heat_floating_resource)

        except Exception as e:
            LOG.error(_LE("restore server backup failed, server_id: %s."),
                      original_server_id)
            raise exception.RestoreBackupFailed(
                reason=e,
                resource_id=original_server_id,
                resource_type=constants.SERVER_RESOURCE_TYPE
            )

    def delete_backup(self, cntxt, checkpoint, **kwargs):
        resource_node = kwargs.get("node")
        resource_id = resource_node.value.id
        bank_section = checkpoint.get_resource_bank_section(resource_id)

        LOG.info(_("deleting server backup, server_id: %s."), resource_id)

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
            # update resource_definition backup_status
            LOG.error(_LE("delete backup failed, server_id: %s."), resource_id)
            bank_section.update_object("status",
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.DeleteBackupFailed(
                reason=err,
                resource_id=resource_id,
                resource_type=constants.SERVER_RESOURCE_TYPE)

    def get_supported_resources_types(self):
        return self._SUPPORT_RESOURCE_TYPES
