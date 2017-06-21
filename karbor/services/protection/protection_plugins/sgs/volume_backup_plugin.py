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

from functools import partial
import six

from cinderclient import exceptions as c_exc
from oslo_config import cfg
from oslo_log import log as logging
from sgsclient import exceptions as sgs_exc

from karbor.common import constants
from karbor import exception
from karbor.services.protection.client_factory import ClientFactory
from karbor.services.protection import protection_plugin
from karbor.services.protection.protection_plugins.sgs \
    import volume_backup_plugin_schemas as backup_schemas
from karbor.services.protection.protection_plugins import utils

LOG = logging.getLogger(__name__)

sgs_backup_opts = [
    cfg.IntOpt(
        'poll_interval', default=15,
        help='Poll interval for SGS backup status'
    ),
]


def get_backup_status(sgs_client, backup_id):
    return get_resource_status(sgs_client.backups, backup_id, 'backup')


def get_volume_status(client, volume_id):
    return get_resource_status(client.volumes, volume_id, 'volume')


def get_resource_status(resource_manager, resource_id, resource_type):
    LOG.debug('Polling %(resource_type)s (id: %(resource_id)s)', {
        'resource_type': resource_type,
        'resource_id': resource_id,
    })
    try:
        resource = resource_manager.get(resource_id)
        status = resource.status
    except (sgs_exc.NotFound, c_exc.NotFound):
        status = 'not-found'
    LOG.debug(
        'Polled %(resource_type)s (id: %(resource_id)s) status: %(status)s',
        {
            'resource_type': resource_type,
            'resource_id': resource_id,
            'status': status
        }
    )
    return status


class ProtectOperation(protection_plugin.Operation):
    def __init__(self, poll_interval):
        super(ProtectOperation, self).__init__()
        self._interval = poll_interval

    def _create_backup(self, sgs_client, volume_id, backup_name,
                       description, backup_destination='local',
                       backup_type='full'):
        backup = sgs_client.backups.create(
            volume_id=volume_id,
            name=backup_name,
            description=description,
            destination=backup_destination,
            type=backup_type
        )

        backup_id = backup.id
        is_success = utils.status_poll(
            partial(get_backup_status, sgs_client, backup_id),
            interval=self._interval,
            success_statuses={'available'},
            failure_statuses={'error'},
            ignore_statuses={'creating'})

        if not is_success:
            try:
                backup = sgs_client.backups.get(backup_id)
            except Exception:
                reason = 'Unable to find sgs backup'
            else:
                reason = 'SGS Backup is in erroneous state: %s' % backup.status
            raise Exception(reason)

        return backup_id

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        volume_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(volume_id)
        sgs_client = ClientFactory.create_client('sgs', context)
        LOG.info('creating volume backup, volume_id: %s', volume_id)
        bank_section.update_object('status',
                                   constants.RESOURCE_STATUS_PROTECTING)
        resource_metadata = {
            'volume_id': volume_id,
        }
        is_success = utils.status_poll(
            partial(get_volume_status, sgs_client, volume_id),
            interval=self._interval,
            success_statuses={'enabled', 'in-use'},
            failure_statuses={'error', 'error_deleting', 'deleting',
                              'not-found', 'error_detaching',
                              'error_attaching', },
            ignore_statuses={'attaching', 'detaching', 'enabling',
                             'backing-up', 'restoring_backup'},
        )
        if not is_success:
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.CreateResourceFailed(
                reason='SGS Volume is in erroneous state',
                resource_id=volume_id,
                resource_type=constants.SGVOLUME_RESOURCE_TYPE,
            )

        backup_name = parameters.get('backup_name', None)
        if backup_name is None:
            backup_name = "karbor-backup-sgs-volume-%s-%s" % (
                volume_id, checkpoint.id)
        description = parameters.get('description', None)
        backup_type = parameters.get('backup_type', "full")
        backup_destination = parameters.get('backup_destination', "local")
        try:
            sgs_volume = sgs_client.volumes.get(volume_id)
            if backup_destination == 'local':
                availability_zone = sgs_volume.availability_zone
            else:
                availability_zone = sgs_volume.replication_zone

            backup_id = self._create_backup(sgs_client, volume_id,
                                            backup_name, description,
                                            backup_destination, backup_type)
        except Exception as e:
            LOG.error('Error creating sgs backup volume_id: %(volume_id)s, '
                      'reason:%(reason)s',
                      {'volume_id': volume_id,
                       'reason': e})
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.CreateResourceFailed(
                reason=e,
                resource_id=volume_id,
                resource_type=constants.SGVOLUME_RESOURCE_TYPE,
            )

        resource_metadata['backup_id'] = backup_id
        resource_metadata['destination'] = backup_destination
        resource_metadata['availability_zone'] = availability_zone
        bank_section.update_object('metadata', resource_metadata)
        bank_section.update_object('status',
                                   constants.RESOURCE_STATUS_AVAILABLE)
        LOG.info('Backed up sgs volume volume_id:%(volume_id)s, '
                 'backup_id: %(backup_id)s) successfully',
                 {'backup_id': backup_id,
                  'volume_id': volume_id})


class RestoreOperation(protection_plugin.Operation):
    def __init__(self, poll_interval):
        super(RestoreOperation, self).__init__()
        self._interval = poll_interval

    def _create_volume(self, cinder_client, name, description, size,
                       volume_type, availability_zone):
        volume = cinder_client.volumes.create(
            name=name,
            description=description,
            size=size,
            volume_type=volume_type,
            availability_zone=availability_zone)
        volume_id = volume.id

        is_success = utils.status_poll(
            partial(get_volume_status, cinder_client, volume_id),
            interval=self._interval,
            success_statuses={'available', },
            failure_statuses={'error', 'not-found'},
            ignore_statuses={'creating', },
        )
        if not is_success:
            try:
                volume = cinder_client.volumes.get(volume_id)
            except Exception:
                reason = 'Unable to find created cinder volume'
            else:
                reason = 'Volume is in erroneous state: %s' % volume.status
            raise Exception(reason)

        return volume_id

    def on_prepare_finish(self, checkpoint, resource, context, parameters,
                          **kwargs):
        resource_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(resource_id)
        resource_metadata = bank_section.get_object('metadata')
        sgs_client = ClientFactory.create_client('sgs', context)
        cinder_client = ClientFactory.create_client('cinder', context)
        backup_id = resource_metadata['backup_id']
        restore_name = parameters.get('restore_name', None)
        restore = kwargs.get('restore')
        if restore_name is None:
            restore_name = 'karbor-restore-sgs-volume-%s-%s' % (
                resource_id, restore.id)

        if 'volume_id' in parameters:
            self.volume_id = parameters.get('volume_id')
        else:
            volume_property = {
                'name': restore_name,
                'volume_type': parameters.get('volume_type', None),
                'description': parameters.get('description', None)
            }
            try:
                backup = sgs_client.backups.get(backup_id)
                if backup.destination == 'local':
                    availability_zone = backup.availability_zone
                else:
                    availability_zone = backup.replication_zone
                volume_property['size'] = backup.size
                volume_property['availability_zone'] = availability_zone
                self.volume_id = self._create_volume(cinder_client,
                                                     **volume_property)
            except Exception:
                raise exception.RestoreResourceFailed(
                    reason='Error creating new cinder volume',
                    resource_id=resource_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE)

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        resource_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(resource_id)
        resource_metadata = bank_section.get_object('metadata')
        sgs_client = ClientFactory.create_client('sgs', context)

        backup_id = resource_metadata['backup_id']
        backup_destination = resource_metadata['destination']
        restore_reference = kwargs.get('restore_reference')
        # check and update status
        update_method = partial(
            utils.update_resource_restore_result,
            kwargs.get('restore'), resource.type, self.volume_id)

        update_method(constants.RESOURCE_STATUS_RESTORING)

        import_backup = None
        try:
            if backup_destination == 'remote':
                backup_record = sgs_client.backups.export_record(backup_id)
                backup = sgs_client.backups.import_record(backup_record._info)
                import_backup = backup
                sgs_client.backups.restore(backup.id, self.volume_id)
            else:
                sgs_client.backups.restore(backup_id, self.volume_id)
        except Exception as ex:
            reason = 'Error restoring backup (backup_id: %(backup_id)s): ' \
                     '%(reason)s' % {'backup_id': backup_id,
                                     'reason': ex}
            LOG.error(reason)
            update_method(constants.RESOURCE_STATUS_ERROR, reason)
            raise exception.RestoreResourceFailed(
                reason=reason,
                resource_id=resource_id,
                resource_type=resource.type)

        is_success = self._check_restore_complete(sgs_client, self.volume_id)
        if not is_success:
            reason = 'Error restoring volume'
            update_method(constants.RESOURCE_STATUS_ERROR, reason)

            raise exception.RestoreResourceFailed(
                reason=reason,
                resource_id=resource_id,
                resource_type=resource.type)
        if import_backup:
            try:
                sgs_client.backups.delete(import_backup.id)
            except sgs_exc.NotFound:
                LOG.info('Backup id: %s not found. '
                         'Assuming deleted', backup_id)
            utils.status_poll(
                partial(get_backup_status, sgs_client,
                        import_backup.id),
                interval=self._interval,
                success_statuses={'deleted', 'not-found'},
                failure_statuses={'error', 'error_deleting'},
                ignore_statuses={'deleting'})
        try:
            sgs_client.volumes.enable(self.volume_id)
        except Exception as ex:
            reason = 'Error enable restored-volume:%(volume_id)s): ' \
                     '%(reason)s' % {'volume_id': self.volume_id,
                                     'reason': ex}
            LOG.error(reason)
            update_method(constants.RESOURCE_STATUS_ERROR, reason)
            raise exception.RestoreResourceFailed(
                reason=reason,
                resource_id=resource_id,
                resource_type=resource.type)

        is_success = self._check_enable_complete(sgs_client,
                                                 self.volume_id)
        if not is_success:
            reason = 'Error enable restored-volume'
            update_method(constants.RESOURCE_STATUS_ERROR, reason)
            raise exception.RestoreResourceFailed(
                reason=reason,
                resource_id=resource_id,
                resource_type=resource.type)

        update_method(constants.RESOURCE_STATUS_AVAILABLE)
        restore_reference.put_resource(resource_id,
                                       self.volume_id)

    def _check_restore_complete(self, sgs_client, volume_id):
        return utils.status_poll(
            partial(get_volume_status, sgs_client, volume_id),
            interval=self._interval,
            success_statuses={'available'},
            failure_statuses={'error_restoring', 'not-found', 'error'},
            ignore_statuses={'restoring_backup'})

    def _check_enable_complete(self, sgs_client, volume_id):
        return utils.status_poll(
            partial(get_volume_status, sgs_client, volume_id),
            interval=self._interval,
            success_statuses={'enabled'},
            failure_statuses={'error', 'not-found'},
            ignore_statuses={'enabling'})


class DeleteOperation(protection_plugin.Operation):
    def __init__(self, poll_interval):
        super(DeleteOperation, self).__init__()
        self._interval = poll_interval
        self.volume_id = None

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        resource_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(resource_id)
        try:
            resource_metadata = bank_section.get_object('metadata')
            if resource_metadata is None:
                raise
        except Exception:
            bank_section.delete_object('metadata')
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETED)
            return

        backup_id = None
        try:
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETING)
            backup_id = resource_metadata['backup_id']
            sgs_client = ClientFactory.create_client('sgs', context)
            try:
                sgs_client.backups.delete(backup_id)
            except sgs_exc.NotFound:
                LOG.info('Backup id: %s not found. Assuming deleted',
                         backup_id)
            is_success = utils.status_poll(
                partial(get_backup_status, sgs_client, backup_id),
                interval=self._interval,
                success_statuses={'deleted', 'not-found'},
                failure_statuses={'error', 'error_deleting'},
                ignore_statuses={'deleting'})
            if not is_success:
                raise exception.NotFound()
            bank_section.delete_object('metadata')
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETED)
        except Exception as e:
            LOG.error('delete sgs backup failed, backup_id: %s',
                      backup_id)
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.DeleteResourceFailed(
                reason=six.text_type(e),
                resource_id=resource_id,
                resource_type=constants.SGVOLUME_RESOURCE_TYPE)


class SGSBackupProtectionPlugin(protection_plugin.ProtectionPlugin):
    _SUPPORT_RESOURCE_TYPES = [constants.SGVOLUME_RESOURCE_TYPE]

    def __init__(self, config=None):
        super(SGSBackupProtectionPlugin, self).__init__(config)
        self._config.register_opts(sgs_backup_opts,
                                   'sgs_backup_protection_plugin')
        self._plugin_config = self._config.sgs_backup_protection_plugin
        self._poll_interval = self._plugin_config.poll_interval

    @classmethod
    def get_supported_resources_types(cls):
        return cls._SUPPORT_RESOURCE_TYPES

    @classmethod
    def get_options_schema(cls, resources_type):
        return backup_schemas.OPTIONS_SCHEMA

    @classmethod
    def get_restore_schema(cls, resources_type):
        return backup_schemas.RESTORE_SCHEMA

    @classmethod
    def get_saved_info_schema(cls, resources_type):
        return backup_schemas.SAVED_INFO_SCHEMA

    @classmethod
    def get_saved_info(cls, metadata_store, resource):
        pass

    def get_protect_operation(self, resource):
        return ProtectOperation(self._poll_interval)

    def get_restore_operation(self, resource):
        return RestoreOperation(self._poll_interval)

    def get_delete_operation(self, resource):
        return DeleteOperation(self._poll_interval)
