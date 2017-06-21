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

from oslo_config import cfg
from oslo_log import log as logging
from sgsclient import exceptions as sgs_exc

from karbor.common import constants
from karbor import exception
from karbor.services.protection.client_factory import ClientFactory
from karbor.services.protection import protection_plugin
from karbor.services.protection.protection_plugins.sgs \
    import volume_snapshot_plugin_schemas as snapshot_schemas
from karbor.services.protection.protection_plugins import utils

LOG = logging.getLogger(__name__)

sgs_snapshot_opts = [
    cfg.IntOpt(
        'poll_interval', default=15,
        help='Poll interval for SGS snapshot status'
    ),
]


def get_snapshot_status(sgs_client, snapshot_id):
    return get_resource_status(sgs_client.snapshots, snapshot_id, 'snapshot')


def get_checkpoint_status(sgs_client, checkpoint_id):
    return get_resource_status(sgs_client.checkpoints, checkpoint_id,
                               'checkpoint')


def get_volume_status(sgs_client, volume_id):
    return get_resource_status(sgs_client.volumes, volume_id, 'volume')


def get_cinder_volume_status(cinder_client, volume_id):
    return get_resource_status(cinder_client.volumes, volume_id, 'volume')


def get_resource_status(resource_manager, resource_id, resource_type):
    LOG.debug('Polling %(resource_type)s (id: %(resource_id)s)', {
        'resource_type': resource_type,
        'resource_id': resource_id,
    })
    try:
        resource = resource_manager.get(resource_id)
        status = resource.status
    except sgs_exc.NotFound:
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

    def _create_snapshot(self, sgs_client, volume_id, name, description):
        snapshot = sgs_client.snapshots.create(
            volume_id=volume_id,
            name=name,
            description=description)

        snapshot_id = snapshot.id
        is_success = utils.status_poll(
            partial(get_snapshot_status, sgs_client, snapshot_id),
            interval=self._interval,
            success_statuses={'available'},
            failure_statuses={'error'},
            ignore_statuses={'creating'})

        if not is_success:
            try:
                snapshot = sgs_client.snapshots.get(snapshot_id)
            except Exception:
                reason = 'Unable to find sgs snapshot'
            else:
                reason = 'SGS snapshot is in erroneous ' \
                         'state: %s' % snapshot.status
            raise Exception(reason)

        return snapshot_id

    def _create_checkpoint(self, sgs_client, replication_id, name,
                           description):
        checkpoint = sgs_client.checkpoints.create(
            replication_id=replication_id,
            name=name,
            description=description)

        checkpoint_id = checkpoint.id
        is_success = utils.status_poll(
            partial(get_checkpoint_status, sgs_client, checkpoint_id),
            interval=self._interval,
            success_statuses={'available'},
            failure_statuses={'error'},
            ignore_statuses={'creating'})

        if not is_success:
            try:
                checkpoint = sgs_client.checkpoints.get(checkpoint)
            except Exception:
                reason = 'Unable to find remote sgs snapshot(checkpoint)'
            else:
                reason = 'SGS remote snapshot(checkpoint) is in erroneous ' \
                         'state: %s' % checkpoint.status
            raise Exception(reason)

        return checkpoint_id

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        volume_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(volume_id)
        sgs_client = ClientFactory.create_client('sgs', context)
        LOG.info('creating sgs snapshot, volume_id: %s', volume_id)
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
                reason='Volume is in erroneous state',
                resource_id=volume_id,
                resource_type=constants.SGVOLUME_RESOURCE_TYPE,
            )

        snapshot_name = parameters.get('snapshot_name', None)
        description = parameters.get('description', None)
        snapshot_destination = parameters.get('snapshot_destination', "local")
        sgs_volume = sgs_client.volumes.get(volume_id)
        if sgs_volume.replication_id is None:
            snapshot_destination = 'local'
        resource_metadata['availability_zone'] = sgs_volume.availability_zone
        resource_metadata['replication_zone'] = sgs_volume.replication_zone

        if snapshot_destination == 'local':
            try:
                snapshot_id = self._create_snapshot(sgs_client, volume_id,
                                                    name=snapshot_name,
                                                    description=description)
            except Exception as e:
                LOG.error('Error creating sgs snapshot '
                          'volume_id:%(volume_id)s, reason:%(reason)s',
                          {'volume_id': volume_id,
                           'reason': e})
                bank_section.update_object('status',
                                           constants.RESOURCE_STATUS_ERROR)
                raise exception.CreateResourceFailed(
                    reason=e,
                    resource_id=volume_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            resource_metadata['snapshot_id'] = snapshot_id
        else:
            try:
                checkpoint_id = self._create_checkpoint(
                    sgs_client, sgs_volume.replication_id, name=snapshot_name,
                    description=description)
            except Exception as e:
                LOG.error('Error creating sgs remote snapshot(checkpoint) '
                          'volume_id:%(volume_id)s, reason:%(reason)s',
                          {'volume_id': volume_id,
                           'reason': e})
                bank_section.update_object('status',
                                           constants.RESOURCE_STATUS_ERROR)
                raise exception.CreateResourceFailed(
                    reason=e,
                    resource_id=volume_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            resource_metadata['checkpoint_id'] = checkpoint_id
        volume_info = sgs_client.vollumes.get(volume_id)
        if volume_info.status == 'in-use':
            resource_metadata['attached_instance'] \
                = volume_info['attachments'][0]['server_id']
        bank_section.update_object('metadata', resource_metadata)
        bank_section.update_object('status',
                                   constants.RESOURCE_STATUS_AVAILABLE)
        LOG.info('Backed up sgs volume volume_id:%(volume_id)s successfully',
                 {'volume_id': volume_id})


class RestoreOperation(protection_plugin.Operation):
    def __init__(self, poll_interval):
        super(RestoreOperation, self).__init__()
        self._interval = poll_interval

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        resource_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(resource_id)
        resource_metadata = bank_section.get_object('metadata')
        sgs_client = ClientFactory.create_client('sgs', context)

        restore_reference = kwargs.get('restore_reference')
        restore_name = parameters.get('restore_name',
                                      'karbor-restore-%s' % resource_id)
        volume_type = parameters.get('volume_type', None)
        description = parameters.get('description', None)
        snapshot_id = resource_metadata.get('snapshot_id', None)

        if snapshot_id:
            availability_zone = resource_metadata['availability_zone']
            try:
                volume = sgs_client.volumes.create(
                    snapshot_id=snapshot_id,
                    name=restore_name,
                    volume_type=volume_type,
                    availability_zone=availability_zone,
                    description=description)
            except Exception as ex:
                LOG.error('Error restoring from snapshot '
                          '(snapshot_id: %(snapshot_id)s): %(reason)s',
                          {'snapshot_id': snapshot_id,
                           'reason': ex})
                raise
        else:
            replication_zone = resource_metadata['replication_zone']
            checkpoint_id = resource_metadata.get('checkpoint_id', None)
            try:
                volume = sgs_client.volumes.create(
                    checkpoint_id=checkpoint_id,
                    name=restore_name,
                    volume_type=volume_type,
                    availability_zone=replication_zone,
                    description=description)
            except Exception as ex:
                LOG.error('Error restoring from snapshot '
                          '(snapshot_id: %(snapshot_id)s): %(reason)s',
                          {'snapshot_id': snapshot_id,
                           'reason': ex})
                raise

        # check and update status
        update_method = partial(
            utils.update_resource_restore_result,
            kwargs.get('restore'), resource.type, volume.id)

        update_method(constants.RESOURCE_STATUS_RESTORING)

        is_success = self._check_create_complete(sgs_client, volume.id)
        if not is_success:
            reason = 'Error restoring sgs volume from snapshot'
            update_method(constants.RESOURCE_STATUS_ERROR, reason)

            raise exception.RestoreBackupFailed(
                reason=reason,
                resource_id=resource_id,
                resource_type=resource.type)

        try:
            sgs_client.volumes.enable(volume.id)
        except Exception as ex:
            LOG.error('Error restoring from snapshot: %(reason)s',
                      {'reason': ex})
            raise
        is_success = self._check_enable_complete(sgs_client, volume.id)
        if not is_success:
            reason = 'Error restoring volume'
            update_method(constants.RESOURCE_STATUS_ERROR, reason)

            raise exception.RestoreResourceFailed(
                reason=reason,
                resource_id=resource_id,
                resource_type=resource.type)

        attached_instance = resource_metadata.get('attached_instance')
        if attached_instance:
            utils.reference_poll(self._interval, restore_reference,
                                 attached_instance)
            instance = restore_reference.get_reference(
                attached_instance)
            sgs_client.volumes.attach(volume.id, instance)
            is_success = self._check_attach_complete(sgs_client, volume.id)
            if not is_success:
                reason = 'Error restoring volume'
                update_method(constants.RESOURCE_STATUS_ERROR, reason)

                raise exception.RestoreBackupFailed(
                    reason=reason,
                    resource_id=resource_id,
                    resource_type=resource.type)

        update_method(constants.RESOURCE_STATUS_AVAILABLE)
        if is_success:
            update_method(constants.RESOURCE_STATUS_AVAILABLE)
            restore_reference.put_resource(resource_id, volume.id)
            return

    def _check_create_complete(self, sgs_client, volume_id):
        return utils.status_poll(
            partial(get_volume_status, sgs_client, volume_id),
            interval=self._interval,
            success_statuses={'available'},
            failure_statuses={'error', 'not-found'},
            ignore_statuses={'restoring_backup'})

    def _check_enable_complete(self, sgs_client, volume_id):
        return utils.status_poll(
            partial(get_volume_status, sgs_client, volume_id),
            interval=self._interval,
            success_statuses={'enabled'},
            failure_statuses={'error', 'not-found'},
            ignore_statuses={'enabling'})

    def _check_attach_complete(self, sgs_client, volume_id):
        return utils.status_poll(
            partial(get_volume_status, sgs_client, volume_id),
            interval=self._interval,
            success_statuses={'in-use'},
            failure_statuses={'error', 'not-found', 'enabled',
                              'error_attaching'},
            ignore_statuses={'attaching'})


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
        except Exception:
            bank_section.delete_object('metadata')
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETED)
            return

        if resource_metadata is not None:
            try:
                bank_section.update_object('status',
                                           constants.RESOURCE_STATUS_DELETING)
                sgs_client = ClientFactory.create_client('sgs', context)
                snapshot_id = resource_metadata.get('snapshot_id', None)
                if snapshot_id:
                    try:
                        sgs_client.snapshots.delete(snapshot_id)
                    except sgs_exc.NotFound:
                        LOG.info('Snapshot id: %s not found. Assuming deleted',
                                 snapshot_id)
                    is_success = utils.status_poll(
                        partial(get_snapshot_status, sgs_client, snapshot_id),
                        interval=self._interval,
                        success_statuses={'deleted', 'not-found'},
                        failure_statuses={'error', 'error_deleting'},
                        ignore_statuses={'deleting'})
                    if not is_success:
                        raise exception.NotFound()
                else:
                    checkpoint_id = resource_metadata.get('checkpoint_id')
                    try:
                        sgs_client.checkpoints.delete(checkpoint_id)
                    except sgs_exc.NotFound:
                        LOG.info('Checkpoint id: %s not found. Assuming '
                                 'deleted', checkpoint_id)
                    is_success = utils.status_poll(
                        partial(get_checkpoint_status, sgs_client,
                                checkpoint_id),
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
                LOG.error('delete volume snapshot/checkpoint failed')
                bank_section.update_object('status',
                                           constants.RESOURCE_STATUS_ERROR)
                raise exception.DeleteBackupFailed(
                    reason=six.text_type(e),
                    resource_id=resource_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE)
        else:
            bank_section.delete_object('metadata')
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETED)


class SGSBackupProtectionPlugin(protection_plugin.ProtectionPlugin):
    _SUPPORT_RESOURCE_TYPES = [constants.SGVOLUME_RESOURCE_TYPE]

    def __init__(self, config=None):
        super(SGSBackupProtectionPlugin, self).__init__(config)
        self._config.register_opts(sgs_snapshot_opts,
                                   'sgs_snapshot_protection_plugin')
        self._plugin_config = self._config.sgs_backup_protection_plugin
        self._poll_interval = self._plugin_config.poll_interval

    @classmethod
    def get_supported_resources_types(cls):
        return cls._SUPPORT_RESOURCE_TYPES

    @classmethod
    def get_options_schema(cls, resources_type):
        return snapshot_schemas.OPTIONS_SCHEMA

    @classmethod
    def get_restore_schema(cls, resources_type):
        return snapshot_schemas.RESTORE_SCHEMA

    @classmethod
    def get_saved_info_schema(cls, resources_type):
        return snapshot_schemas.SAVED_INFO_SCHEMA

    @classmethod
    def get_saved_info(cls, metadata_store, resource):
        pass

    def get_protect_operation(self, resource):
        return ProtectOperation(self._poll_interval)

    def get_restore_operation(self, resource):
        return RestoreOperation(self._poll_interval)

    def get_delete_operation(self, resource):
        return DeleteOperation(self._poll_interval)
