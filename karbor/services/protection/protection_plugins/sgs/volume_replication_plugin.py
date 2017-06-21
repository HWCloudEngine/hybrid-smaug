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
    import volume_replication_plugin_schemas as replication_schemas
from karbor.services.protection.protection_plugins import utils

LOG = logging.getLogger(__name__)

sgs_replication_opts = [
    cfg.IntOpt(
        'poll_interval', default=15,
        help='Poll interval for SGS replication status'
    ),
]


def get_replication_status(sgs_client, replication_id):
    return get_resource_status(sgs_client.replications,
                               replication_id, 'replication')


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

    def _create_cinder_volume(self, cinder_client, availability_zone, name,
                              size, volume_type):
        volume = cinder_client.volumes.create(
            availability_zone=availability_zone,
            name=name, size=size, volume_type=volume_type)

        volume_id = volume.id
        is_success = utils.status_poll(
            partial(get_volume_status, cinder_client, volume_id),
            interval=self._interval,
            success_statuses={'available'},
            failure_statuses={'error'},
            ignore_statuses={'creating'})

        if not is_success:
            try:
                volume = cinder_client.volumes.get(volume_id)
            except Exception:
                reason = 'Unable to find cinder volume'
            else:
                reason = 'Cinder volume is in erroneous ' \
                         'state: %s' % volume.status
            raise Exception(reason)

        return volume_id

    def on_main(self, checkpoint, resource, context, parameters, **kwargs):
        volume_id = resource.id
        bank_section = checkpoint.get_resource_bank_section(volume_id)
        sgs_client = ClientFactory.create_client('sgs', context)
        cinder_client = ClientFactory.create_client('cinder', context)
        LOG.info('creating sgs replication, volume_id: %s', volume_id)
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

        replicate_volume = parameters.get('replicate_volume', None)
        sgs_volume = sgs_client.volumes.get(volume_id)
        if replicate_volume is None:
            volume_type = parameters.get('volume_type', None)
            replicate_name = parameters.get('replicate_name', None)
            if replicate_name is None:
                replicate_name = 'karbor-replicate-sgs-volume-%s-%s' % (
                    volume_id, checkpoint.id)
            replicate_volume = self._create_cinder_volume(
                cinder_client, sgs_volume.replication_zone,
                replicate_name, sgs_volume.size, volume_type)
            sgs_client.volumes.enable(replicate_volume)
        is_success = utils.status_poll(
            partial(get_volume_status, sgs_client, replicate_volume),
            interval=self._interval,
            success_statuses={'enabled'},
            failure_statuses={'error', 'error_deleting', 'deleting',
                              'not-found', 'error_detaching',
                              'error_attaching', 'in-use'},
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

        replication_name = parameters.get("replication_name", None)
        if replication_name is None:
            replication_name = "karbor-replication-sgs-volume-%s-%s" % (
                volume_id, checkpoint.id)
        replication = sgs_client.replications.create(
            volume_id, replicate_volume, name=replication_name)
        replication_id = replication.id
        is_success = utils.status_poll(
            partial(get_replication_status, sgs_client, replication_id),
            interval=self._interval,
            success_statuses={'enabled'},
            failure_statuses={'error', 'not-found',},
            ignore_statuses={'creating', 'enabling'},
        )
        if not is_success:
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.CreateResourceFailed(
                reason='Create failed. Replication is in erroneous state',
                resource_id=volume_id,
                resource_type=constants.SGVOLUME_RESOURCE_TYPE,
            )

        resource_metadata['replication_id'] = replication_id
        resource_metadata['replicate_volume'] = replicate_volume
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
        sgs_client = ClientFactory.create_client('sgs', context)

        restore_reference = kwargs.get('restore_reference')
        resource_metadata = bank_section.get_object('metadata')
        replicate_volume = resource_metadata['replicate_volume']
        # check and update status
        update_method = partial(
            utils.update_resource_restore_result,
            kwargs.get('restore'), resource.type, replicate_volume)
        update_method(constants.RESOURCE_STATUS_RESTORING)
        try:
            replication_id = resource_metadata['replication_id']
            is_force = parameters.get('is_force', False)
            replication = sgs_client.replications.get(replication_id)
            if replication.status != 'enabled':
                reason = 'Get replication failed. ' \
                         'Replication is in erroneous state'
                update_method(constants.RESOURCE_STATUS_ERROR, reason)
                raise exception.RestoreResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            sgs_client.replications.failover(replication_id, force=is_force)
            is_success = utils.status_poll(
                partial(get_replication_status, sgs_client, replication_id),
                interval=self._interval,
                success_statuses={'failed-over'},
                failure_statuses={'error', 'not-found'},
                ignore_statuses={'failing-over'},
            )
            if not is_success:
                reason = 'Failover replication failed. ' \
                         'Replication is in erroneous state'
                update_method(constants.RESOURCE_STATUS_ERROR, reason)
                raise exception.RestoreResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            sgs_client.replications.reverse(replication_id)
            is_success = utils.status_poll(
                partial(get_replication_status, sgs_client, replication_id),
                interval=self._interval,
                success_statuses={'disabled'},
                failure_statuses={'error', 'not-found'},
                ignore_statuses={'reversing'},
            )
            if not is_success:
                reason = 'Reverse replication failed. ' \
                         'Replication is in erroneous state'
                update_method(constants.RESOURCE_STATUS_ERROR, reason)
                raise exception.RestoreResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            sgs_client.replications.delete(replication_id)
            is_success = utils.status_poll(
                partial(get_replication_status, sgs_client, replication_id),
                interval=self._interval,
                success_statuses={'deleted', 'not-found'},
                failure_statuses={'error'},
                ignore_statuses={'deleting'},
            )
            if not is_success:
                reason = 'Delete replication failed. ' \
                         'Replication is in erroneous state'
                update_method(constants.RESOURCE_STATUS_ERROR, reason)
                raise exception.RestoreResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            update_method(constants.RESOURCE_STATUS_AVAILABLE)
            restore_reference.put_resource(resource_id, replicate_volume)
            return
        except Exception as ex:
            reason = "Restore failed, err: %s" % ex
            update_method(constants.RESOURCE_STATUS_ERROR, reason)
            raise exception.RestoreResourceFailed(
                    reason=ex,
                    resource_id=resource_id,
                    resource_type=resource.type)


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

        try:
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETING)
            replication_id = resource_metadata['replication_id']
            sgs_client = ClientFactory.create_client('sgs', context)
            replication = sgs_client.replications.get(replication_id)
        except sgs_exc.NotFound:
            bank_section.delete_object('metadata')
            bank_section.update_object(
                'status', constants.RESOURCE_STATUS_DELETED)
            return
        if replication.status != 'enabled':
            bank_section.delete_object('metadata')
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETED)
            return

        try:
            sgs_client.replications.disable(replication_id)
            is_success = utils.status_poll(
                partial(get_replication_status, sgs_client, replication_id),
                interval=self._interval,
                success_statuses={'disabled'},
                failure_statuses={'error', 'not-found'},
                ignore_statuses={'disabling'},
            )
            if not is_success:
                reason = 'Disable replication failed. ' \
                         'Replication is in erroneous state'
                raise exception.DeleteResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            sgs_client.replications.delete(replication_id)
            is_success = utils.status_poll(
                partial(get_replication_status, sgs_client, replication_id),
                interval=self._interval,
                success_statuses={'deleted', 'not-found'},
                failure_statuses={'error'},
                ignore_statuses={'deleting'},
            )
            if not is_success:
                reason = 'Delete replication failed. ' \
                         'Replication is in erroneous state'
                raise exception.DeleteResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            replicate_volume = resource_metadata['replicate_volume']
            sgs_client.volumes.disable(replicate_volume)
            is_success = utils.status_poll(
                partial(get_volume_status, sgs_client, replicate_volume),
                interval=self._interval,
                success_statuses={'disabled', 'not-found'},
                failure_statuses={'error'},
                ignore_statuses={'disabling'},
            )
            if not is_success:
                reason = 'Disable sgs volume failed. ' \
                         'SGS volume is in erroneous state'
                raise exception.DeleteResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            cinder_client = ClientFactory.create_client('cinder', context)
            cinder_client.volumes.delete(replicate_volume)
            is_success = utils.status_poll(
                partial(get_volume_status, cinder_client, replicate_volume),
                interval=self._interval,
                success_statuses={'deleted', 'not-found'},
                failure_statuses={'error'},
                ignore_statuses={'deleting'},
            )
            if not is_success:
                reason = 'Disable cinder volume failed. ' \
                         'Cinder volume is in erroneous state'
                raise exception.DeleteResourceFailed(
                    reason=reason,
                    resource_id=replication_id,
                    resource_type=constants.SGVOLUME_RESOURCE_TYPE,
                )
            bank_section.delete_object('metadata')
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_DELETED)
            return
        except Exception as e:
            LOG.error('Delete resource failed, error:%s' % e)
            bank_section.update_object('status',
                                       constants.RESOURCE_STATUS_ERROR)
            raise exception.DeleteResourceFailed(
                reason=six.text_type(e),
                resource_id=resource_id,
                resource_type=constants.SGVOLUME_RESOURCE_TYPE)


class SGSReplicationProtectionPlugin(protection_plugin.ProtectionPlugin):
    _SUPPORT_RESOURCE_TYPES = [constants.SGVOLUME_RESOURCE_TYPE]

    def __init__(self, config=None):
        super(SGSReplicationProtectionPlugin, self).__init__(config)
        self._config.register_opts(sgs_replication_opts,
                                   'sgs_replication_protection_plugin')
        self._plugin_config = self._config.sgs_replication_protection_plugin
        self._poll_interval = self._plugin_config.poll_interval

    @classmethod
    def get_supported_resources_types(cls):
        return cls._SUPPORT_RESOURCE_TYPES

    @classmethod
    def get_options_schema(cls, resources_type):
        return replication_schemas.OPTIONS_SCHEMA

    @classmethod
    def get_restore_schema(cls, resources_type):
        return replication_schemas.RESTORE_SCHEMA

    @classmethod
    def get_saved_info_schema(cls, resources_type):
        return replication_schemas.SAVED_INFO_SCHEMA

    @classmethod
    def get_saved_info(cls, metadata_store, resource):
        pass

    def get_protect_operation(self, resource):
        return ProtectOperation(self._poll_interval)

    def get_restore_operation(self, resource):
        return RestoreOperation(self._poll_interval)

    def get_delete_operation(self, resource):
        return DeleteOperation(self._poll_interval)
