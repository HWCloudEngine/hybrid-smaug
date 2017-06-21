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

from oslo_config import cfg
from oslo_log import log as logging
from sgsclient import client as sc

from karbor.common import config
from karbor.services.protection.clients import utils

LOG = logging.getLogger(__name__)

SERVICE = "sgs"
sgs_client_opts = [
    cfg.StrOpt(SERVICE + '_endpoint',
               help='URL of the sgs endpoint.'),
    cfg.StrOpt(SERVICE + '_catalog_info',
               default='sg-service:sgservice:publicURL',
               help='Info to match when looking for sgs in the service '
               'catalog. Format is: separated values of the form: '
               '<service_type>:<service_name>:<endpoint_type> - '
               'Only used if sgs_endpoint is unset'),
    cfg.StrOpt(SERVICE + '_ca_cert_file',
               default=None,
               help='Location of the CA certificate file '
                    'to use for client requests in SSL connections.'),
    cfg.BoolOpt(SERVICE + '_auth_insecure',
                default=False,
                help='Bypass verification of server certificate when '
                     'making SSL connection to sgs.'),
]

CONFIG_GROUP = '%s_client' % SERVICE
CONF = cfg.CONF
CONF.register_opts(config.service_client_opts, group=CONFIG_GROUP)
CONF.register_opts(sgs_client_opts, group=CONFIG_GROUP)
CONF.set_default('service_name', 'sgservice', CONFIG_GROUP)
CONF.set_default('service_type', 'sg-service', CONFIG_GROUP)

SGSCLIENT_VERSION = '1'


def create(context, conf, **kwargs):
    conf.register_opts(sgs_client_opts, group=CONFIG_GROUP)

    client_config = conf[CONFIG_GROUP]
    url = utils.get_url(SERVICE, context, client_config,
                        append_project_fmt='%(url)s/%(project)s', **kwargs)
    LOG.debug('Creating sgs client with url %s.', url)

    if kwargs.get('session'):
        return sc.Client(SGSCLIENT_VERSION, session=kwargs.get('session'),
                         endpoint_override=url)

    args = {
        'input_auth_token': context.auth_token,
        'project_id': context.project_id,
        'service_catalog_url': url,
        'cacert': client_config.sgs_ca_cert_file,
        'insecure': client_config.sgs_auth_insecure,
    }
    client = sc.Client(SGSCLIENT_VERSION, **args)
    client.client.auth_token = context.auth_token
    client.client.management_url = url
    return client
