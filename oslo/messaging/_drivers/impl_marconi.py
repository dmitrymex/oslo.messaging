# Copyright (c) 2014 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import logging
import Queue
import threading
import time
import uuid

from marconiclient.queues import client
from oslo.config import cfg

from oslo.messaging._drivers import base
from oslo.messaging._drivers import common as rpc_common
from oslo.messaging._executors import impl_eventlet
from oslo.messaging.openstack.common import jsonutils


marconi_opts = [
    cfg.StrOpt('marconi_url',
               help='A URL pointing to Marconi service.'),
    cfg.StrOpt('keystone_url', default='...',
               help='A URL pointing to Keystone Auth'),
    cfg.StrOpt('m_username', default='...'),
    cfg.StrOpt('m_password', default='...'),
    cfg.StrOpt('m_project_name', default='...')
]


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


CLIENT_OPTS_TEMPLATE = {'auth_opts': {'backend': 'keystone', 'options': {}}}


tenant_init_lock = threading.Lock()
tenant_lock = {}
tenant_awaiters = {}
tenant_poller = {}

tenant_reply_q = {}


def _get_envelope(marconi, queue_name):
    LOG.debug('Getting envelope from %s' % queue_name)

    q = marconi.queue(queue_name)
    q.ensure_exists()

    try:
        msg = next(q.messages())
        msg.delete()

        envelope = jsonutils.loads(msg.body)
        return rpc_common.deserialize_msg(envelope)
    except StopIteration:
        return None


def _post_envelope(marconi, envelope, queue_name):
    LOG.debug('Posting envelope to %s' % queue_name)

    envelope = rpc_common.serialize_msg(envelope)
    envelope = jsonutils.dumps(envelope)

    q = marconi.queue(queue_name)
    q.ensure_exists()
    q.post({'ttl': 60, 'body': envelope})


def _marconi_client_with_conf():
    conf = copy.deepcopy(CLIENT_OPTS_TEMPLATE)

    opts = conf['auth_opts']['options']
    opts['os_username'] = CONF.m_username
    opts['os_password'] = CONF.m_password
    opts['os_project_name'] = CONF.m_project_name
    opts['os_auth_url'] = CONF.keystone_url

    return client.Client(CONF.marconi_url, 1, conf=conf)


def _marconi_client_with_token(token):
    conf = copy.deepcopy(CLIENT_OPTS_TEMPLATE)

    opts = conf['auth_opts']['options']
    opts['os_auth_token'] = token

    return client.Client(CONF.marconi_url, 1, conf=conf)


def _ensure_tenant_lock_exists(tenant_id):
    if tenant_id not in tenant_lock:
        with tenant_init_lock:
            if tenant_id not in tenant_lock:
                tenant_lock[tenant_id] = threading.Lock()
                tenant_awaiters[tenant_id] = {}
                tenant_poller[tenant_id] = None


def _get_reply_q(tenant_id):
    if tenant_id not in tenant_reply_q:
        with tenant_init_lock:
            if tenant_id not in tenant_reply_q:
                name = 'messaging_reply_%s' % uuid.uuid4().hex
                tenant_reply_q[tenant_id] = name

    return tenant_reply_q[tenant_id]


def _poll(token, tenant_id):
    while True:
        with tenant_lock[tenant_id]:
            if len(tenant_awaiters[tenant_id]) == 0:
                tenant_poller[tenant_id] = None
                return

        marconi = _marconi_client_with_token(token)

        envelope = _get_envelope(marconi, _get_reply_q(tenant_id))

        if envelope:
            msg_id = envelope['in_reply_to']

            LOG.info('Received reply to %s, type - %s' % (msg_id, type(msg_id)))

            queue = tenant_awaiters[tenant_id].get(msg_id)
            if queue:
                queue.put(envelope)
            else:
                LOG.info('[messaging.marconi] Nobody waits for '
                         'reply to %s' % msg_id)
        else:
            time.sleep(1)


def _wait_for_reply(msg_id, token, tenant_id, timeout):
    _ensure_tenant_lock_exists(tenant_id)

    LOG.debug('Waiting for reply to %s, type - %s' % (msg_id, type(msg_id)))

    with tenant_lock[tenant_id]:
        tenant_awaiters[tenant_id][msg_id] = Queue.Queue()

        if not tenant_poller[tenant_id]:
            try:
                t = threading.Thread(target=_poll,
                                     name='%s-messaging-poller' % tenant_id,
                                     args=(token, tenant_id))

                tenant_poller[tenant_id] = t

                t.start()
            except BaseException:
                del tenant_awaiters[tenant_id][msg_id]
                raise

    try:
        queue = tenant_awaiters[tenant_id][msg_id]
        envelope = queue.get(block=True, timeout=timeout)
    finally:
        with tenant_lock[tenant_id]:
            del tenant_awaiters[tenant_id][msg_id]

    if envelope['failure']:
        exc = rpc_common.deserialize_remote_exception(envelope['failure'])
        raise exc

    return envelope['body']


class MarconiIncomingMessage(base.IncomingMessage):

    def __init__(self, listener, ctxt, message, msg_id, reply_q):
        super(MarconiIncomingMessage, self).__init__(listener, ctxt, message)

        self.msg_id = msg_id
        self.reply_q = reply_q

    def reply(self, reply=None, failure=None, log_failure=True):
        marconi = _marconi_client_with_conf()

        if failure:
            failure = rpc_common.serialize_remote_exception(failure,
                                                            log_failure)

        envelope = {
            'in_reply_to': self.msg_id,
            'body': reply,
            'failure': failure,
            'reply_q': self.reply_q,
        }

        _post_envelope(marconi, envelope, self.reply_q)


class MarconiListener(base.Listener):

    def listen_on(self, queue_name):
        if hasattr(self, 'queue_name'):
            raise NotImplementedError('Listening on multiple queues '
                                      'is not implemented.')

        self.queue_name = queue_name

    def poll(self):
        marconi = _marconi_client_with_conf()

        LOG.debug('Listening for messages on %s' % self.queue_name)

        envelope = _get_envelope(marconi, self.queue_name)
        while not envelope:
            time.sleep(1)
            envelope = _get_envelope(marconi, self.queue_name)

        msg_id = envelope['msg_id']
        ctxt = envelope['context']
        body = envelope['body']
        reply_q = envelope['reply_q']

        return MarconiIncomingMessage(self, ctxt, body, msg_id, reply_q)


class MarconiDriver(base.BaseDriver):

    def __init__(self, conf, url, default_exchange=None,
                 allowed_remote_exmods=[]):
        conf.register_opts(marconi_opts)
        conf.register_opts(impl_eventlet._eventlet_opts)

        super(MarconiDriver, self).__init__(conf, url, default_exchange,
                                        allowed_remote_exmods)

    def _send(self, target, ctxt, message,
              wait_for_reply=None, timeout=None):

        if target.fanout:
            raise NotImplementedError('Fanout is not implemented.')

        if not target.server:
            raise NotImplementedError('A specific server must be targeted.')

        topic = '%s_%s' % (target.topic, target.server)

        token = target.token
        tenant_id = target.tenant_id

        reply_q = _get_reply_q(tenant_id)

        marconi = _marconi_client_with_token(token)

        msg_id = unicode(uuid.uuid4().hex)

        envelope = {
            'msg_id': msg_id,
            'context': ctxt,
            'body': message,
            'reply_q': reply_q,
        }

        _post_envelope(marconi, envelope, topic)

        if wait_for_reply:
            return _wait_for_reply(msg_id, token, tenant_id, timeout)

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None):
        return self._send(target, ctxt, message, wait_for_reply, timeout)

    def send_notification(self, target, ctxt, message, version):
        raise NotImplementedError('Notifications are not implemented.')

    def listen(self, target):
        listener = MarconiListener(self)
        listener.listen_on('%s_%s' % (target.topic, target.server))

        return listener

    def cleanup(self):
        pass
