
# Copyright 2013 Red Hat, Inc.
#
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


class Target(object):

    """Identifies the destination of messages.

    A Target encapsulates all the information to identify where a message
    should be sent or what messages a server is listening for.

    Different subsets of the information encapsulated in a Target object is
    relevant to various aspects of the API:

      creating a server:
        topic and server is required; exchange is optional
      an endpoint's target:
        namespace and version is optional
      client sending a message:
        topic is required, all other attributes optional

    Its attributes are:

    :param exchange: A scope for topics. Leave unspecified to default to the
      control_exchange configuration option.
    :type exchange: str
    :param topic: A name which identifies the set of interfaces exposed by a
      server. Multiple servers may listen on a topic and messages will be
      dispatched to one of the servers in a round-robin fashion.
    :type topic: str
    :param namespace: Identifies a particular interface (i.e. set of methods)
      exposed by a server. The default interface has no namespace identifier
      and is referred to as the null namespace.
    :type namespace: str
    :param version: Interfaces have a major.minor version number associated
      with them. A minor number increment indicates a backwards compatible
      change and an incompatible change is indicated by a major number bump.
      Servers may implement multiple major versions and clients may require
      indicate that their message requires a particular minimum minor version.
    :type version: str
    :param server: Clients can request that a message be directed to a specific
      server, rather than just one of a pool of servers listening on the topic.
    :type server: str
    :param fanout: Clients may request that a message be directed to all
      servers listening on a topic by setting fanout to ``True``, rather than
      just one of them.
    :type fanout: bool
    """

    def __init__(self, exchange=None, topic=None, namespace=None,
                 version=None, server=None, fanout=None, token=None,
                 tenant_id=None):
        self.exchange = exchange
        self.topic = topic
        self.namespace = namespace
        self.version = version
        self.server = server
        self.fanout = fanout
        self.token = token
        self.tenant_id = tenant_id

    def __call__(self, **kwargs):
        kwargs.setdefault('exchange', self.exchange)
        kwargs.setdefault('topic', self.topic)
        kwargs.setdefault('namespace', self.namespace)
        kwargs.setdefault('version', self.version)
        kwargs.setdefault('server', self.server)
        kwargs.setdefault('fanout', self.fanout)
        kwargs.setdefault('token', self.token)
        kwargs.setdefault('tenant_id', self.tenant_id)

        return Target(**kwargs)

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        attrs = []
        for a in ['exchange', 'topic', 'namespace',
                  'version', 'server', 'fanout']:
            v = getattr(self, a)
            if v:
                attrs.append((a, v))
        values = ', '.join(['%s=%s' % i for i in attrs])
        return '<Target ' + values + '>'
