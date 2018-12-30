"""
Authentication "worker" module for the crossbar server, implementing
authentication for use with the 'test_vger' testing module.
"""
from __future__ import print_function
from pprint import pprint

from twisted.internet.defer import inlineCallbacks

from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.exception import ApplicationError


# our principal "database"
PRINCIPALS_DB = {
    u'service1': {
        u'realm': u'pangalactic-services',
        u'role': u'service',
        u'ticket': u'789secret'
    },
    u'service2': {
        u'realm': u'pangalactic-services',
        u'role': u'service',
        u'ticket': u'987secret'
    },
    u'steve': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'fester': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'zaphod': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'buckaroo': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'whorfin': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'bigboote': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'smallberries': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'thornystick': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    },
    u'manyjars': {
        u'realm': u'pangalactic-services',
        u'role': u'user',
        u'ticket': u'1234'
    }
}


class AuthenticatorSession(ApplicationSession):

    @inlineCallbacks
    def onJoin(self, details):

        print("WAMP-Ticket dynamic authenticator joined: {}".format(details))

        def authenticate(realm, authid, details):
            # TODO:  implement logging rather than prints
            print("WAMP-Ticket dynamic authenticator invoked:")
            print(" - realm='{}', authid='{}', details=".format(realm, authid))
            pprint(details)
            if authid in PRINCIPALS_DB:
                ticket = details['ticket']
                principal = PRINCIPALS_DB[authid]

                if ticket != principal['ticket']:
                    raise ApplicationError(u'com.example.invalid_ticket',
                          "could not authenticate session - "
                          "invalid ticket '{}' for principal {}".format(
                                                            ticket, authid))
                if realm and realm != principal[u'realm']:
                    raise ApplicationError(u'com.example_invalid_realm',
                            "user {} should join {}, not {}".format(authid,
                                                principal[u'realm'], realm))
                res = {
                    u'realm': principal[u'realm'],
                    u'role': principal[u'role'],
                    u'extra': {
                        u'my-custom-welcome-data': [1, 2, 3]
                    }
                }
                print("WAMP-Ticket authentication success: {}".format(res))
                return res
            else:
                raise ApplicationError("com.example.no_such_user",
                                       "could not authenticate session - "
                                       "no such principal {}".format(authid))
        try:
            yield self.register(authenticate, u'omb.authenticate')
            print("WAMP-Ticket dynamic authenticator registered!")
        except Exception as e:
            print("Failed to register dynamic authenticator: {0}".format(e))

