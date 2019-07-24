"""
Authentication "worker" module for the crossbar server, implementing
authentication for use with the 'test_vger' testing module.
"""
from pprint import pprint

from twisted.internet.defer import inlineCallbacks

from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.exception import ApplicationError


# our principal "database"
PRINCIPALS_DB = {
    'service1': {
        'realm': 'pangalactic-services',
        'role': 'service',
        'ticket': '789secret'
    },
    'service2': {
        'realm': 'pangalactic-services',
        'role': 'service',
        'ticket': '987secret'
    },
    'steve': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'fester': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'zaphod': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'buckaroo': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'whorfin': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'bigboote': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'smallberries': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'thornystick': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
    },
    'manyjars': {
        'realm': 'pangalactic-services',
        'role': 'user',
        'ticket': '1234'
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
                    raise ApplicationError('com.example.invalid_ticket',
                          "could not authenticate session - "
                          "invalid ticket '{}' for principal {}".format(
                                                            ticket, authid))
                if realm and realm != principal['realm']:
                    raise ApplicationError('com.example_invalid_realm',
                            "user {} should join {}, not {}".format(authid,
                                                principal['realm'], realm))
                res = {
                    'realm': principal['realm'],
                    'role': principal['role'],
                    'extra': {
                        'my-custom-welcome-data': [1, 2, 3]
                    }
                }
                print("WAMP-Ticket authentication success: {}".format(res))
                return res
            else:
                raise ApplicationError("com.example.no_such_user",
                                       "could not authenticate session - "
                                       "no such principal {}".format(authid))
        try:
            yield self.register(authenticate, 'pgef.authenticate')
            print("WAMP-Ticket dynamic authenticator registered!")
        except Exception as e:
            print("Failed to register dynamic authenticator: {0}".format(e))

