"""
Authentication "worker" module for the crossbar server, implementing
authentication for use with the 'test_vger' testing module.
"""
from __future__ import print_function
import bcrypt
import fileinput
import os
from pprint import pprint

from twisted.internet.defer import inlineCallbacks

from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.exception import ApplicationError

#import socket
#FQDN = socket.getfqdn()
#try:
#    import kerberos as krb
#except:
#    import kerberos_sspi as krb
#errs, server = krb.authGSSServerInit(crossbar@%s' % FQDN)

# import gssapi
# service_name = gssapi.Name('crossbar@%s' % FQDN, name_type=gssapi.NameType.hostbased_service)
# server_creds = gssapi.Credentials(usage='accept', name=service_name)
# ctx = gssapi.SecurityContext(creds=server_creds, usage='accept')


# our principal "database"
PRINCIPALS_DB = {
    u'service1': {
        u'realm_role': {
            u'services': u'service',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'789secret'.encode('utf8'), bcrypt.gensalt())
    },
    u'service2': {
        u'realm_role': {
            u'services': u'service',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'987secret'.encode('utf8'), bcrypt.gensalt())
    },
    u'james': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'frank': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'steve': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'zaphod': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'buckaroo': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'whorfin': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'hector': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'michael': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'terri': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'terry': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'kevin': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    },
    u'synthia': {
        u'realm_role': {
            u'services': u'user',
            u'realm-auth': u'authenticator'
        },
        u'ticket': bcrypt.hashpw(u'1234'.encode('utf8'), bcrypt.gensalt())
    }
}



class AuthenticatorSession(ApplicationSession):

    @inlineCallbacks
    def onJoin(self, details):

        print("WAMP-Ticket dynamic authenticator joined: {}".format(details))

        def add_login(auid):
            if auid not in PRINCIPALS_DB:
                for line in fileinput.input(os.path.abspath(__file__), inplace=1):
                    trimmed_line = line.rstrip()
                    print(trimmed_line)
                    hashed_pw = bcrypt.hashpw(auid.encode('utf8'), bcrypt.gensalt())
                    if trimmed_line == 'PRINCIPALS_DB = {':
                        print("    u'%s': {" % auid)
                        print("        u'realm_role': {")
                        print("            u'services': u'user',")
                        print("            u'realm-auth': u'authenticator'")
                        print("        },")
                        print("        u'ticket': '%s'" % hashed_pw)
                        print("    },")
                PRINCIPALS_DB[auid] = {
                                         u'realm_role': {
                                             u'services': u'user',
                                             u'realm-auth': u'authenticator'
                                         },
                                         u'ticket': hashed_pw
                                      }

        def modify_login(auid, new_pass):
            if auid in PRINCIPALS_DB:
                is_current_entry = False
                hashed_pw = bcrypt.hashpw(new_pass.encode('utf8'), bcrypt.gensalt())
                for line in fileinput.input(os.path.abspath(__file__), inplace=1):
                    trimmed_line = line.rstrip()
                    if trimmed_line == ("    u'%s': {" % auid):
                        is_current_entry = True
                    if is_current_entry and trimmed_line.startswith("        u'ticket': "):
                        print("        u'ticket': '%s'" % hashed_pw)
                        is_current_entry = False
                    else:
                        print(trimmed_line)
                PRINCIPALS_DB[auid][u'ticket'] = hashed_pw

        def authenticate(realm, authid, details, create=False, new_passwd=None):
            # TODO:  implement logging rather than prints
            assert(not(create and new_passwd is not None))
            if create:
                add_login(authid)
                return
            if new_passwd is not None:
                modify_login(authid, new_passwd)
                return
            print("WAMP-Ticket dynamic authenticator invoked:")
            print(" - realm='{}', authid='{}', details=".format(realm, authid))
            ticket = details['ticket']
            del details['ticket']
            pprint(details)
            if authid in PRINCIPALS_DB:
                #sres = krb.authGSSServerStep(server, ticket)
                #response = krb.authGSSServerResponse(server)
                #krb.authGSSServerUserName(server)

                #server_token = ctx.step(ticket)
                #str(ctx.delegated_creds.name).split('@')[0]
                principal = PRINCIPALS_DB[authid]

                if not bcrypt.checkpw(ticket.encode('utf8'), principal['ticket']):
                    raise ApplicationError(u'com.example.invalid_ticket',
                          "could not authenticate session - "
                          "invalid ticket for principal {}".format(authid))
                if realm and realm not in principal[u'realm_role']:
                    raise ApplicationError(u'com.example_invalid_realm',
                            "user {} should join {}, not {}".format(authid,
                                                list(principal[u'realm_role'].keys()), realm))
                if realm is None:
                    realm = u'services'
                res = {
                    u'realm': realm,
                    u'role': principal[u'realm_role'][realm],
                    u'extra': {
                        u'my-custom-welcome-data': [1, 2, 3],
                        u'update_password': ticket == authid or ticket == '1234'
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

