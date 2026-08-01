# -*- coding: utf-8 -*-
"""
This authenticator module is intended to be located in the directory that is
mapped to the "/node" directory of the crossbar docker service (see example
'run_cb_crypto.sh.template') and in the crossbar configuration it should be
configured as a "component" with the role of "authenticator" -- see the example
crossbar config.json file 'crossbar_config_tls.json'.
"""
import json, os, sqlite3

from twisted.internet.defer import inlineCallbacks

from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.exception import ApplicationError


# NOTE: "/node" is the directory mapped into the crossbar docker service (see
# the module docstring above), and remains the default so docker deployments
# are unaffected.  These paths used to be hardcoded, which meant a non-docker
# deployment -- local interactive testing, for instance -- could not make this
# module and vger's "auth_db_path" refer to the same file: this component runs
# inside crossbar, so it cannot read vger's config.  Set PGEF_PRINCIPALS_DIR
# in the environment crossbar is started from, and set "auth_db_path" in
# vger's config to the matching principals.db, so that public keys added by
# vger.add_person() land in the db this authenticator actually reads.
PRINCIPALS_DIR = os.environ.get('PGEF_PRINCIPALS_DIR', '/node')
PRINCIPALS_JSON = os.path.join(PRINCIPALS_DIR, 'principals.json')
PRINCIPALS_DB = os.path.join(PRINCIPALS_DIR, 'principals.db')


class AuthenticatorSession(ApplicationSession):

    @inlineCallbacks
    def onJoin(self, details):

        self.log.info('* authenticator using principals db: {db}',
                      db=PRINCIPALS_DB)
        # if db does not exist, initialize it with test principals ...
        if (os.path.exists(PRINCIPALS_JSON)
            and not os.path.exists(PRINCIPALS_DB)):
            f = open(PRINCIPALS_JSON)
            PRINCIPALS = json.load(f)
            f.close()
            conn = sqlite3.connect(PRINCIPALS_DB)
            c = conn.cursor()
            c.execute('''CREATE TABLE users
                         (pubkey blob, authid text, role text)''')
            recs = [(p['pubkey'], p['authid'], p['role']) for p in PRINCIPALS]
            c.executemany('INSERT INTO users VALUES (?, ?, ?)', recs)
            conn.commit()
            conn.close()

        # dynamic authentication procedure
        def authenticate(realm, authid, details):
            self.log.debug("authenticate({realm}, {authid}, {details})",
                           realm=realm, authid=authid, details=details)
            assert('authmethod' in details)
            assert(details['authmethod'] == 'cryptosign')
            assert('authextra' in details)
            assert('pubkey' in details['authextra'])
            pubkey = details['authextra']['pubkey']
            self.log.info("authenticating session with public key = {pubkey}",
                          pubkey=pubkey)

            conn = sqlite3.connect(PRINCIPALS_DB)
            c = conn.cursor()
            c.execute('SELECT authid, role FROM users WHERE pubkey = ?',
                      (pubkey,))
            res = c.fetchone()
            conn.close()
            if res:
                authid, role = res
            else:
                authid, role = None, None
            if authid:
                auth = {
                   'pubkey': pubkey,
                   'realm': 'pangalactic-services',
                   'authid': authid,
                   'role': role,
                   'cache': True
                }
                self.log.info(
                    "found valid principal {authid} matching public key",
                    authid=authid)
                return auth
            else:
                self.log.error("no principal found matching public key")
                raise ApplicationError('pangalactic.no_such_user',
                                      'no principal with matching public')

        # register the authenticator
        try:
            yield self.register(authenticate, 'pangalactic.authenticate')
            self.log.info("Dynamic authenticator registered!")
        except Exception as e:
            self.log.info("Failed to register authenticator: {0}".format(e))

