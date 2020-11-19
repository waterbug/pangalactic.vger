#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
The Virtual Galactic Entropy Reverser
"""
import argparse, atexit, os, sqlite3, sys, traceback
from uuid import uuid4

import ruamel_yaml as yaml

from louie import dispatcher

from twisted.internet.defer import inlineCallbacks
from twisted.internet._sslverify import OpenSSLCertificateAuthorities
from twisted.internet.ssl import CertificateOptions

from OpenSSL import crypto

from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp         import cryptosign
from autobahn.wamp.types   import RegisterOptions

from pangalactic.core                  import __version__
from pangalactic.core                  import (config, state, read_config,
                                               write_config, write_state)
from pangalactic.core.access           import get_perms, is_cloaked
from pangalactic.core.entity           import Entity
from pangalactic.core.mapping          import schema_maps
from pangalactic.core.parametrics      import set_dval, set_pval
from pangalactic.core.serializers      import (DESERIALIZATION_ORDER,
                                               deserialize, serialize)
from pangalactic.core.refdata          import ref_oids
from pangalactic.core.test.utils       import (create_test_users,
                                               create_test_project)
from pangalactic.core.utils.datetimes  import dtstamp, earlier
from pangalactic.core.uberorb          import orb
from pangalactic.core.utils.meta       import uncook_datetime
from pangalactic.vger.userdir          import search_ldap_directory


gsfc_mel_parms = ['m', 'P', 'R_D',
                  'm[CBE]', 'm[Ctgcy]', 'm[MEV]',
                  'P[CBE]', 'P[Ctgcy]', 'P[MEV]',
                  'R_D[CBE]', 'R_D[Ctgcy]', 'R_D[MEV]',
                  'Cost']
gsfc_mel_des = ['Vendor', 'TRL']


class RepositoryService(ApplicationSession):
    """
    The Pan Galactic Engineering Repository Service container object
    (Application Session)
    """
    # NOTE to developers:  
    # For the serialization structure of PGEF domain class definitions, see
    # pangalactic.meta.registry._update_schemas_from_extracts

    def __init__(self, *args, **kw):
        """
        NOTE:  orb home directory and database connection url must be
        specified.  The 'self.config.extra' dict is set from the 'extra'
        keyword arg in ApplicationRunner.
        """
        super(RepositoryService, self).__init__(*args, **kw)
        # start the orb ...
        orb.start(home=self.config.extra['home'], gui=False,
                  db_url=self.config.extra['db_url'],
                  debug=self.config.extra['debug'],
                  console=self.config.extra['console'])
        # always load test users steve, zaphod, buckaroo, etc.
        orb.log.info('* checking for test users ...')
        deserialize(orb, create_test_users())
        orb.log.info('  test users loaded.')
        if self.config.extra['test']:
            # check whether test objects have been loaded
            if state.get('test_project_loaded'):
                orb.log.info('* H2G2 objects already loaded.')
            else:
                # set default parms for create_test_project
                orb.log.info('* loading H2G2 objects ...')
                deserialize(orb, create_test_project())
                hw = orb.search_exact(cname='HardwareProduct', id_ns='test')
                orb.assign_test_parameters(hw, parms=gsfc_mel_parms,
                                           des=gsfc_mel_des)
                state['test_project_loaded'] = True
            write_state(os.path.join(orb.home, 'state'))
        # create an "uploads" directory if there isn't one
        self.uploads_path = os.path.join(orb.home, 'vault', 'uploads')
        if not os.path.exists(self.uploads_path):
            os.makedirs(self.uploads_path)
        # load data from "extra_data" dir
        extra_data_path = os.path.join(orb.home, 'extra_data')
        if os.path.exists(extra_data_path) and os.listdir(extra_data_path):
            orb.log.info('* "extra_data" is present, checking ...')
            extra_data_fnames = os.listdir(extra_data_path)
            extra_data_fnames.sort()
            for fname in extra_data_fnames:
                if fname.endswith('.yaml'):
                    orb.log.info(f'  - found "{fname}", loading ...')
                    fpath = os.path.join(extra_data_path, fname)
                    with open(fpath) as f:
                        data = f.read()
                        sobjs = yaml.safe_load(data)
                        try:
                            objs = deserialize(orb, sobjs)
                            orb.log.info('    successfully deserialized.')
                            if objs:
                                ids = [o.id for o in objs]
                                orb.log.info('    loaded {} objs: {}'.format(
                                             len(ids), str(ids)))
                            else:
                                msg = '0 new or modified objs in data.'
                                orb.log.info('    {}'.format(msg))
                        except:
                            orb.log.info('    exception in deserializing ...')
                            orb.log.info(traceback.format_exc())
        dispatcher.connect(self.on_log_info_msg, 'log info msg')
        dispatcher.connect(self.on_log_debug_msg, 'log debug msg')
        atexit.register(self.shutdown)
        # load private key (raw format)
        try:
            self._key = cryptosign.SigningKey.from_raw_key(
                                                    self.config.extra['key'])
        except Exception as e:
            self.log.error("* could not load public key: {log_failure}",
                           log_failure=e)
            self.leave()
        else:
            self.log.info("* public key loaded: {}".format(
                                                    self._key.public_key()))

    def on_log_info_msg(self, msg=''):
        orb.log.info(msg)

    def on_log_debug_msg(self, msg=''):
        orb.log.debug(msg)

    def shutdown(self):
        """
        Serialize all database objects to a yaml file
        (db-dump-[datetime stamp].yaml) and save all caches, putting all files
        into the `backup` directory.  If the server is updated and the update
        includes a schema change, the orb can read, convert, and import the db
        dump file into a new database and parameter / data element caches after
        the server is restarted.
        """
        # dump_all() saves all caches and writes db to a yaml file
        orb.dump_all()

    def onConnect(self):
        self.log.info("* RepositoryService connected ...")
        # self.config is set up by the session
        realm = self.config.realm
        self.log.info("* realm set to: '%s'" % str(realm))
        # authentication extra information for wamp-cryptosign
        extra = {
            # forward the client pubkey: this allows us to omit authid as
            # the router can identify us with the pubkey already
            'pubkey': self._key.public_key(),
            # not yet implemented. a public key the router should provide
            # a trustchain for it's public key. the trustroot can eg be
            # hard-coded in the client, or come from a command line option.
            'trustroot': None,
            # not yet implemented. for authenticating the router, this
            # challenge will need to be signed by the router and send back
            # in AUTHENTICATE for client to verify. A string with a hex
            # encoded 32 bytes random value.
            'challenge': None,
            'channel_binding': 'tls-unique'
        }
        self.join(realm,
                  authmethods=['cryptosign'],
                  authid=None,
                  authextra=extra)

    def onChallenge(self, challenge):
        self.log.info("* authentication challenge received ...")
        # sign the challenge with our private key.
        signed_challenge = self._key.sign_challenge(self, challenge)
        # send back the signed challenge for verification
        return signed_challenge

    def on_vger_msg(self, msg):
        """
        Handle messages from the 'vger.channel.public' channel.
        """
        for item in msg.items():
            subject, content = item
            orb.log.info("* on_vger_msg")
            orb.log.info("      subject: {}".format(str(subject)))
            if subject == 'deleted':
                orb.log.info("      content: {}".format(str(content)))
                orb.log.info("      (taking no action)")
            # elif subject == 'decloaked':
                # obj_oid, obj_id = content
            # elif subject == 'modified':
                # obj_oid, obj_id, obj_mod_datetime = content

    @inlineCallbacks
    def onJoin(self, details):
        self.log.info("* session joined: {details}", details=details)
        self.log.info("  authenticated with WAMP-cryptosign.")
        try:
            yield self.subscribe(self.on_vger_msg, 'vger.channel.public')
        except:
            orb.log.info("  subscribe to vger.channel.public failed.")

        def assign_role(serialized_ra, cb_details=None):
            """
            Save a role assignment (RoleAssignment instance) to the repository.

            Args:
                serialized_ra (list of dict):  a serialized RoleAssignment
                    object (list containing a single dict)

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                dict of dicts, in the form:
                    {'new_obj_dts':  {obj0.oid : str(obj0.mod_datetime),
                                      obj1.oid : str(obj1.mod_datetime),
                                      ...},
                     'mod_obj_dts':  {obj2.oid : str(obj2.mod_datetime),
                                      obj3.oid : str(obj3.mod_datetime),
                                      ...}
                                      }
            """
            orb.log.info('* [rpc] vger.assign_role() ...')
            if not serialized_ra:
                orb.log.info('  called with nothing; returning.')
                return {'result': 'nothing saved.'}
            orb.log.info('  inspecting serialized ra ...')
            try:
                ra_dict = serialized_ra[0]
                orb.log.info(str(ra_dict))
            except:
                orb.log.info('  deserialization failed.')
                return {'result': 'nothing saved.'}
            userid = getattr(cb_details, 'caller_authid', 'unknown')
            orb.log.info('  caller authid: {}'.format(str(userid)))
            user_obj = orb.select('Person', id=userid)
            org_oid = ra_dict.get('role_assignment_context')
            admin_role = orb.get('pgefobjects:Role.Administrator')
            global_admin = orb.select('RoleAssignment',
                                  assigned_role=admin_role,
                                  assigned_to=user_obj,
                                  role_assignment_context=None)
            if org_oid:
                # is user an Administrator for this org or a global Admin?
                org = orb.get(org_oid)
                admin_ra = orb.select('RoleAssignment',
                                      assigned_role=admin_role,
                                      assigned_to=user_obj,
                                      role_assignment_context=org)
                if admin_ra or global_admin:
                    orb.log.info('  role assignment is authorized, saving ...')
                    output = deserialize(orb, [ra_dict], dictify=True)
                    mod_ra_dts = {}
                    new_ra_dts = {}
                    for mod_ra in output['modified']:
                        orb.log.info('   modified ra oid: {}'.format(
                                                                mod_ra.oid))
                        orb.log.info('                id: {}'.format(
                                                                mod_ra.id))
                        content = (mod_ra.oid, mod_ra.id,
                                   str(mod_ra.mod_datetime))
                        # role assignments are always "public"
                        orb.log.info('   publishing mod ra on public channel.')
                        self.publish('vger.channel.public',
                                     {'modified': content})
                        mod_ra_dts[mod_ra.oid] = str(mod_ra.mod_datetime)
                    for new_ra in output['new']:
                        orb.log.info('   new ra oid: {}'.format(new_ra.oid))
                        orb.log.info('           id: {}'.format(new_ra.id))
                        new_ra_dts[new_ra.oid] = str(new_ra.mod_datetime)
                        log_msg = 'new ra {} on public channel.'.format(
                                                                new_ra.id)
                        orb.log.info('   {}'.format(log_msg))
                        self.publish('vger.channel.public', {'decloaked':
                                                      [new_ra.oid, new_ra.id]})
                    return dict(new_obj_dts=new_ra_dts, mod_obj_dts=mod_ra_dts)
                else:
                    orb.log.info('  role assignment not authorized.')
            else:
                # the ra is Global Admin, can only be assigned by another
                # Global Admin ...
                if global_admin:
                    orb.log.info('  global admin assignment is authorized ...')
                    output = deserialize(orb, [ra_dict], dictify=True)
                    mod_ra_dts = {}
                    new_ra_dts = {}
                    # ignore mod_ra_dts (a global admin ra can be created or
                    # deleted, but not modified)
                    for new_ra in output['new']:
                        orb.log.info('   new ra oid: {}'.format(new_ra.oid))
                        orb.log.info('           id: {}'.format(new_ra.id))
                        new_ra_dts[new_ra.oid] = str(new_ra.mod_datetime)
                        log_msg = 'new ra {} on public channel.'.format(
                                                                new_ra.id)
                        orb.log.info('   {}'.format(log_msg))
                        self.publish('vger.channel.public', {'decloaked':
                                                      [new_ra.oid, new_ra.id]})
                    return dict(new_obj_dts=new_ra_dts, mod_obj_dts=mod_ra_dts)
                else:
                    orb.log.info('  no role_assignment_context found.')
                    return {'result': 'nothing saved.'}

        yield self.register(assign_role, 'vger.assign_role',
                            RegisterOptions(details_arg='cb_details'))

        def upload_chunk(fname=None, seq=0, data=b'', cb_details=None):
            """
            Upload a chunk of file data.

            Keyword Args:
                fname (str):  name of the data's file
                seq (int):  sequence number of chunk
                data (bytes):  data
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            n = len(data)
            orb.log.info('* [rpc] vger.upload_chunk() ...')
            orb.log.info(f'  fname: {fname}')
            orb.log.info(f'  chunk size: {n}')
            # write to file
            fpath = os.path.join(self.uploads_path, fname)
            with open(fpath, 'ab') as f:
                f.write(data)
            return seq

        yield self.register(upload_chunk, 'vger.upload_chunk',
                            RegisterOptions(details_arg='cb_details'))

        def save_uploaded_file(fname=None, oid=None, cb_details=None):
            """
            Finalize the upload of a file.

            Keyword Args:
                fname (str):  name of the data's file
                oid (str):  oid of the object (typically a RepresentationFile
                    instance) whose 'url' attr points to the data's file
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            orb.log.info('* [rpc] vger.save_uploaded_file() ...')
            orb.log.info(f'  fname: {fname}')
            # write to file
            return 'success'

        yield self.register(save_uploaded_file, 'vger.save_uploaded_file',
                            RegisterOptions(details_arg='cb_details'))

        def save(serialized_objs, cb_details=None):
            """
            Save a collection of objects to the repository.

            Args:
                serialized_objs (list of dict):  a serialized collection of objects

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                dict of dicts, in the form:
                  {'new_obj_dts':  {obj0.oid : str(obj0.mod_datetime),
                                    obj1.oid : str(obj1.mod_datetime),
                                    ...},
                   'mod_obj_dts':  {obj2.oid : str(obj2.mod_datetime),
                                    obj3.oid : str(obj3.mod_datetime),
                                    ...}
                   'unauth': [ids of objects for which save was unauthorized],
                   'no_owners': [ids of objects that did not have owners but
                                 should]
            """
            orb.log.info('* [rpc] vger.save() ...')
            no_owners = []
            if not serialized_objs:
                orb.log.info('  called with nothing.')
                return dict(new_obj_dts={}, mod_obj_dts={}, unauth=[],
                            no_owners=[])
            orb.log.info('  called for objects with object ids:')
            # uniquifies and gets rid of oidless objects
            sobjs_unique = {so.get('oid'): so for so in serialized_objs
                            if so.get('oid')}
            sobjs = sobjs_unique.values()
            sobjs_list = ''
            for so in sobjs:
                sobjs_list += '   + {} ({})\n'.format(so.get('id', '[no id]'),
                                                      so['_cname'])
            orb.log.info(sobjs_list)
            userid = getattr(cb_details, 'caller_authid', 'unknown')
            orb.log.info('  caller authid: {}'.format(str(userid)))
            user_obj = orb.select('Person', id=userid)
            user_oid = getattr(user_obj, 'oid', None)
            # check for objects that have no owners but should ...
            ownerless = []
            for sobj in sobjs:
                if (issubclass(orb.classes[sobj['_cname']],
                               orb.classes['ManagedObject'])
                    and not sobj.get('owner')):
                    # add object id to 'no_owners' and remove the object from
                    # sobjs_unique ...
                    no_owners.append(sobj['id'])
                    ownerless.append(sobj['oid'])
            for oid in ownerless:
                del sobjs_unique[oid]
            # new objects created by the user
            authorized = {oid:so for oid, so in sobjs_unique.items()
                          if so.get('creator') == user_oid}
            # existing objects for which the user has 'modify' permission
            for oid, so in sobjs_unique.items():
                if 'modify' in get_perms(orb.get(so.get('oid')),
                                         user=user_obj):
                    authorized[oid] = so 
            unauthorized = {oid:so for oid, so in sobjs_unique.items()
                            if oid not in authorized}
            unauth_ids = [unauthorized[oid].get('id', 'no id')
                          for oid in unauthorized]
            if not authorized:
                orb.log.info('  no save: {} unauthorized object(s).'.format(
                                                          len(unauthorized)))
                return dict(new_obj_dts={}, mod_obj_dts={}, unauth=unauth_ids,
                            no_owners=no_owners)
            output = deserialize(orb, authorized.values(), dictify=True)
            mod_obj_dts = {}
            new_obj_dts = {}
            # the "new_objs" and "mod_obj" dicts need to group object dicts by
            # the channels on which they will be published:
            # {'public': {oid: id, ...}, 'org1': {oid: id, ...}, 'org2': ...}
            new_objs = {'public': {}}
            mod_objs = {'public': {}}
            for mod_obj in output['modified']:
                orb.log.info('   modified object oid: {}'.format(mod_obj.oid))
                orb.log.info('                    id: {}'.format(mod_obj.id))
                content = (mod_obj.oid, mod_obj.id,
                           str(mod_obj.mod_datetime))
                # if the object has a public attr set to True or does not have
                # a 'public' attr*, it is public unless it is a SANDBOX PSU.
                # NOTE:  * this includes Acu and non-SANDBOX PSU objects
                if is_cloaked(mod_obj):
                    orb.log.info('   cloaked: only owner org has access:')
                    # if cloaked, publish 'modified' message only on owner
                    # channel
                    owner_id = ''
                    if hasattr(mod_obj, 'owner'):
                        owner_id = getattr(mod_obj.owner, 'id', None)
                    elif isinstance(mod_obj,
                                    orb.classes['ProjectSystemUsage']):
                        owner = getattr(mod_obj.system, 'owner', None)
                        if owner:
                            owner_id = getattr(mod_obj.system.owner, 'id',
                                               None)
                    elif isinstance(mod_obj, orb.classes['Acu']):
                        owner = getattr(mod_obj.assembly, 'owner', None)
                        if owner:
                            owner_id = getattr(mod_obj.assembly.owner, 'id',
                                               None)
                    if owner_id:
                        if owner_id in mod_objs:
                            mod_objs[owner_id][mod_obj.oid] = mod_obj.id
                        else:
                            mod_objs[owner_id] = {mod_obj.oid: mod_obj.id}
                        log_msg = 'cloaked: publishing "modified" on channel:'
                        channel = 'vger.channel.' + owner_id
                        orb.log.info('   + {} {}'.format(log_msg, channel))
                        self.publish(channel, {'modified': content})
                    else:
                        orb.log.info('   not publishing -- no owner org.')
                else:
                    orb.log.info('   + modified object is public, publishing')
                    orb.log.info('     "modified" on public channel ...')
                    channel = 'vger.channel.public'
                    self.publish(channel, {'modified': content})
                mod_obj_dts[mod_obj.oid] = str(mod_obj.mod_datetime)
            for new_obj in output['new']:
                orb.log.info('   new object oid: {}'.format(new_obj.oid))
                orb.log.info('               id: {}'.format(new_obj.id))
                content = (new_obj.oid, new_obj.id,
                           str(new_obj.mod_datetime))
                if is_cloaked(new_obj):
                    orb.log.info('   + new object oid: {}'.format(new_obj.oid))
                    orb.log.info('     object is cloaked -- ')
                    owner_id = ''
                    if isinstance(new_obj, orb.classes['Product']):
                        owner_id = getattr(new_obj.owner, 'id', None)
                    elif isinstance(new_obj,
                                    orb.classes['ProjectSystemUsage']):
                        owner = getattr(new_obj.system, 'owner', None)
                        if owner:
                            owner_id = getattr(new_obj.system.owner, 'id',
                                               None)
                    elif isinstance(new_obj, orb.classes['Acu']):
                        owner = getattr(new_obj.assembly, 'owner', None)
                        if owner:
                            owner_id = getattr(new_obj.assembly.owner, 'id',
                                               None)
                    if owner_id:
                        msg = '   + publishing "new" only to owner org: "{}"'
                        orb.log.info(msg.format(owner_id))
                        if owner_id in new_objs:
                            new_objs[owner_id][new_obj.oid] = new_obj.id
                        else:
                            new_objs[owner_id] = {new_obj.oid: new_obj.id}
                        log_msg = 'cloaked: publishing "new" on channel:'
                        channel = 'vger.channel.' + owner_id
                        orb.log.info('   + {} {}'.format(log_msg, channel))
                        self.publish(channel, {'new': content})
                    else:
                        orb.log.info('   not publishing -- no owner org.')
                else:
                    orb.log.info('   + new object is public --')
                    orb.log.info('     will publish on public channel ...')
                    new_objs["public"][new_obj.oid] = new_obj.id
                new_obj_dts[new_obj.oid] = str(new_obj.mod_datetime)
            # publish "decloaked" messages for new objects here ...
            for org_id in new_objs:
                if org_id == 'public' and new_objs['public']:
                    # publish decloaked for new public objs on public channel
                    txt = 'publishing decloaked items on public channel...'
                    orb.log.info('   + {}'.format(txt))
                    for obj_oid, obj_id in new_objs['public'].items():
                        self.publish('vger.channel.public',
                                     {'decloaked': [obj_oid, obj_id]})
                elif not org_id == 'public':
                    # if not public, publish "new" on owner org channel
                    channel = 'vger.channel.' + org_id
                    txt = 'publishing cloaked items on channel "{}" ...'
                    orb.log.info('   + {}'.format(txt.format(channel)))
                    for obj_oid, obj_id in new_objs[org_id].items():
                        self.publish(channel, {'new': [obj_oid, obj_id]})
            return dict(new_obj_dts=new_obj_dts, mod_obj_dts=mod_obj_dts,
                        unauth=unauth_ids, no_owners=no_owners)

        yield self.register(save, 'vger.save',
                            RegisterOptions(details_arg='cb_details'))

        def delete(oids, cb_details=None):
            """
            Deletes a set of objects by their oids.

            Args:
                oids (list of str):  object oids

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                tuple of lists:  (oids_not_found, oids_deleted)
            """
            orb.log.info('* vger.delete({})'.format(str(oids)))
            # TODO:  check that user has permission to delete
            userid = getattr(cb_details, 'caller_authid', None)
            user = orb.select('Person', id=userid)
            objs_by_oid = {oid: orb.get(oid) for oid in oids}
            oids_not_found = [oid for oid, obj in objs_by_oid.items()
                              if obj is None]
            objs_found = {oid: obj for oid, obj in objs_by_oid.items()
                          if obj is not None}
            # deletions are authorized only for objs created by this user,
            # except in the case of RoleAssignments ...
            # NOTE: *Except* for RoleAssignments, objects without a 'creator'
            # attribute cannot be deleted -> only instances of subclasses of
            # 'Modelable' can be deleted.
            admin_role = orb.get('pgefobjects:Role.Administrator')
            global_admin = bool(orb.select('RoleAssignment', assigned_to=user,
                                assigned_role=admin_role,
                                role_assignment_context=None))
            if global_admin:
                auth_dels = objs_found
            else:
                auth_dels = {}
                for obj in objs_found.values():
                    # first check for RoleAssignments
                    if isinstance(obj, orb.classes['RoleAssignment']):
                        # RoleAssignments can only be deleted by an Administrator
                        # for the Organization in which the Role was assigned
                        org = obj.role_assignment_context
                        admin = orb.select('RoleAssignment', assigned_to=user,
                                           assigned_role=admin_role,
                                           role_assignment_context=org)
                        if admin:
                            auth_dels[obj.oid] = obj
                    # next, SANDBOX PSUs
                    elif (hasattr(obj, 'project') and
                          getattr(obj.project, 'id', '') == 'SANDBOX'):
                        # if SANDBOX PSU, delete but don't publish
                        orb.delete([obj])
                    elif getattr(obj, 'creator', None) is user:
                        auth_dels[obj.oid] = obj
                    elif 'delete' in get_perms(obj, user=user):
                        auth_dels[obj.oid] = obj
            oids_deleted = list(auth_dels.keys())
            orb.delete(auth_dels.values())
            for oid in oids_deleted:
                orb.log.info('   publishing "deleted" msg to public channel.')
                channel = 'vger.channel.public'
                self.publish(channel, {'deleted': oid})
            return (oids_not_found, oids_deleted)

        yield self.register(delete, 'vger.delete',
                            RegisterOptions(details_arg='cb_details'))

        def sync_objects(data, cb_details=None):
            """
            Sync the objects referenced by the data.  NOTE:  oids in the data
            that are unknown to the server will be returned in the 4th element
            of the result (i.e., [3] in the result specification below).

            NOTE: the main use case for `sync_objects()` is as the first step
            in syncing a user's created objects between their client's local
            database and the repository, so that any objects the user created
            since their last login will be added to the repository (that will
            be done in a separate rpc by the client after it receives this
            result with the oids not found on the server).

            Args:
                data (dict):  dict {oid: str(mod_datetime)}
                    for the objects to be synced
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (list of lists):  list containing:
                    [0]:  server objects that have later mod_datetime(s)
                    [1]:  oids of server objects with same mod_datetime(s)
                    [2]:  oids of server objects with earlier mod_datetime(s)
                    [3]:  any oids in data that were not found on the server
            """
            orb.log.info('* [rpc] vger.sync_objects()')
            result = [[], [], [], []]
            # remove any refdata
            non_ref = set(data.keys()) - set(ref_oids)
            data = {oid: data[oid] for oid in non_ref}
            if not data:
                orb.log.info('  no data sent; returning empty.')
                return result
            orb.log.info('   data: {}'.format(str(data)))
            # oids of objects unknown to the server
            unknown_oids = list(set(data) - set(orb.get_oids()))
            for oid in unknown_oids:
                del data[oid]
            dts_by_oid = {oid: uncook_datetime(dt_str)
                          for oid, dt_str in data.items()}
            server_dts = {oid: uncook_datetime(dt_str) for oid, dt_str
                          in orb.get_mod_dts(oids=list(data)).items()}
            # oids of newer objects on the server
            newer_oids = []
            for server_oid, server_dt in server_dts.items():
                client_dt = dts_by_oid.get(server_oid)
                if earlier(client_dt, server_dt):
                    newer_oids.append(server_oid)
            for oid in newer_oids:
                if oid in dts_by_oid:
                    del dts_by_oid[oid]
            # oids of server objects with same mod_datetime as submitted oids
            same_oids = [oid for oid, dt in dts_by_oid.items()
                         if dt == server_dts.get(oid)]
            # oids of older objects on the server
            older_oids = [oid for oid, dt in dts_by_oid.items()
                          if server_dts.get(oid) and dt > server_dts.get(oid)]
            if newer_oids:
                newer_sobjs = serialize(orb, orb.get(oids=newer_oids),
                                        include_components=True)
                result = [newer_sobjs, same_oids, older_oids, unknown_oids]
            else:
                result = [[], same_oids, older_oids, unknown_oids]
            orb.log.info('   result: {}'.format(str(result)))
            return result

        yield self.register(sync_objects, 'vger.sync_objects',
                            RegisterOptions(details_arg='cb_details'))

        def sync_library_objects(data, cb_details=None):
            """
            Sync all instances of ManagedObject* to which the user has access.
            (NOTE: `sync_objects()` should be called first with the user's
            local objects, so that any objects the user created since their
            last login will be added to the server.)

            NOTE:  the use of the keyword arg 'public' in orb.search_exact()
            implies that only instances of ManagedObject and its subclasses
            (Product, Template, etc.) will be returned.  Note also that this
            means PortTypes and PortTemplates libraries will not be synced,
            since they are not ManagedObjects, but they are also "reference
            data", so they should not be synced anyway.

            Args:
                data (dict):  dict {oid: str(mod_datetime)}
                    containing the library objects that the user has (all
                    objects the user has that were not created by the user)
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (list of lists):  list containing:
                    [0]:  any oids that have later mod_datetime(s) or were not
                          found in data, sorted in DESERIALIZATION_ORDER (the
                          client will do get_objects() calls to get these
                          objects)
                    [1]:  any oids in data that were not found on the server --
                          the user should delete these from their local db if
                          they are either
                          [a] not created by the user or
                          [b] created by the user but are in 'trash'.
            """
            orb.log.info('* [rpc] vger.sync_library_objects()')
            orb.log.info('  data: {}'.format(str(data)))

            # TODO: user object will be needed when more than "public" objects
            # are to be returned -- e.g., organizational product libraries to
            # which the user has access by having a role in the organization
            # user = None
            # userid = getattr(cb_details, 'caller_authid', '')
            # if userid:
                # user = orb.select('Person', id=userid)
            result = [[], []]

            # oids of objects unknown to the server (these would be objects
            # in data that were deleted on the server) -- the user should
            # delete these from their local db (NOTE that this is the REVERSE
            # of the action taken by `sync_objects()`, which assumes they are
            # to be deleted on the server!!).
            unknown_oids = list(set(data) - set(orb.get_oids()))
            for oid in unknown_oids:
                del data[oid]
            # submitted data:  mod_datetimes by oid
            dts_by_oid = {oid: uncook_datetime(dt_str)
                          for oid, dt_str in data.items()}
            # get mod_dts of all objects on the server to which the user should
            # have access ...
            # initially, just public objects (`ManagedObject` subtypes)
            server_dts = {}
            all_public_oids = [o.oid for o in orb.search_exact(public=True)]
            # exclude reference data
            public_oids = list(set(all_public_oids) - set(ref_oids))
            if public_oids:
                server_dts = {oid: uncook_datetime(dt_str) for oid, dt_str
                              in orb.get_mod_dts(oids=public_oids).items()}
            # oids of newer objects on the server (or objects unknown to user)
            newer_oids = []
            if server_dts:
                for server_oid, server_dt in server_dts.items():
                    client_dt = dts_by_oid.get(server_oid)
                    if earlier(client_dt, server_dt):
                        newer_oids.append(server_oid)
                for oid in newer_oids:
                    if dts_by_oid.get(oid):
                        del dts_by_oid[oid]
            if newer_oids:
                newer_oc = orb.get_oid_cnames(oids=newer_oids)
                # TODO:  don't recompute this every time!!  create a
                # DESERIALIZATION_ORDER that includes all classes
                all_ord = (DESERIALIZATION_ORDER +
                             list(set(newer_oc.values()) -
                                  set(DESERIALIZATION_ORDER)))
                sorted_newer_oids = sorted(newer_oc, key=lambda x:
                                           all_ord.index(newer_oc.get(x)))
                result = [sorted_newer_oids, unknown_oids]
                orb.log.info('   result: {}'.format(str(result)))
            else:
                result = [{}, unknown_oids]
                orb.log.info('   result: {}'.format(str(result)))
            return result

        yield self.register(sync_library_objects, 'vger.sync_library_objects',
                            RegisterOptions(details_arg='cb_details'))

        def sync_project(project_oid, data, cb_details=None):
            """
            Sync all objects for the specified project in the repository.

            Args:
                project_oid (str):  oid of the project to be synced
                data (dict):  dict {oid: str(mod_datetime)}
                    for known objects of the project to be synced
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (list of lists):  list containing:
                    [0]:  server objects that either have later mod_datetime(s)
                          or are not represented in the data that was sent
                    [1]:  oids of server objects with same mod_datetime(s)
                    [2]:  oids of server objects with earlier mod_datetime(s)
                    [3]:  any oids in data that were not found on the server
            """
            orb.log.info('* [rpc] vger.sync_project() ...')
            orb.log.info('   project oid: {}'.format(str(project_oid)))
            orb.log.info('   data: {}'.format(str(data)))
            result = [[], [], [], []]
            if not project_oid or project_oid == 'pgefobjects:SANDBOX':
                return result
            project = orb.get(project_oid)
            if project:
                same_oids = []
                older_oids = []
                unknown_oids = []
                server_objs = orb.get_objects_for_project(project)
                if data:
                    # for oid in data:
                        # if not orb.get(oid):
                            # unknown_oids.append(oid)
                    unknown_oids = list(set(data) - set(orb.get_oids()))
                    for oid in unknown_oids:
                        del data[oid]
                    dts_by_oid = {oid: uncook_datetime(dts)
                                  for oid, dts in data.items()}
                    newer_objs = [obj for obj in server_objs
                                  if obj.oid not in dts_by_oid]
                    for o in server_objs:
                        dts = dts_by_oid.get(o.oid)
                        if earlier(dts, o.mod_datetime):
                            newer_objs.append(o)
                    same_oids = [o.oid for o in server_objs
                                 if o.mod_datetime == dts_by_oid.get(o.oid)]
                    older_oids = list(set(dts_by_oid.keys()) - set(same_oids)
                                      - set([o.oid for o in newer_objs]))
                else:
                    newer_objs = server_objs
                if newer_objs:
                    newer_sobjs = serialize(orb, newer_objs,
                                            include_components=True)
                    result = [newer_sobjs, same_oids, older_oids, unknown_oids]
                    orb.log.info('   result: {}'.format(str(result)))
                else:
                    result = [[], same_oids, older_oids, unknown_oids]
                    orb.log.info('   result: {}'.format(str(result)))
            else:
                orb.log.info('   ** project not found on server **')
            return result

        yield self.register(sync_project, 'vger.sync_project',
                            RegisterOptions(details_arg='cb_details'))

        def data_new_row(proj_id=None, dm_oid=None, row_oid=None,
                         cb_details=None):
            """
            Add a new row to a DataMatrix.

            Keyword Args:
                proj_id (str):  id of the project
                dm_oid (str):  oid of the DataMatrix
                row_oid (str):  oid of the new row
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            orb.log.info('* [rpc] vger.data_new_row() ...')
            orb.log.info('  project id: {}'.format(str(proj_id)))
            orb.log.info('  dm oid: {}'.format(str(dm_oid)))
            orb.log.info('  row_oid: {}'.format(str(row_oid)))
            # TODO:  update local copy of datamatrix ...
            # For now, just publish!
            # publish item on project channel
            channel = 'vger.channel.' + proj_id
            txt = 'publishing new row oid on channel "{}" ...'
            orb.log.info('  + {}'.format(txt.format(channel)))
            self.publish(channel,
                         {'data new row':
                          [proj_id, dm_oid, row_oid]})
            return 'success'

        yield self.register(data_new_row, 'vger.data_new_row',
                            RegisterOptions(details_arg='cb_details'))

        def data_update_item(proj_id=None, dm_oid=None, row_oid=None,
                             col_id=None, value=None, cb_details=None):
            """
            Update a DataMatrix item.

            Keyword Args:
                proj_id (str):  id of the project
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            orb.log.info('* [rpc] vger.data_update_item() ...')
            orb.log.info('  project id: {}'.format(str(proj_id)))
            orb.log.info('  dm oid: {}'.format(str(dm_oid)))
            orb.log.info('  row_oid: {}'.format(str(row_oid)))
            orb.log.info('  col_id: {}'.format(str(col_id)))
            orb.log.info('  value: {}'.format(str(value)))
            # TODO:  update local copy of datamatrix ...
            # For now, just publish!
            # publish item on project channel
            channel = 'vger.channel.' + proj_id
            txt = 'publishing data item on channel "{}" ...'
            orb.log.info('  + {}'.format(txt.format(channel)))
            self.publish(channel,
                         {'data item updated':
                          [proj_id, dm_oid, row_oid, col_id, value]})
            return 'success'

        yield self.register(data_update_item, 'vger.data_update_item',
                            RegisterOptions(details_arg='cb_details'))

        def set_parameter(oid=None, pid=None, value=None, units=None,
                          mod_datetime=None, cb_details=None):
            """
            Set a parameter value.

            Keyword Args:
                oid (str):  oid of the parent object or entity
                pid (str):  parameter id
                value (str):  string representation of the value
                units (str):  string representation of the units
                mod_datetime (str):  string representation of the modified
                    datetime
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            # argstr = f'oid={oid}, pid={pid}, value={value}, units={units}'
            # orb.log.info(f'* [rpc] set_parameter({argstr})')
            # For now, just publish on public channel
            set_pval(oid, pid, value, units=units, mod_datetime=mod_datetime)
            channel = 'vger.channel.public'
            # orb.log.info(f'  + publishing parameter on "{channel}" ...')
            self.publish(channel,
                         {'parameter set':
                          [oid, pid, value, units, mod_datetime]})
            return 'success'

        yield self.register(set_parameter, 'vger.set_parameter',
                            RegisterOptions(details_arg='cb_details'))

        def set_data_element(oid=None, deid=None, value=None, units=None,
                             mod_datetime=None, cb_details=None):
            """
            Set a data element value.

            Keyword Args:
                oid (str):  oid of the parent object or entity
                deid (str):  data element id
                value (str):  string representation of the value
                units (str):  string representation of the units
                mod_datetime (str):  string representation of the modified
                    datetime
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            # argstr = f'oid={oid}, deid={deid}, value={value}, units={units}'
            # orb.log.info(f'* [rpc] set_data_element({argstr})')
            # For now, just publish on public channel
            set_dval(oid, deid, value, units=units, mod_datetime=mod_datetime)
            channel = 'vger.channel.public'
            # orb.log.info(f'  + publishing data element on "{channel}" ...')
            self.publish(channel,
                         {'data element set':
                          [oid, deid, value, mod_datetime]})
            return 'success'

        yield self.register(set_data_element, 'vger.set_data_element',
                            RegisterOptions(details_arg='cb_details'))

        def save_entity(oid=None, creator=None, modifier=None,
                          create_datetime=None, mod_datetime=None,
                          owner=None, assembly_level=None, parent_oid=None,
                          system_oid=None, system_name=None, cb_details=None):
            """
            Save a (new or modified) entity.

            Keyword Args:
                oid (str):  oid of the parent object or entity
                creator (str):  oid of the entity's creator
                modifier (str):  oid of the entity's modifier
                create_datetime (str):  string representation of the created
                    datetime
                mod_datetime (str):  string representation of the modified
                    datetime
                owner (str):  oid of the owner Organization
                assembly_level (int):  level of assembly (for a MEL entity)
                parent_oid (str):  oid of the parent entity
                system_oid (str):  oid of the system represented (for a MEL
                    entity)
                system_name (str):  name of the system represented (for a MEL
                    entity)
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (str):  'success'
            """
            argstr = f'oid={oid}, creator={creator}, modifier={modifier}, '
            argstr += f'create_datetime={create_datetime}, '
            argstr += f'mod_datetime={mod_datetime}, owner={owner}, '
            argstr += f'assembly_level={assembly_level}, '
            argstr += f'parent_oid={parent_oid}, system_oid={system_oid}, '
            argstr += f'system_name={system_name}'
            orb.log.info(f'* [rpc] save_entity({argstr})')
            Entity(oid=oid, creator=creator, modifier=modifier,
                   create_datetime=create_datetime, mod_datetime=mod_datetime,
                   owner=owner, assembly_level=assembly_level,
                   parent_oid=parent_oid, system_oid=system_oid,
                   system_name=system_name)
            # For now, just publish on public channel
            channel = 'vger.channel.public'
            orb.log.info(f'  + publishing entity on "{channel}" ...')
            self.publish(channel,
                         {'entity created':
                          [oid, creator, modifier, create_datetime,
                           mod_datetime, owner, assembly_level, parent_oid,
                           system_oid, system_name]})
            return 'success'

        yield self.register(save_entity, 'vger.save_entity',
                            RegisterOptions(details_arg='cb_details'))

        def search_exact(**kw):
            """
            Search for instances of the specified class by exact match on a set
            of search arguments.

            Args:
                cname (str):  class name

            Keyword Args:
                kw:  keyword arguments dictionary

            Returns:
                list:  the objects found by the search
            """
            orb.log.info('* [rpc] vger.search_exact() ...')
            return serialize(orb, orb.search_exact(**kw))

        yield self.register(search_exact, 'vger.search_exact')

        def get_version():
            """
            Return the curent version of pangalactic and whether a schema
            change is involved. 

            Returns:
                tuple:  version (str), schema_change (bool)
            """
            orb.log.info('* [rpc] vger.get_version() ...')
            schema_change = bool(__version__ in schema_maps)
            return __version__, schema_change

        yield self.register(get_version, 'vger.get_version')

        def get_object(oid, include_components=True, cb_details=None):
            """
            Retrieve the pangalactic object with the specified oid. 

            Args:
                oid (str):  object oid

            Keyword Args:
                include_components (bool):  if True, components (items linked by
                    Acu relationships) will be included in the serialization --
                    i.e., a "white box" representation
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                list of dict:  A serialization of the object with the oid --
                    this will be a list that may include related objects. If no
                    object is found, returns an empty list
            """
            orb.log.info('* [rpc] vger.get_object({}) ...'.format(oid))
            userid = getattr(cb_details, 'caller_authid', '')
            # short-circuit requests for refdata ...
            if oid in ref_oids:
                return []
            if userid:
                user = orb.select('Person', id=userid)
                obj = orb.get(oid)
                if obj is None:
                    # orb.log.info('      not found.')
                    return []
                elif getattr(obj, 'public', True):
                    return serialize(orb, [obj],
                                     include_components=include_components)
                else:
                    if 'view' in get_perms(obj, user=user):
                        # TODO:  if include_components is True, get_perms()
                        # should be used to determine the user's access to the
                        # components ...
                        return serialize(orb, [obj],
                                         include_components=include_components)
                    else:
                        # orb.log.info('  not permitted for "{}".'.format(
                                                                    # userid))
                        return []
            else:
                return []

        yield self.register(get_object, 'vger.get_object',
                            RegisterOptions(details_arg='cb_details'))

        def get_objects(oids, include_components=True, cb_details=None):
            """
            Retrieve the pangalactic objects with the specified oids.

            Args:
                oids (list):  object oids

            Keyword Args:
                include_components (bool):  if True, components (items linked by
                    Acu relationships) will be included in the serialization --
                    i.e., a "white box" representation
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                list:  A list of the serialized objects with the oids
                    (note that it may be a longer list than that submitted
                    because it will often include related objects). If no
                    object is found, an empty list is returned.
            """
            orb.log.info('* [rpc] vger.get_objects({}) ...'.format(oids))
            # TODO: use get_perms() to determine authorization
            # userid = getattr(cb_details, 'caller_authid', '')
            # if userid:
                # user = orb.select('Person', id=userid)
            # exclude all ref data
            non_ref_oids = list(set(oids) - set(ref_oids))
            objs = orb.get(oids=non_ref_oids)
            if objs:
                # TODO:  if include_components is True, get_perms() should be
                # used to determine the user's access to the components ...
                return serialize(orb, objs,
                                 include_components=include_components)
            else:
                return []

        yield self.register(get_objects, 'vger.get_objects',
                            RegisterOptions(details_arg='cb_details'))

        def get_mod_dts(cname=None, oids=None):
            """
            Retrieves the 'mod_datetime' for the objects with the specified
            oids.

            Keyword Args:
                cname (str):  name of a class
                oids (iterable of str):  iterable of object oids

            Returns:
                dict:  A dict mapping oids to 'mod_datetime' strings.
            """
            orb.log.info('* [rpc] vger.get_mod_dts() ...')
            return orb.get_mod_dts(cname=cname, oids=oids)

        yield self.register(get_object, 'vger.get_mod_dts')

        def get_user_roles(userid, data=None, cb_details=None):
            """
            Get [0] the Person object that corresponds to the userid, [1] all
            Organization and Project objects, [2] all Person objects, and [3]
            all RoleAssignment objects.

            Args:
                userid (str):  userid of a person (Person.id)
                    NOTE: the "userid" arg is a remnant of ticket-based
                    authentication; it is now ignored in favor of the
                    "caller_authid" from cb_details
                data (dict):  dict {oid: str(mod_datetime)}
                    for the requestor's Person, Organization, Project, and
                    RoleAssignment objects
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                tuple of lists:  [0] serialized user (Person) object,
                                 [1] serialized Organizations/Projects
                                 [2] serialized Person objects
                                 [3] serialized RoleAssignment objects
            """
            orb.log.info('* [rpc] vger.get_user_roles({}) ...'.format(userid))
            data = data or {}
            same_dts = []
            unknown_oids = []
            userid = getattr(cb_details, 'caller_authid', '')
            orb.log.info(f'  userid: "{userid}" ...')
            if data:
                server_dts = orb.get_mod_dts(oids=list(data))
                for oid in data:
                    if (oid in server_dts
                        and server_dts[oid] == data[oid]):
                        same_dts.append(oid)
                    elif oid not in server_dts:
                        unknown_oids.append(oid)
            # Now all oids in 'same_dts' are for objects that exist on the
            # server and have the same mod_datetime as their counterparts on
            # the client -- they will not be returned to the client.  All
            # server objects whose mod_datetime differs from that of the
            # corresponding object in the client's db will be returned to
            # replace the client's object, since the server's objects for
            # Person, Project, Organization, and RoleAssignment are
            # authoritative.
            szd_user = []
            szd_orgs = []
            szd_people = []
            szd_ras = []
            user = orb.select('Person', id=userid)
            if user:
                szd_user = serialize(orb, [user])
            else:
                orb.log.info(f'  no Person object found for "{userid}".')
            # all Organizations *and* Projects
            all_orgs = orb.get_all_subtypes('Organization')
            orgs = [o for o in all_orgs if o.oid not in same_dts]
            if orgs:
                szd_orgs = serialize(orb, orgs)
            # all Person objects
            all_people = orb.get_by_type('Person')
            people = [p for p in all_people if p.oid not in same_dts]
            if people:
                szd_people = serialize(orb, people)
            # all RoleAssignment objects
            all_ras = orb.get_by_type('RoleAssignment')
            ras = [ra for ra in all_ras if ra.oid not in same_dts]
            if ras:
                szd_ras = serialize(orb, ras)
            return [szd_user, szd_orgs, szd_people, szd_ras, unknown_oids]

        yield self.register(get_user_roles, 'vger.get_user_roles',
                            RegisterOptions(details_arg='cb_details'))

        def get_user_object(userid):
            """
            Retrieves the Person object for the specified userid.

            Args:
                userid (str):  userid of a person (Person.id)

            Returns:
                list:  list containing a serialized Person object
            """
            orb.log.info('* [rpc] vger.get_user_object()')
            return serialize(orb, [orb.select('Person', id=userid)])[0]

        yield self.register(get_user_object, 'vger.get_user_object')

        def search_ldap(**kw):
            """
            Search an LDAP directory using the specified keywords.

            Keyword Args:
                kw (dict): a dict of keyword arguments from which to compose
                    the LDAP search filter

            Returns:
                list:  list containing dicts of info on persons in the LDAP
                    directory
            """
            orb.log.info('* [rpc] vger.search_ldap')
            ldap_url = config.get('ldap_url') or ''
            base_dn = config.get('base_dn') or ''
            if ldap_url and base_dn:
                msg = 'calling search_ldap_directory() with {}'.format(kw)
                orb.log.info('      {}'.format(msg))
                return search_ldap_directory(ldap_url, base_dn, **kw)
            elif 'test' in kw and kw.get('test'):
                people = orb.get_by_type('Person')
                attrs = ['oid', 'id', 'last_name', 'first_name', 'mi_or_name',
                         'email']
                users = []
                for p in people:
                    # make "users" conform to the standard search return schema
                    user = {a: getattr(p, a) or '' for a in attrs}
                    user['name'] = ' '.join([user['first_name'],
                                             user['last_name']])
                    user['org_code'] = getattr(p.org, 'id', 'None')
                    user['employer_name'] = getattr(p.employer, 'id', 'None')
                    users.append(user)
                return ['local users', users]
            else:
                # TODO:  return a message that ldap is not available ...
                orb.log.info('      ldap is not available')
                return []

        yield self.register(search_ldap, 'vger.search_ldap')

        def add_person(data, cb_details=None):
            """
            Add a new Person (user) based on a set of attribute data.

            Args:
                data (dict): the attribute data of the Person

            Returns:
                saved_objs (list of dict):  if successful, a list containing
                    the serialized Person object, and if either the Person's
                    'org' or 'employer' Organizations are previously unknown to
                    the repository, objects for them will be created and
                    included in the returned list along with the Person object.
            """
            orb.log.info('* [rpc] vger.add_person')
            pk_added = False
            userid = getattr(cb_details, 'caller_authid', '')
            user = orb.select('Person', id=userid)
            # check that the caller is a Global Admin
            admin_role = orb.get('pgefobjects:Role.Administrator')
            global_admin = bool(orb.select('RoleAssignment', assigned_to=user,
                                assigned_role=admin_role,
                                role_assignment_context=None))
            if global_admin and data:
                msg = 'called with data: {}'.format(str(data))
                orb.log.info('    {}'.format(msg))
                # check if person is already in db ...
                person = orb.get(data.get('oid'))
                if person:
                    # TODO: if person is in the repo, update with data ...
                    orb.log.info('  person is in the repo; will update ...')
                else:
                    orb.log.info('  person is not in the repo; adding ...')
                saved_objs = []
                admin = orb.get('pgefobjects:admin')
                dts = dtstamp()
                employer_name = data.pop('employer_name', '')
                if employer_name:
                    employer = orb.select('Organization', name=employer_name)
                    if employer:
                        data['employer'] = employer
                    elif employer_name:
                        # if there is a non-null employer name and it does not
                        # have an Organization object, make one
                        Organization = orb.classes['Organization']
                        new_oid = str(uuid4())
                        new_id = '_'.join(employer_name.split(' '))
                        employer = Organization(oid=new_oid, id=new_id,
                                                name=employer_name,
                                                creator=admin, modifier=admin,
                                                create_datetime=dts,
                                                mod_datetime=dts)
                        orb.save([employer], recompute=False)
                        data['employer'] = employer
                        saved_objs.append(employer)
                # TODO: "org code" is some NASA-specific stuff
                org_code = data.pop('org_code', '')
                if org_code:
                    org = orb.select('Organization', id=org_code)
                    if org:
                        data['org'] = org
                    else:
                        # if there is not an Organization object for that org
                        # code, make one
                        Organization = orb.classes['Organization']
                        new_oid = str(uuid4())
                        org = Organization(oid=new_oid, id=org_code,
                                           name='Code '+org_code,
                                           creator=admin, modifier=admin,
                                           create_datetime=dts,
                                           mod_datetime=dts)
                        orb.save([org], recompute=False)
                        data['org'] = org
                        saved_objs.append(org)
                # pull public key out of data before adding/updating user
                public_key = data.pop('public_key', '')
                if public_key:
                    orb.log.info('  public_key is present, will add ...')
                if person:
                    # update person
                    for a in data:
                        setattr(person, a, data[a])
                    person.mod_datetime = dts
                    orb.save([person], recompute=False)
                    saved_objs.append(person)
                else:
                    # create person
                    Person = orb.classes['Person']
                    person = Person(creator=admin, modifier=admin,
                                    create_datetime=dts, mod_datetime=dts, **data)
                    orb.save([person], recompute=False)
                    saved_objs.append(person)
                if public_key:
                    default_db_path = os.path.join(orb.home, 'crossbar',
                                                   'principals.db')
                    auth_db_path = config.get('auth_db_path', default_db_path)
                    if os.path.exists(auth_db_path):
                        try:
                            # add pk to principals db
                            conn = sqlite3.connect(auth_db_path)
                            c = conn.cursor()
                            c.execute('INSERT INTO users VALUES (?, ?, ?)',
                                (public_key, data['id'], 'user'))
                            conn.commit()
                            conn.close()
                            orb.log.info('  - added public key')
                            orb.log.info('    for "{}".'.format(data['id']))
                            pk_added = True
                        except:
                            orb.log.info('  - exception encountered when')
                            orb.log.info('    attempting to add public key')
                            orb.log.info('    for "{}":'.format(data['id']))
                            orb.log.info(traceback.format_exc())
                            pk_added = False
                    else:
                        orb.log.info(f'  - path "{auth_db_path}" not found --')
                        orb.log.info(f'    could not add public key.')
                        pk_added = False
                ser_objs = serialize(orb, saved_objs)
                orb.log.info('    new person oid: {}'.format(person.oid))
                orb.log.info('                id: {}'.format(person.id))
                orb.log.info('    publishing "person added" on public channel.')
                channel = 'vger.channel.public'
                self.publish(channel, {'person added': ser_objs})
                res = (pk_added, ser_objs)
                orb.log.info('  returning result: {}'.format(str(res)))
                return res
            else:
                if not global_admin:
                    orb.log.info('  not global admin -- unauthorized!')
                elif not data:
                    orb.log.info('  no data provided!')
                return [False, []]

        yield self.register(add_person, 'vger.add_person',
                            RegisterOptions(details_arg='cb_details'))

        def get_people():
            """
            Get all Person objects and their "active" status (i.e., whether
            they have a public key in the crossbar authenticator's "principals"
            database).

            Returns:
                list of tuples:  if successful, a list of
                    (has_pk, serialized Person object) tuples, where has_pk is
                    True if the person has a public key in the principals db.
            """
            orb.log.info('* [rpc] vger.get_people')
            people = orb.get_by_type('Person')
            if people:
                serialized_people = serialize(orb, people)
                default_db_path = os.path.join(orb.home, 'crossbar',
                                               'principals.db')
                auth_db_path = config.get('auth_db_path', default_db_path)
                if os.path.exists(auth_db_path):
                    # TODO: use a try/except block here ...
                    conn = sqlite3.connect(auth_db_path)
                    c = conn.cursor()
                    c.execute('SELECT * from users')
                    active_users = c.fetchall()
                    conn.commit()
                    conn.close()
                    active_user_ids = [au[1] for au in active_users]
                    msg = 'returning {} people ({} active users)'.format(
                                            len(people), len(active_users))
                    orb.log.info(f'  {msg}')
                    return [((sp['id'] in active_user_ids), sp)
                            for sp in serialized_people]
                else:
                    orb.log.info(f'  "{auth_db_path}" not found ...')
                    orb.log.info('  active users could not be determined.')
                    return [(False, sp) for sp in serialized_people]
            else:
                return []

        yield self.register(get_people, 'vger.get_people')

        # end of backend setup
        orb.log.info("procedures registered")


if __name__ == '__main__':

    home_help = 'home directory (used by orb) [default: current directory]'
    config_help = 'initial config file name [default: "config"]'
    cert_help = 'crossbar host cert file name [default: "server_cert.pem"].'
    parser = argparse.ArgumentParser()
    parser.add_argument('--authid', dest='authid', type=str,
                        help='id to connect as (required)')
    parser.add_argument('--home', dest='home', type=str,
                        help=home_help)
    parser.add_argument('--config', dest='config', type=str,
                        default='config', help=config_help)
    parser.add_argument('--db_url', dest='db_url', type=str,
                        help='db connection url (used by orb)')
    parser.add_argument('--cb_host', dest='cb_host', type=str,
                        help='crossbar host [default: localhost].')
    parser.add_argument('--cb_port', dest='cb_port', type=int,
                        help='crossbar port [default: 8080].')
    parser.add_argument('--cert', dest='cert', type=str,
                        default='server_cert.pem', help=cert_help)
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Set logging level to DEBUG')
    parser.add_argument('-t', '--test', dest='test', action='store_true',
                        help='Loads test data at startup')
    parser.add_argument('--console', dest='console', action='store_true',
                        help='Sends log output to stdout')
    options = parser.parse_args()

    from autobahn.twisted.wamp import ApplicationRunner
    # from autobahn.twisted.component import Component, run

    # command options override config settings; if neither, defaults are used
    home = options.home or ''
    if os.path.exists(options.config):
        read_config(options.config)
    else:
        read_config(os.path.join(home, 'config'))
    # NOTE: do not need to submit authid when using cryptosign auth; crossbar
    # will look up the authid associated with our public key ...
    # authid = options.authid or config.get('authid')
    # unix domain socket connection to db:  socket located in home dir
    domain_socket = os.path.join(home, 'vgerdb_socket')
    db_url = options.db_url or config.get('db_url',
             'postgresql://scred@:5432/vgerdb?host={}'.format(domain_socket))
    private_key = os.path.join(home, 'vger.key')
    test = options.test or config.get('test', False)
    debug = options.debug or config.get('debug', False)
    console = options.console or config.get('console', False)
    extra = {
        'authid': None,
        'home': home,
        'db_url': db_url,
        'key': private_key,
        'console': console,
        'debug': debug,
        'test': test
        }
    cb_host = options.cb_host or config.get('cb_host', 'localhost')
    cb_port = options.cb_port or config.get('cb_port', 8080)
    cb_url = 'wss://{}:{}/ws'.format(cb_host, cb_port)
    # router can auto-choose the realm, so not necessary to specify
    realm = 'pangalactic-services'
    # config['authid'] = None
    config['db_url'] = db_url
    config['debug'] = debug
    config['console'] = console
    config['test'] = test
    config['cb_host'] = cb_host
    config['cb_port'] = cb_port
    # write the new config file
    write_config(os.path.join(home, 'config'))
    print("vger starting with")
    print("   home directory:  '{}'".format(home))
    print("   connecting to crossbar at:  '{}'".format(cb_url))
    print("       realm:  '{}'".format(realm))
    print("       server cert:  '{}'".format(options.cert))
    # print("       authid: '{}'".format(authid))
    print("   db url: '{}'".format(options.db_url))
    print("   ldap url: '{}'".format(config.get('ldap_url', '[not set]')))
    print("   base_dn: '{}'".format(config.get('base_dn', '[not set]')))
    print("   test: '{}'".format(str(test)))
    print("   debug: '{}'".format(str(debug)))
    print("   console: '{}'".format(str(console)))
    # load crossbar host certificate (default: file 'server_cert.pem' in
    # home directory)
    cert_fname = options.cert or 'server_cert.pem'
    cert_fpath = os.path.join(home, cert_fname)
    cert_content = crypto.load_certificate(crypto.FILETYPE_PEM,
                                           str(open(cert_fpath, 'r').read()))
    try:
        tls_options = CertificateOptions(
                    trustRoot=OpenSSLCertificateAuthorities([cert_content]))
    except:
        print("Could not get tls_options from server cert -- exiting.")
        sys.exit()
    # comp = Component(session_factory=RepositoryService,
                     # transports=[{
                        # "type": "websocket",
                        # "url": cb_url,
                        # "endpoint": {
                            # "type": "tcp",
                            # "host": cb_host,
                            # "port": cb_port,
                            # "tls":  tls_options},
                        # "serializers": ["json"]
                            # }],
                     # realm=realm,
                     # extra=extra)
    # run([comp])

    print("Connecting to {}: realm={}".format(cb_url, realm))
    runner = ApplicationRunner(url=cb_url, realm=realm, extra=extra,
                               ssl=tls_options)
    runner.run(RepositoryService)

