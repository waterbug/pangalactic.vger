#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
The Virtual Galactic Entropy Reverser
"""
import argparse, atexit, json, os, sqlite3, sys, traceback
from uuid import uuid4

import ruamel_yaml as yaml

from louie import dispatcher

from twisted.internet.defer import inlineCallbacks
from twisted.internet._sslverify import OpenSSLCertificateAuthorities
from twisted.internet.ssl import CertificateOptions

from OpenSSL import crypto

from autobahn.twisted.component import Component, run
from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp         import cryptosign
from autobahn.wamp.types   import RegisterOptions

from pangalactic.core                  import __version__
from pangalactic.core                  import (config, deleted, state,
                                               read_config, write_config,
                                               read_deleted, write_deleted,
                                               write_state)
from pangalactic.core.access           import (get_perms, is_cloaked,
                                               is_global_admin, modifiables)
from pangalactic.core.mapping          import schema_maps
from pangalactic.core.parametrics      import (add_default_parameters,
                                               add_default_data_elements,
                                               componentz, systemz,
                                               data_elementz, parameterz,
                                               de_defz,
                                               delete_parameter,
                                               delete_data_element,
                                               mode_defz,
                                               parm_defz, parmz_by_dimz,
                                               req_allocz,
                                               serialize_compz,
                                               serialize_systemz,
                                               serialize_req_allocz,
                                               set_dval, set_pval)
from pangalactic.core.serializers      import (DESERIALIZATION_ORDER,
                                               deserialize, serialize,
                                               uncook_datetime)
from pangalactic.core.refdata          import ref_oids
from pangalactic.core.test.utils       import (create_test_users,
                                               create_test_project)
from pangalactic.core.utils.datetimes  import dtstamp, earlier
from pangalactic.core.uberorb          import orb
from pangalactic.vger.userdir          import search_ldap_directory


test_mel_parms = ['m', 'P', 'R_D',
                  'm[CBE]', 'm[Ctgcy]', 'm[MEV]',
                  'P[CBE]', 'P[Ctgcy]', 'P[MEV]',
                  'R_D[CBE]', 'R_D[Ctgcy]', 'R_D[MEV]',
                  'Cost']
test_mel_des = ['Vendor', 'TRL']

# Default minimum client version is the current version, but this can be
# modified for a particular release if appropriate
MINIMUM_CLIENT_VERSION = __version__


class RepositoryService(ApplicationSession):
    """
    The Pan Galactic Engineering Repository Service container object
    (Application Session)

    Note that the canonical way to add application-specific reference data is
    to place file(s) containing serialized objects representing the reference
    data into the "extra_data" directory, which the server will load when
    started or restarted.  This applies particularly to DataElementDefinition,
    ParameterDefinition, and ParameterContext objects but can apply to any
    reference data.  For Parameters and Data Elements, these are critical since
    the repository will ignore any parameter or data element instance data for
    which corresponding definition objects do not exist.
    """
    # NOTE to developers:  
    # For the serialization structure of PGEF domain class definitions, see
    # pangalactic.meta.registry._update_schemas_from_extracts

    def __init__(self, *args, **kw):
        """
        NOTE:  orb home directory and database connection url must be
        specified.
        """
        super(RepositoryService, self).__init__(*args, **kw)
        home = kw.get('home', config.get('home')) or ''
        local_user = kw.get('local_user') or config.get('local_user', 'scred')
        db_url = kw.get('db_url') or config.get('db_url',
                        f'postgresql://{local_user}@localhost:5432/vgerdb')
        debug = kw.get('debug') or config.get('debug', True)
        console = kw.get('console') or config.get('console', True)
        test = kw.get('test') or config.get('test', True)
        ldap_url = kw.get('ldap_url') or config.get('ldap_url', '[not set]')
        base_dn = kw.get('base_dn') or config.get('base_dn', '[not set]')
        config['cb_host'] = cb_host
        config['cb_port'] = cb_port
        # start the orb ...
        orb.start(home=home, debug=debug, console=console, db_url=db_url)
        orb.log.info("* vger starting with")
        orb.log.info(f"    home directory:  '{home}'")
        orb.log.info(f"    connecting to crossbar at:  '{cb_url}'")
        orb.log.info(f"        realm:  '{realm}'")
        orb.log.info("        server cert:  'server_cert.pem'")
        orb.log.info(f"    db url: '{db_url}'")
        orb.log.info(f"    ldap url: '{ldap_url}'")
        orb.log.info(f"    base_dn: '{base_dn}'")
        orb.log.info(f"    test: '{test}'")
        orb.log.info(f"    debug: '{debug}'")
        orb.log.info(f"    console: '{console}'")
        orb.log.info('* checking for test users ...')
        # always load test users steve, zaphod, buckaroo, etc.
        deserialize(orb, create_test_users())
        orb.log.info('  test users loaded.')
        if test:
            # check whether test objects have been loaded
            if state.get('test_project_loaded'):
                orb.log.info('* H2G2 objects already loaded.')
            else:
                # set default parms for create_test_project
                orb.log.info('* loading H2G2 objects ...')
                deserialize(orb, create_test_project())
                hw = orb.search_exact(cname='HardwareProduct', id_ns='test')
                orb.assign_test_parameters(hw, parms=test_mel_parms,
                                           des=test_mel_des)
                state['test_project_loaded'] = True
        # create an "uploads" directory if there isn't one
        self.uploads_path = os.path.join(orb.home, 'vault', 'uploads')
        if not os.path.exists(self.uploads_path):
            os.makedirs(self.uploads_path)
        # load data from "extra_data" dir
        extra_data_path = os.path.join(orb.home, 'extra_data')
        if not os.path.exists(extra_data_path):
            # if 'extra_data' dir is not found, check current directory for it
            extra_data_path = 'extra_data'
        already_loaded = state.get('extra_data_loaded') or []
        if os.path.exists(extra_data_path) and os.listdir(extra_data_path):
            orb.log.info('* "extra_data" is present, checking ...')
            extra_data_fnames = os.listdir(extra_data_path)
            extra_data_fnames.sort()
            # set flag for new HW (triggers check for deprecated data)
            hw_added = False
            for fname in extra_data_fnames:
                if fname.endswith('.yaml'):
                    orb.log.info(f'  - found "{fname}"')
                    if fname in already_loaded:
                        orb.log.info('    + previously loaded, skipping ...')
                        continue
                    orb.log.info('    + loading ...')
                    fpath = os.path.join(extra_data_path, fname)
                    with open(fpath) as f:
                        data = f.read()
                        sobjs = yaml.safe_load(data)
                        try:
                            objs = deserialize(orb, sobjs)
                            orb.log.info('    successfully deserialized.')
                            if objs:
                                n = len(objs)
                                orb.log.info(f'    loaded {n} objs.')
                                # ensure all HW items have default parms/des
                                HW = orb.classes['HardwareProduct']
                                hw = [o for o in objs if isinstance(o, HW)]
                                if hw:
                                    hw_added = True
                                    msg = '- adding default parameters and '
                                    msg += 'data elements to HW products ... '
                                    orb.log.info(f'    {msg}')
                                    for product in hw:
                                        add_default_parameters(product)
                                        add_default_data_elements(product)
                                        # freeze all non-project specs
                                        Project = orb.classes['Project']
                                        if not isinstance(product.owner,
                                                          Project):
                                            product.frozen = True
                                    orb.db.commit()
                                orb.log.info('    done.')
                            else:
                                msg = '0 new or modified objs in data.'
                                orb.log.info('    {}'.format(msg))
                        except:
                            orb.log.info('    exception in deserializing ...')
                            orb.log.info(traceback.format_exc())
                    already_loaded.append(fname)
                    state['extra_data_loaded'] = already_loaded
            if hw_added:
                orb.remove_deprecated_data()
        else:
            if not os.path.exists(extra_data_path):
                orb.log.info('* "extra_data" dir not found.')
            elif not os.listdir(extra_data_path):
                orb.log.info('* "extra_data" dir was empty.')
        # =====================================================================
        # load "deleted" cache from file, if it exists, and check that the oids
        # referenced in that file do not exist in the db, or if so delete them.
        path_of_deleted = os.path.join(home, 'deleted')
        if os.path.exists(path_of_deleted):
            read_deleted(path_of_deleted)
        self.audit_deletions()
        # =====================================================================
        # check HardwareProduct and Template 'id' attributes to make sure they
        # are consistent with the current owners and ProductType abbreviations
        orb.log.info('* validating all HW and Template ids ...')
        hw = orb.get_by_type('HardwareProduct')
        hw += orb.get_by_type('Template')
        id_mods = 0
        dts = dtstamp()
        id_corrections = []
        for p in hw:
            generated_id = orb.gen_product_id(p)
            if p.id != generated_id:
                p.id = generated_id
                p.mod_datetime = dts
                id_corrections.append(generated_id)
        if id_corrections:
            # NOTE: because the mod_datetimes have been updated, all clients
            # will get the new hw object ids when they sync.
            orb.db.commit()
            id_mods = len(id_corrections)
            orb.log.info(f'  ids corrected for {id_mods} HW & Template items:')
            id_corrections.sort()
            for id_ in id_corrections:
                orb.log.info(f'  -> {id_}')
        else:
            orb.log.info('  all HW and Template ids are correct.')
        # =====================================================================
        # orb.dump_all()
        dispatcher.connect(self.on_log_info_msg, 'log info msg')
        dispatcher.connect(self.on_log_debug_msg, 'log debug msg')
        atexit.register(self.shutdown)
        # load private key (raw format)
        key_path = os.path.join(home, 'vger.key')
        try:
            self._key = cryptosign.CryptosignKey.from_file(key_path)
        except Exception as e:
            self.log.error("* could not load public key: {log_failure}",
                           log_failure=e)
            self.leave()
        else:
            self.log.info("* public key loaded: {}".format(
                                                    self._key.public_key()))

    def audit_deletions(self):
        """
        Audit the db to ensure that all oids in the "deleted" cache have indeed
        been deleted.
        """
        orb.log.info('* performing self-audit of deletions ...')
        supposed_to_be_deleted = list(set(deleted) & set(orb.get_oids()))
        if supposed_to_be_deleted:
            orb.log.info(f'  deletions needed: {supposed_to_be_deleted}')
            orb.delete(orb.get(oids=supposed_to_be_deleted))
        else:
            orb.log.info('  passed.')

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
        write_state(os.path.join(orb.home, 'state'))
        # dump_all() saves all caches and writes db to a yaml file
        orb.dump_all()

    def onConnect(self):
        self.log.info("* connected to crossbar ...")
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
                  # authid=None,
                  authextra=extra)

    def onChallenge(self, challenge):
        self.log.info("* authentication challenge received ...")
        # sign the challenge with our private key.
        signed_challenge = self._key.sign_challenge(challenge)
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
                dict of dicts, in ]the form:
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
                ser_ra = serialized_ra[0]
                orb.log.info(str(ser_ra))
            except:
                orb.log.info('  deserialization failed.')
                return {'result': 'nothing saved.'}
            userid = getattr(cb_details, 'caller_authid', 'unknown')
            orb.log.info('  caller authid: {}'.format(str(userid)))
            user_obj = orb.select('Person', id=userid)
            org_oid = ser_ra.get('role_assignment_context')
            admin_role = orb.get('pgefobjects:Role.Administrator')
            if org_oid:
                # is user an Administrator for this org or a global Admin?
                org = orb.get(org_oid)
                admin_ra = orb.select('RoleAssignment',
                                      assigned_role=admin_role,
                                      assigned_to=user_obj,
                                      role_assignment_context=org)
                if admin_ra or is_global_admin(user_obj):
                    orb.log.info('  role assignment is authorized, saving ...')
                    output = deserialize(orb, [ser_ra], dictify=True)
                    mod_ra_dts = {}
                    new_ra_dts = {}
                    # for mod_ra in output['modified']:
                    if output['modified']:
                        mod_ra = output['modified'][0]
                        orb.log.info('   modified ra oid: {}'.format(
                                                                mod_ra.oid))
                        orb.log.info('                id: {}'.format(
                                                                mod_ra.id))
                        # content = (mod_ra.oid, mod_ra.id,
                                   # str(mod_ra.mod_datetime))
                        # role assignments are always "public"
                        orb.log.info('   publishing mod ra on public channel.')
                        self.publish('vger.channel.public',
                                     {'modified': [ser_ra]})
                        mod_ra_dts[mod_ra.oid] = str(mod_ra.mod_datetime)
                    # for new_ra in output['new']:
                    elif output['new']:
                        new_ra = output['new'][0]
                        orb.log.info('   new ra oid: {}'.format(new_ra.oid))
                        orb.log.info('           id: {}'.format(new_ra.id))
                        new_ra_dts[new_ra.oid] = str(new_ra.mod_datetime)
                        log_msg = 'new ra {} on public channel.'.format(
                                                                new_ra.id)
                        orb.log.info('   {}'.format(log_msg))
                        self.publish('vger.channel.public', {'decloaked':
                                                             [ser_ra]})
                                                      # [new_ra.oid, new_ra.id]})
                    return dict(new_obj_dts=new_ra_dts, mod_obj_dts=mod_ra_dts)
                else:
                    orb.log.info('  role assignment not authorized.')
            else:
                # the ra is Global Admin, can only be assigned by another
                # Global Admin ...
                if is_global_admin(user_obj):
                    orb.log.info('  global admin assignment is authorized ...')
                    output = deserialize(orb, [ser_ra], dictify=True)
                    mod_ra_dts = {}
                    new_ra_dts = {}
                    # ignore mod_ra_dts (a global admin ra can be created or
                    # deleted, but not modified)
                    if output['new']:
                        new_ra = output['new'][0]
                        orb.log.info('   new ra oid: {}'.format(new_ra.oid))
                        orb.log.info('           id: {}'.format(new_ra.id))
                        new_ra_dts[new_ra.oid] = str(new_ra.mod_datetime)
                        log_msg = 'new ra {} on public channel.'.format(
                                                                new_ra.id)
                        orb.log.info('   {}'.format(log_msg))
                        self.publish('vger.channel.public', {'decloaked':
                                                             [ser_ra]})
                                                      # [new_ra.oid, new_ra.id]})
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
            n = len(data or b'')
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
            # ================================================================
            # first, check to make sure none of the submitted oids are in the
            # "deleted" cache ...
            unauth_ids = []
            for oid, so in sobjs_unique.items():
                if oid in deleted and so in sobjs:
                    unauth_ids.append(so.get('id') or 'unknown_id')
                    sobjs.remove(so)
                    orb.log.info(f'  "{oid}" was in "deleted" cache; ignored.')
            if not sobjs:
                orb.log.info('  all oids submitted were in "deleted".')
                return dict(new_obj_dts={}, mod_obj_dts={}, unauth=unauth_ids,
                            no_owners=[])
            # ================================================================
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
            # objects created by the user
            authorized = {oid:so for oid, so in sobjs_unique.items()
                          if so.get('creator') == user_oid}
            # existing objects for which the user has 'modify' permission
            for oid, so in sobjs_unique.items():
                obj_in_repo = orb.get(so.get('oid'))
                perms = get_perms(obj_in_repo, user=user_obj)
                if 'modify' in perms:
                    # orb.log.info('  auth (perms: {})'.format(str(perms)))
                    authorized[oid] = so
            # instances of classes which anyone can modify
            for oid, so in sobjs_unique.items():
                if so['_cname'] in modifiables:
                    authorized[oid] = so
            # everything else is unauthorized
            unauthorized = {oid:so for oid, so in sobjs_unique.items()
                            if oid not in authorized}
            unauth_ids += [unauthorized[oid].get('id', 'no id')
                           for oid in unauthorized]
            if not authorized:
                orb.log.info('  no save: {} unauthorized object(s).'.format(
                                                          len(unauthorized)))
                return dict(new_obj_dts={}, mod_obj_dts={}, unauth=unauth_ids,
                            no_owners=no_owners)
            output = deserialize(orb, authorized.values(), dictify=True)
            # ================================================================
            # special case for PSUs in the "SANDBOX" project: DO NOT send
            # messages to other users regarding ProjectSystemUsages for which
            # the "project" is the SANDBOX project (oid: "pgefobjects:SANDBOX")
            # ...  those object have been saved by the deserialize() above, but
            # should be removed from the output ...
            # ================================================================
            sb_oid = "pgefobjects:SANDBOX"
            for label in ['new', 'modified', 'unmodified', 'error']:
                if output.get(label):
                    psus = [o for o in output[label]
                        if isinstance(o, orb.classes['ProjectSystemUsage'])]
                    if psus:
                        for psu in psus:
                            if getattr(psu.system, 'oid', '') == sb_oid:
                                output[label].remove(psu)
            # ================================================================
            mod_obj_dts = {}
            new_obj_dts = {}
            # the "new_objs" and "mod_obj" dicts need to group object dicts by
            # the channels on which they will be published:
            # {'public': {oid: id, ...}, 'org1': {oid: id, ...}, 'org2': ...}
            new_objs = {'public': []}
            mod_objs = {'public': []}
            new_obj_ids = []
            for mod_obj in output['modified']:
                # orb.log.info(f'   modified object oid: {mod_obj.oid}')
                # orb.log.info(f'                    id: {mod_obj.id}')
                # content = (mod_obj.oid, mod_obj.id,
                           # str(mod_obj.mod_datetime))
                # if the object has a public attr set to True or does not have
                # a 'public' attr*, it is public unless it is a SANDBOX PSU.
                # NOTE:  * this includes Acu and non-SANDBOX PSU objects
                if is_cloaked(mod_obj):
                    # orb.log.info('   cloaked: only owner org has access:')
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
                            # 2.2.dev8: add serialized object, not id
                            # mod_objs[owner_id][mod_obj.oid] = mod_obj.id
                            mod_objs[owner_id].append(authorized[mod_obj.oid])
                        else:
                            # mod_objs[owner_id] = {mod_obj.oid: mod_obj.id}
                            mod_objs[owner_id] = [authorized[mod_obj.oid]]
                    # else:
                        # orb.log.info('   not publishing -- no owner org.')
                else:
                    mod_objs['public'].append(authorized[mod_obj.oid])
                    # orb.log.info('   + modified object is public, publishing')
                    # orb.log.info('     "modified" on public channel ...')
                    # channel = 'vger.channel.public'
                    # self.publish(channel, {'modified': content})
                mod_obj_dts[mod_obj.oid] = str(mod_obj.mod_datetime)
            for owner_id in mod_objs:
                # content is now simply a list of serialized objects
                obj_ids = [so.get('id') or "unknown"
                           for so in mod_objs[owner_id]]
                if owner_id == 'public':
                    channel = 'vger.channel.public'
                    orb.log.info(f'   + public objects, publishing {obj_ids}')
                    orb.log.info('     "modified" on public channel ...')
                else:
                    channel = 'vger.channel.' + owner_id
                    orb.log.info(f'   + cloaked objects, publishing {obj_ids}')
                    orb.log.info(f'     "modified" on "{channel}" channel.')
                self.publish(channel, {'modified': mod_objs[owner_id]})
            for new_obj in output['new']:
                # orb.log.info('   new object oid: {}'.format(new_obj.oid))
                # orb.log.info('               id: {}'.format(new_obj.id))
                new_obj_ids.append(new_obj.id)
                # content = (new_obj.oid, new_obj.id,
                           # str(new_obj.mod_datetime))
                if is_cloaked(new_obj):
                    # orb.log.info(f'   + new object oid: {new_obj.oid}')
                    # orb.log.info('     new object is cloaked -- ')
                    owner_id = ''
                    if isinstance(new_obj, orb.classes['ManagedObject']):
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
                        # msg = '   + publishing "new" only to owner org: "{}"'
                        # orb.log.info(msg.format(owner_id))
                        if owner_id in new_objs:
                            new_objs[owner_id].append(authorized[new_obj.oid])
                        else:
                            new_objs[owner_id] = [authorized[new_obj.oid]]
                else:
                    orb.log.info('   + new object is public --')
                    orb.log.info('     publishing on public channel ...')
                    new_objs["public"].append(authorized[new_obj.oid])
                    # NOTE: this stuff is probably unnecessary, now that the
                    # "Relation" and "ParameterRelation" classes are modifiable
                    # by anyone ... still testing that. [2022-03-02 SCW]
                    # # Requirements are always "public" (not cloaked) --
                    # # add related objects for performance requirements (in
                    # # future, Requirement needs refactoring to avoid this)
                    # # [1] find the related Relation object
                    # # [2] find the Relation's ParameterRelation object
                    # # [3] add them both to what is being published
                    # if (isinstance(new_obj, orb.classes['Requirement'])
                        # and (new_obj.req_type == 'performance')):
                        # for o in output['new']:
                            # if (isinstance(o, orb.classes['Relation'])
                            # and (new_obj.computable_form.oid == o.oid)):
                                # new_objs[owner_id].append(
                                                    # authorized[o.oid])
                            # for oo in output['new']:
                                # if (isinstance(oo,
                                     # orb.classes['ParameterRelation'])
                                # and (oo.referenced_relation.oid == o.oid)):
                                    # new_objs[owner_id].append(
                                                    # authorized[oo.oid])

                new_obj_dts[new_obj.oid] = str(new_obj.mod_datetime)
            # publish "decloaked" messages for new objects here ...
            for org_id in new_objs:
                if org_id == 'public' and new_objs['public']:
                    # publish decloaked for new public objs on public channel
                    n = len(new_objs['public'])
                    txt = f'publishing {n} decloaked items on public channel'
                    orb.log.info('   + {}'.format(txt))
                    self.publish('vger.channel.public',
                                 {'decloaked': new_objs["public"]})
                elif (not org_id == 'public') and new_objs[org_id]:
                    # if not public, publish "new" on owner org channel
                    channel = 'vger.channel.' + org_id
                    n = len(new_objs[org_id])
                    txt = f'publishing {n} items on channel "{org_id}"'
                    orb.log.info(f'   + {txt}')
                    if new_obj_ids:
                        orb.log.debug('     new object ids:')
                        for obj_id in new_obj_ids:
                            orb.log.debug(f'     - {obj_id}')
                    self.publish(channel, {'new': new_objs[org_id]})
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
            # attribute only be deleted by a Global Admin -> only instances of
            # subclasses of 'Modelable' can be deleted by ordinary users.
            admin_role = orb.get('pgefobjects:Role.Administrator')
            if is_global_admin(user):
                orb.log.info('  caller is a global admin')
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
            # add oids of objects to be deleted to the 'deleted' cache
            if auth_dels:
                auth = list(auth_dels)
                orb.log.info(f'  authorized to delete: {auth}')
                for oid, obj in auth_dels.items():
                    deleted[oid] = obj.id
                write_deleted(os.path.join(orb.home, 'deleted'))
            oids_deleted = list(auth_dels.keys())
            objs_to_delete = list(auth_dels.values())
            orb.delete(objs_to_delete)
            for oid in oids_deleted:
                orb.log.info('   publishing "deleted" msg to public channel.')
                channel = 'vger.channel.public'
                self.publish(channel, {'deleted': oid})
            orb.log.info(f'  deleted: {oids_deleted}')
            return (oids_not_found, oids_deleted)

        yield self.register(delete, 'vger.delete',
                            RegisterOptions(details_arg='cb_details'))

        def freeze(oids, cb_details=None):
            """
            Freezes a set of objects.

            Args:
                oids (list of str):  oids of the objects to freeze

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                result (tuple of lists):  frozen obj attrs, unauthorized oids
                    where frozen obj attrs is a tuple of
                    (obj.oid, str(obj.mod_datetime), obj.modifier.oid)
            """
            orb.log.info('* vger.freeze({})'.format(str(oids)))
            if not oids:
                orb.log.info('  called with no oids, nothing frozen.')
                return ([], [])
            # TODO:  check that user has permission to freeze
            userid = getattr(cb_details, 'caller_authid', None)
            user = orb.select('Person', id=userid)
            unauth, frozens, frozen_oids = [], [], []
            dts = dtstamp()
            channel = 'vger.channel.public'
            for obj in orb.get(oids=oids):
                if 'modify' in get_perms(obj, user=user):
                    orb.log.info(f'  - freeze authorized for {obj.oid}.')
                    obj.frozen = True
                    obj.mod_datetime = dts
                    obj.modifier = user
                    obj_attrs = (obj.oid, str(dts), user.oid)
                    frozens.append(obj_attrs)
                    frozen_oids.append(obj.oid)
                else:
                    orb.log.info(f'  - freeze NOT authorized for {obj.oid}.')
                    unauth.append(obj.oid)
            orb.db.commit()
            orb.log.info(f'  frozen: {str(frozen_oids)}')
            orb.log.info(f'  unauth: {str(unauth)}')
            if frozens:
                msg = 'publishing "frozen" to public channel.'
                orb.log.info(f'   {msg}')
                channel = 'vger.channel.public'
                self.publish(channel, {'frozen': frozens})
            return frozens, unauth

        yield self.register(freeze, 'vger.freeze',
                            RegisterOptions(details_arg='cb_details'))

        def thaw(oids, cb_details=None):
            """
            Thaws a set of objects.

            Args:
                oids (list of str):  oids of the objects to thaw

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                result (tuple of lists):  thawed obj attrs, unauthorized oids
                    where thawed obj attrs is a tuple of
                    (obj.oid, str(obj.mod_datetime), obj.modifier.oid)
            """
            orb.log.info('* vger.thaw({})'.format(str(oids)))
            if not oids:
                orb.log.info('  called with no oids, nothing thawed.')
                return ([], [])
            userid = getattr(cb_details, 'caller_authid', None)
            user = orb.select('Person', id=userid)
            if not is_global_admin(user):
                orb.log.info('  caller is not global admin; ignored.')
                return ([], oids)
            objs = orb.get(oids=oids)
            thawed, failed = [], []
            dts = dtstamp()
            for obj in objs:
                try:
                    obj.frozen = False
                    obj.mod_datetime = dts
                    obj.modifier = user
                    obj_attrs = (obj.oid, str(dts), user.oid)
                    thawed.append(obj_attrs)
                except:
                    failed.append(obj.oid)
            orb.db.commit()
            orb.log.info(f'  thawed: {str(thawed)}')
            orb.log.info(f'  failed: {str(failed)}')
            if thawed:
                orb.log.info('   publishing "thawed" to public channel.')
                channel = 'vger.channel.public'
                self.publish(channel, {'thawed': thawed})
            return (thawed, failed)

        yield self.register(thaw, 'vger.thaw',
                            RegisterOptions(details_arg='cb_details'))

        def sync_objects(data, cb_details=None):
            """
            Sync the objects referenced by the data.  NOTE:  oids in the data
            that are unknown to the server will be returned in the 4th element
            of the result (i.e., [3] in the result specification below).  Any
            oids in the 'deleted' cache will trigger a "deleted" message to be
            published.

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
                    [4]:  all oids in the "deleted" cache
                    [5]:  parameter data for all objs requested
                    [6]:  data element data for all objs requested
            """
            orb.log.info('* [rpc] vger.sync_objects(data)')
            result = [[], [], [], [], [], {}, {}]
            if not data:
                orb.log.info('  no data sent; returning empty result.')
                return result
            n = len(data)
            orb.log.info(f'  received {n} items in data')
            # ================================================================
            # first, a quick self-audit to make sure all oids in the "deleted"
            # cache have really been deleted from the db ...
            self.audit_deletions()
            # ================================================================
            # if any oids in data are in 'deleted', delete them
            for oid in deleted:
                if oid in data:
                    del data[oid]
            # remove any refdata
            non_ref = set(data.keys()) - set(ref_oids)
            data = {oid: data[oid] for oid in non_ref}
            # oids of objects unknown to the server
            unknown_oids = list(set(data) - set(orb.get_oids()))
            for oid in unknown_oids:
                del data[oid]
            # parameter and data element data
            parm_data = {oid: parameterz.get(oid) for oid in data}
            de_data = {oid: data_elementz.get(oid) for oid in data}
            # oids of newer objects on the server
            dts_by_oid = {oid: uncook_datetime(dt_str)
                          for oid, dt_str in data.items()}
            server_dts = {oid: dts for oid, dts
                          in orb.get_mod_dts(oids=list(data),
                                             datetimes=True).items()}
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
            deleted_oids = list(deleted)
            if newer_oids:
                newer_sobjs = serialize(orb, orb.get(oids=newer_oids),
                                        include_components=True)
                result = [newer_sobjs, same_oids, older_oids, unknown_oids,
                          deleted_oids, parm_data, de_data]
            else:
                result = [[], same_oids, older_oids, unknown_oids,
                          deleted_oids, parm_data, de_data]
            n_newer = len(result[0])
            n_same = len(result[1])
            n_older = len(result[2])
            n_unknown = len(result[3])
            n_deleted = len(result[4])
            n_obj_parms = len(result[5])
            n_obj_data = len(result[6])
            # orb.log.info('   result: {}'.format(str(result)))
            orb.log.info('   result: of the objects with oids in data ...')
            orb.log.info(f'   - {n_newer} have a newer copy on the server,')
            orb.log.info(f'   - {n_same} are the same as the server copies,')
            orb.log.info(f'   - {n_older} have an older copy on the server,')
            orb.log.info(f'   - {n_unknown} are unknown to the server.')
            orb.log.info('     ... also included are:')
            orb.log.info(f'   - {n_deleted} oids from the "deleted" cache')
            orb.log.info(f'   - parameters for {n_obj_parms} objects')
            orb.log.info(f'   - data for {n_obj_data} objects')
            return result

        yield self.register(sync_objects, 'vger.sync_objects',
                            RegisterOptions(details_arg='cb_details'))

        def sync_library_objects(data, cb_details=None):
            """
            Sync all non-project-owned instances of classes for which libraries
            are used -- these include HardwareProduct, Template,
            DataElementDefinition, and Model.  (ParameterDefinitions are
            considered "reference data" and are not created by users at runtime
            because of the need for standardization, even though they do have
            associated libraries.)

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
                          [b] created by the user but are in 'deleted' cache.
                    [2]:  parameter data for all "public" objects known to the
                          server
                    [3]:  data element data for all "public" objects known to
                          the server
                    [4]:  all mode definitions (serialized "mode_defz" cache)
                    [5]:  datetime stamp for mode definitions ("mode_defz_dts")
            """
            orb.log.info('* [rpc] vger.sync_library_objects()')
            data = data or {}
            n = len(data)
            orb.log.info(f'  received {n} item(s) in data')
            result = [[], [], {}, {}, '', '']
            # if any oids appear in "deleted" cache, publish a "deleted" msg
            for oid in deleted:
                if oid in data:
                    del data[oid]
                    orb.log.info(f'  found in "deleted" cache: oid "{oid}"')
                    orb.log.info('  publishing "deleted" message ...')
                    channel = 'vger.channel.public'
                    self.publish(channel, {'deleted': oid})
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
            all_public_oids = set([o.oid for o in
                                   orb.search_exact(public=True)])
            # include only HW, Templates, DEDs, and Models that are not owned
            # by projects
            projects = orb.get_by_type('Project')
            hw = orb.get_by_type('HardwareProduct')
            non_proj_hw = [o for o in hw if o.owner not in projects]
            hw_oids = set([o.oid for o in non_proj_hw])
            templates = orb.get_by_type('Template')
            non_proj_templates = [o for o in templates
                                  if o.owner not in projects]
            template_oids = set([o.oid for o in non_proj_templates])
            ded_oids = set(orb.get_oids(cname='DataElementDefinition'))
            models = orb.get_by_type('Model')
            non_proj_models = [o for o in models if o.owner not in projects]
            model_oids = set([o.oid for o in non_proj_models])
            all_lib_oids = hw_oids | ded_oids | template_oids | model_oids
            # exclude reference data
            public_lib_oids = list((all_lib_oids & all_public_oids)
                                    - set(ref_oids))
            # public_oids = list(set(public_lib_oids) - set(ref_oids))
            if public_lib_oids:
                server_dts = {oid: dts for oid, dts
                              in orb.get_mod_dts(oids=public_lib_oids,
                                                 datetimes=True).items()}
            parm_data = {oid: parameterz.get(oid) for oid in public_lib_oids}
            de_data = {oid: data_elementz.get(oid) for oid in public_lib_oids}
            md_data = json.dumps(mode_defz)
            md_dts = state.get('mode_defz_dts') or ''
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
                all_ord = (DESERIALIZATION_ORDER +
                             list(set(newer_oc.values()) -
                                  set(DESERIALIZATION_ORDER)))
                sorted_newer_oids = sorted(newer_oc, key=lambda x:
                                           all_ord.index(newer_oc.get(x)))
                result = [sorted_newer_oids, unknown_oids, parm_data, de_data,
                          md_data, md_dts]
                # orb.log.info('   result: {}'.format(str(result)))
            else:
                result = [[], unknown_oids, parm_data, de_data, md_data,
                          md_dts]
                # orb.log.info('   result: {}'.format(str(result)))
            n_newer = len(result[0])
            n_unknown = len(result[1])
            n_obj_parms = len(result[2])
            n_obj_data = len(result[3])
            orb.log.info('  result: of the oids sent to the server ...')
            orb.log.info(f'  - {n_newer} have a newer copy on the server,')
            orb.log.info(f'  - {n_unknown} are unknown to the server.')
            orb.log.info(f'  - parms retrieved for {n_obj_parms} objects.')
            orb.log.info(f'  - data retrieved for {n_obj_data} objects.')
            orb.log.info('  - mode_defz data and mode_defz_dts retrieved.')
            return result

        yield self.register(sync_library_objects, 'vger.sync_library_objects',
                            RegisterOptions(details_arg='cb_details'))

        def force_sync_managed_objects(data, cb_details=None):
            """
            Get all "public" instances of ManagedObject on the server,
            regardless of their mod_datetimes.

            NOTE:  the use of the keyword arg 'public' in orb.search_exact()
            implies that only instances of ManagedObject and its subclasses
            (Product, Template, etc.) will be returned.  Note also that this
            means PortTypes and PortTemplates libraries will not be synced,
            since they are not ManagedObjects, but they are also "reference
            data", so they should not be synced anyway.

            Args:
                data (dict):  dict {oid: str(mod_datetime)} for a set of
                    objects
                cb_details:  added by crossbar; not included in rpc signature

            Return:
                result (list of lists):  list containing:
                    [0]:  oids of all "public" ManagedObject instances
                    [1]:  any oids in data that were not found on the server --
                          the client app should delete these from the local db
                          if they are either
                          [a] not created by the user or
                          [b] created by the user but are in 'deleted' cache.
            """
            orb.log.info('* [rpc] vger.force_sync_managed_objects()')
            data = data or {}
            n = len(data)
            orb.log.info(f'  received {n} item(s) in data')
            # TODO: user object will be needed when more than "public" objects
            # are to be returned -- e.g., organizational product libraries to
            # which the user has access by having a role in the organization
            # user = None
            # userid = getattr(cb_details, 'caller_authid', '')
            # if userid:
                # user = orb.select('Person', id=userid)
            result = [[], []]
            # if any oids appear in "deleted" cache, publish a "deleted" msg
            for oid in deleted:
                if oid in data:
                    del data[oid]
                    orb.log.info(f'  found in "deleted" cache: oid "{oid}"')
                    orb.log.info('  publishing "deleted" message ...')
                    channel = 'vger.channel.public'
                    self.publish(channel, {'deleted': oid})
            # oids of objects unknown to the server (these would be objects
            # in data that were deleted on the server) -- the user app should
            # delete these from their local db (NOTE that this is the REVERSE
            # of the action taken by `sync_objects()`, which assumes they are
            # to be deleted on the server!!).
            unknown_oids = list(set(data) - set(orb.get_oids()))
            for oid in unknown_oids:
                del data[oid]
            # NOTE: for "force_sync_managed_objects", ALL public server objs
            # will be included, regardless of mod_datetimes
            all_public_oids = [o.oid for o in orb.search_exact(public=True)]
            # exclude reference data
            public_oids = list(set(all_public_oids) - set(ref_oids))
            if public_oids:
                server_oc = orb.get_oid_cnames(oids=public_oids)
                all_ord = (DESERIALIZATION_ORDER +
                             list(set(server_oc.values()) -
                                  set(DESERIALIZATION_ORDER)))
                sorted_server_oids = sorted(server_oc, key=lambda x:
                                            all_ord.index(server_oc.get(x)))
                result = [sorted_server_oids, unknown_oids]
                # orb.log.info('   result: {}'.format(str(result)))
            else:
                result = [[], unknown_oids]
                # orb.log.info('   result: {}'.format(str(result)))
            n_server = len(result[0])
            n_unknown = len(result[1])
            orb.log.info('  result: of the oids sent to the server ...')
            orb.log.info(f'  - {n_server} have a copy on the server,')
            orb.log.info(f'  - {n_unknown} are unknown to the server.')
            return result

        yield self.register(force_sync_managed_objects,
                            'vger.force_sync_managed_objects',
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
                    [4]:  all oids in the "deleted" cache
                    [5]:  parameter data for all project-owned objects
                    [6]:  data element data for all objs requested
            """
            orb.log.info('* [rpc] vger.sync_project() ...')
            orb.log.info('   project oid: {}'.format(str(project_oid)))
            userid = getattr(cb_details, 'caller_authid', '')
            if userid:
                user = orb.select('Person', id=userid)
            result = [[], [], [], [], [], {}, {}]
            if not project_oid or project_oid == 'pgefobjects:SANDBOX':
                orb.log.info('   no project oid or SANDBOX -- no result.')
                return result
            if not user:
                orb.log.info('   no user found -- cannot authorize.')
                return result
            project = orb.get(project_oid)
            ras = orb.search_exact(cname='RoleAssignment', assigned_to=user,
                                   role_assignment_context=project)
            if not ras and not is_global_admin(user):
                orb.log.info('   no project role nor GA -- not authorized.')
                return result
            admin_oid = 'pgefobjects:Role.Administrator'
            proj_admin = [ra for ra in ras
                          if ra.assigned_role.oid == admin_oid]
            project_ras = []
            if proj_admin or is_global_admin(user):
                # if user is a project admin or global admin, get all users'
                # role assignments on this project
                project_ras = orb.search_exact(cname='RoleAssignment',
                                               role_assignment_context=project)
            if project:
                data = data or {}
                n = len(data)
                orb.log.info(f'   received {n} items in data')
                # check data against "deleted" cache
                for oid in deleted:
                    if oid in data:
                        del data[oid]
                        orb.log.info(f'  in "deleted" cache: oid "{oid}"')
                same_oids = []
                older_oids = []
                unknown_oids = []
                server_objs = orb.get_objects_for_project(project)
                # add in role assignments (empty list if not admin)
                server_objs += project_ras
                server_oids = [o.oid for o in server_objs]
                # parameter and data element data for *ALL* project
                # objects, regardless of mod_datetime
                parm_data = {oid: parameterz.get(oid)
                             for oid in server_oids}
                de_data = {oid: data_elementz.get(oid)
                           for oid in server_oids}
                if data:
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
                deleted_oids = list(deleted)
                if newer_objs:
                    newer_sobjs = serialize(orb, newer_objs,
                                            include_components=True)
                    result = [newer_sobjs, same_oids, older_oids, unknown_oids,
                              deleted_oids, parm_data, de_data]
                else:
                    result = [[], same_oids, older_oids, unknown_oids,
                              deleted_oids, parm_data, de_data]
            else:
                orb.log.info('   ** project was not found on the server. **')
            n_newer = len(result[0])
            n_same = len(result[1])
            n_older = len(result[2])
            n_unknown = len(result[3])
            n_deleted = len(result[4])
            n_obj_parms = len(result[5])
            n_obj_data = len(result[6])
            # orb.log.info('   result: {}'.format(str(result)))
            orb.log.info('   result: of the oids/dts sent to the server ...')
            orb.log.info(f'   - {n_newer} have a newer copy on the server,')
            orb.log.info(f'   - {n_same} are the same as the server copies,')
            orb.log.info(f'   - {n_older} have an older copy on the server,')
            orb.log.info(f'   - {n_unknown} are unknown to the server.')
            orb.log.info('     ... also included are:')
            orb.log.info(f'   - {n_deleted} oids from the "deleted" cache')
            orb.log.info(f'   - parameters for {n_obj_parms} objects')
            orb.log.info(f'   - data for {n_obj_data} objects')
            return result

        yield self.register(sync_project, 'vger.sync_project',
                            RegisterOptions(details_arg='cb_details'))

        # NOTE: this rpc is currently unnecessary, since parameters are added
        # and/or set by object modifications, which are handled by vger.save()
        def set_parameter(oid=None, pid=None, value=None, cb_details=None):
            """
            Set a parameter value.

            Keyword Args:
                oid (str):  oid of the parent object
                pid (str):  parameter id
                value (str):  string representation of the value
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                result (str):  'success'
            """
            # argstr = f'oid={oid}, pid={pid}, value={value}'
            # orb.log.info(f'* [rpc] set_parameter({argstr})')
            # For now, just publish on public channel
            set_pval(oid, pid, value)
            channel = 'vger.channel.public'
            # orb.log.info(f'  + publishing parameter on "{channel}" ...')
            self.publish(channel,
                         {'parameter set':
                          [oid, pid, value]})
            return 'success'

        yield self.register(set_parameter, 'vger.set_parameter',
                            RegisterOptions(details_arg='cb_details'))

        # NOTE: this rpc is necessary because the serialization format allows
        # data to be incomplete, so absence does not imply deletion (and anyway
        # it is better to have an positive assertion for removals)
        def del_parm(oid=None, pid=None, cb_details=None):
            """
            Remove a parameter from an object.

            Keyword Args:
                oid (str):  oid of the parent object
                pid (str):  parameter id
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                result (str):  message about the result
            """
            argstr = f'oid={oid}, pid={pid}'
            orb.log.info(f'* [rpc] del_parm({argstr})')
            # For now, just publish on public channel
            delete_parameter(oid, pid)
            channel = 'vger.channel.public'
            orb.log.info(f'  + publishing "parm del" on "{channel}" ...')
            self.publish(channel,
                         {'parm del': [oid, pid]})
            return f'parameter "{pid}" removed from object "{oid}".'

        yield self.register(del_parm, 'vger.del_parm',
                            RegisterOptions(details_arg='cb_details'))

        def set_data_elements(des=None, cb_details=None):
            """
            Set data element values.

            Keyword Args:
                des (dict):  dict of data elements to update, in the format of
                    the data_elementz cache dict
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                result (str):  'success'
            """
            if not des or not isinstance(des, dict):
                return 'failure'
            userid = getattr(cb_details, 'caller_authid', 'unknown')
            user_obj = orb.select('Person', id=userid)
            try:
                for oid, dedict in des.items():
                    obj = orb.get(oid)
                    perms = get_perms(obj, user_obj)
                    if "modify" in perms:
                        for deid, value in dedict.items():
                            set_dval(oid, deid, value)
                channel = 'vger.channel.public'
                # publish on public channel
                # orb.log.info('  + publishing data elements to "public" ...')
                self.publish(channel,
                             {'data elements set': des})
                return 'success'
            except:
                return 'failure'

        yield self.register(set_data_elements, 'vger.set_data_elements',
                            RegisterOptions(details_arg='cb_details'))

        # NOTE: this rpc is necessary because the serialization format allows
        # data to be incomplete, so absence does not imply deletion (and anyway
        # it is better to have an positive assertion for removals)
        def del_de(oid=None, deid=None, cb_details=None):
            """
            Remove a data element from an object.

            Keyword Args:
                oid (str):  oid of the parent object
                deid (str):  data element id
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                result (str):  message about the result
            """
            argstr = f'oid={oid}, deid={deid}'
            orb.log.info(f'* [rpc] del_de({argstr})')
            # For now, just publish on public channel
            delete_data_element(oid, deid)
            channel = 'vger.channel.public'
            orb.log.info(f'  + publishing "de del" on "{channel}" ...')
            self.publish(channel,
                         {'de del': [oid, deid]})
            return f'data element "{deid}" removed from object "{oid}".'

        yield self.register(del_de, 'vger.del_de',
                            RegisterOptions(details_arg='cb_details'))

        def get_mode_defs():
            """
            Get the mode_defz cache.

            Returns:
                data (tuple of str):  [0] last-modified datetime stamp, [1]
                    serialized (yaml) mode_defz cache
            """
            data = yaml.safe_dump(mode_defz, default_flow_style=False)
            if not state.get('mode_defz_dts'):
                state['mode_defz_dts'] = str(dtstamp())
            dts = state['mode_defz_dts']
            return dts, data

        yield self.register(get_mode_defs, 'vger.get_mode_defs')

        def update_mode_defs(project_oid=None, data=None, cb_details=None):
            """
            Update the mode_defz cache.

            Keyword Args:
                project_oid (str):  oid of the project
                data (str):  serialized mode data for the project
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                dts (str):  stringified datetime stamp
            """
            orb.log.info('* [rpc] vger.update_mode_defs() ...')
            userid = getattr(cb_details, 'caller_authid', '')
            user = orb.select('Person', id=userid)
            # get role assignments in project
            project = orb.get(project_oid)
            if not project:
                return 'no such project'
            if not data:
                return 'no data submitted'
            pname = project.id
            orb.log.info(f'        mode defs data received for {pname}')
            # extremely verbose debugging, uncomment only if necessary
            # orb.log.info('============================================')
            # orb.log.info(f'{data}')
            # orb.log.info('============================================')
            ras = orb.search_exact(cname='RoleAssignment',
                                   assigned_to=user,
                                   role_assignment_context=project)
            role_names = set([ra.assigned_role.name for ra in ras])
            if ((set(['Administrator', 'Systems Engineer', 'Lead Engineer'])
                 & role_names) or is_global_admin(user)):
                mode_data = yaml.safe_load(data)
                if project_oid in mode_defz:
                    del mode_defz[project_oid]
                mode_defz[project_oid] = mode_data
                md_dts = str(dtstamp())
                state['mode_defz_dts'] = md_dts
                msg = 'publishing "new mode defs" on public channel ...'
                orb.log.info(f'    {msg}')
                channel = 'vger.channel.public'
                ser_mode_defs = json.dumps(mode_defz)
                self.publish(channel, {'new mode defs':
                                       (md_dts, ser_mode_defs, userid)})
                return md_dts
            else:
                return 'unauthorized'

        yield self.register(update_mode_defs, 'vger.update_mode_defs',
                            RegisterOptions(details_arg='cb_details'))

        def set_sys_mode_datum(project_oid=None, link_oid=None, mode=None,
                               value=None, cb_details=None):
            """
            Set the mode value for the specified system link in the specified
            project.

            Keyword Args:
                project_oid (str):  oid of the project
                link_oid (str):  oid of the system link (acu or psu)
                mode (str):  name of the mode
                value (str):  value of the mode for that link
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                dts (str):  stringified datetime stamp
            """
            orb.log.info('* [rpc] vger.set_sys_mode_datum() ...')
            userid = getattr(cb_details, 'caller_authid', '')
            user = orb.select('Person', id=userid)
            # get role assignments in project
            project = orb.get(project_oid)
            if not project:
                return 'no such project'
            pname = project.id
            # link retrieved for debug logging -- this can be removed to
            # improve performance after initial testing ...
            link = orb.get(link_oid)
            if not link:
                return 'unknown link'
            if link_oid not in mode_defz[project_oid]['systems']:
                return 'link not in system section'
            orb.log.info(f'        sys mode datum received for {pname}')
            orb.log.info('============================================')
            orb.log.info(f'system:  {link.name}')
            orb.log.info(f'mode:    {mode}')
            orb.log.info(f'value:   {value}')
            orb.log.info('============================================')
            ras = orb.search_exact(cname='RoleAssignment',
                                   assigned_to=user,
                                   role_assignment_context=project)
            role_names = set([ra.assigned_role.name for ra in ras])
            if ((set(['Administrator', 'Systems Engineer', 'Lead Engineer'])
                 & role_names) or is_global_admin(user)):
                mode_defz[project_oid]['systems'][link_oid][mode] = value
                md_dts = str(dtstamp())
                state['mode_defz_dts'] = md_dts
                msg = 'publishing "sys mode datum updated" ...'
                orb.log.info(f'    {msg}')
                channel = 'vger.channel.public'
                self.publish(channel, {'sys mode datum updated':
                                       (project_oid, link_oid, mode, value,
                                        md_dts, userid)})
                return md_dts
            else:
                return 'unauthorized'

        yield self.register(set_sys_mode_datum, 'vger.set_sys_mode_datum',
                            RegisterOptions(details_arg='cb_details'))

        def set_comp_mode_datum(project_oid=None, link_oid=None, comp_oid=None,
                                mode=None, value=None, cb_details=None):
            """
            Set the mode value for the specified system link in the specified
            project.

            Keyword Args:
                project_oid (str):  oid of the project
                link_oid (str):  oid of the system link (acu or psu)
                comp_oid (str):  oid of the component link (acu)
                mode (str):  name of the mode
                value (str):  value of the mode for that link
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                dts (str):  stringified datetime stamp
            """
            orb.log.info('* [rpc] vger.set_comp_mode_datum() ...')
            userid = getattr(cb_details, 'caller_authid', '')
            user = orb.select('Person', id=userid)
            # get role assignments in project
            project = orb.get(project_oid)
            if not project:
                return 'no such project'
            pname = project.id
            # link and comp retrieved for debug logging -- this can be removed
            # to improve performance after initial testing ...
            link = orb.get(link_oid)
            comp = orb.get(comp_oid)
            if not link:
                return 'unknown link'
            if not comp:
                return 'unknown comp'
            if link_oid not in mode_defz[project_oid]['components']:
                return 'link not in comp section'
            if comp_oid not in mode_defz[project_oid]['components'][link_oid]:
                return 'comp not in comp section'
            orb.log.info(f'        comp mode datum received for {pname}')
            orb.log.info('============================================')
            orb.log.info(f'system:     {link.name}')
            orb.log.info(f'component:  {comp.name}')
            orb.log.info(f'mode:       {mode}')
            orb.log.info(f'value:      {value}')
            orb.log.info('============================================')
            ras = orb.search_exact(cname='RoleAssignment',
                                   assigned_to=user,
                                   role_assignment_context=project)
            role_names = set([ra.assigned_role.name for ra in ras])
            if ((set(['Administrator', 'Systems Engineer', 'Lead Engineer'])
                 & role_names)
                or is_global_admin(user)):
                mode_defz[project_oid]['components'][link_oid][comp_oid][
                                                                mode] = value
                md_dts = str(dtstamp())
                state['mode_defz_dts'] = md_dts
                msg = 'publishing "comp mode datum updated" ...'
                orb.log.info(f'    {msg}')
                channel = 'vger.channel.public'
                self.publish(channel, {'comp mode datum updated':
                                       (project_oid, link_oid, comp_oid, mode,
                                        value, md_dts, userid)})
                return md_dts
            else:
                return 'unauthorized'

        yield self.register(set_comp_mode_datum, 'vger.set_comp_mode_datum',
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
            if oids:
                n = len(oids)
                orb.log.info(f'* [rpc] vger.get_objects({n} oids) ...')
            else:
                orb.log.info('* [rpc] vger.get_objects() ...')
                orb.log.info('        no oids in request, returning empty.')
                return []
            # TODO: use get_perms() to determine authorization
            userid = getattr(cb_details, 'caller_authid', '')
            user_obj = None
            if userid:
                user_obj = orb.select('Person', id=userid)
            # exclude all ref data
            non_ref_oids = list(set(oids) - set(ref_oids))
            objs = orb.get(oids=non_ref_oids)
            if objs:
                # TODO:  if include_components is True, get_perms() should be
                # used to determine the user's access to the components ...
                auth_objs = []
                for obj in objs:
                    if (getattr(obj, 'public', True)
                        or 'view' in get_perms(obj, user=user_obj)):
                        auth_objs.append(obj)
                return serialize(orb, auth_objs,
                                 include_components=include_components)
            else:
                return []

        yield self.register(get_objects, 'vger.get_objects',
                            RegisterOptions(details_arg='cb_details'))

        def get_caches(oids=None):
            """
            Retrieves all related caches for the objects with the specified
            oids (or the full caches if no oids are specified), which includes
            the 'componentz', 'systemz', 'parm_defz', 'de_defz',
            'parmz_by_dimz', and 'req_allocz' caches.

            Keyword Args:
                oids (iterable of str):  iterable of object oids

            Returns:
                list:  Serialized 'componentz' and 'systemz' data sets plus the
                    'parm_defz', 'de_defz', 'parmz_by_dimz', and 'req_allocz'
                    caches.
            """
            orb.log.info('* [rpc] vger.get_caches() ...')
            # "allocz" cache maps usage oids to oids of reqts allocated to them
            allocz = {}
            for req_oid, alloc in req_allocz.items():
                # alloc[0] is the usage oid in an allocation (alloc) record
                if alloc[0] in allocz:
                    allocz[alloc[0]].append(req_oid)
                else:
                    allocz[alloc[0]] = [req_oid]
            if oids:
                return [serialize_compz({oid: componentz.get(oid) for oid in oids
                                         if componentz.get(oid) is not None}),
                        serialize_systemz(systemz),
                        parm_defz, de_defz, parmz_by_dimz, 
                        serialize_req_allocz(req_allocz), allocz]
            # else get the full 'componentz' cache
            return [serialize_compz(componentz),
                    serialize_systemz(systemz),
                    parm_defz, de_defz, parmz_by_dimz,
                    serialize_req_allocz(req_allocz), allocz]

        yield self.register(get_caches, 'vger.get_caches')

        def get_parmz(oids=None):
            """
            Retrieves all cached parameter values for the specified oids, or
            if None, for all the oids in the db.

            Keyword Args:
                oids (iterable of str):  iterable of object oids

            Returns:
                dict:  parameterz data.
            """
            orb.log.info('* [rpc] vger.get_parmz() ...')
            if oids:
                return {oid: parameterz.get(oid) for oid in oids}
            else:
                return {oid: parameterz.get(oid) for oid in orb.get_oids()}

        yield self.register(get_parmz, 'vger.get_parmz')

        def get_mod_dts(cnames=None, oids=None):
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
            return orb.get_mod_dts(cnames=cnames, oids=oids)

        yield self.register(get_object, 'vger.get_mod_dts')

        def get_user_roles(userid, data=None, version=None, cb_details=None):
            """
            Get [0] the Person object that corresponds to the userid, [1] all
            Organization and Project objects, [2] all Person objects, and [3]
            either (a) for ordinary users, only the RoleAssignment objects for
            the specified user or (b) for administrators, all non-project
            RoleAssignment objects that correspond to the administrator's role
            (project-related RoleAssignments will be returned when the project
            is synced).

            Args:
                userid (str):  userid of a person (Person.id)
                    NOTE: the "userid" arg is a remnant of ticket-based
                    authentication; it is now ignored in favor of the
                    "caller_authid" from cb_details
                data (dict):  dict {oid: str(mod_datetime)}
                    for the requestor's Person, Organization, Project, and
                    RoleAssignment objects
                version (str):  caller version string -- if None, an exception
                    will be raised
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                tuple:  [0] serialized user (Person) object,
                        [1] serialized Organizations/Projects
                        [2] serialized Person objects
                        [3] serialized RoleAssignment objects
                        [4] unknown oids in data
                        [5] MINIMUM_CLIENT_VERSION (str)
            """
            orb.log.info('* [rpc] vger.get_user_roles({}) ...'.format(userid))
            if version is None:
                raise RuntimeError("client version is too old")
            data = data or {}
            same_dts = []
            unknown_oids = []
            userid = getattr(cb_details, 'caller_authid', '')
            orb.log.info(f'  login: userid "{userid}" ...')
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
                # set include_refdata=True in case user is "admin"
                szd_user = serialize(orb, [user], include_refdata=True)
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
            # RoleAssignment objects
            # (1) always return the user's direct role assignments
            ras = orb.search_exact(cname='RoleAssignment',
                                   assigned_to=user)
            orgs = orb.get_by_type('Organization')
            admin_role = orb.get('pgefobjects:Role.Administrator')
            # (2) if a global admin, return all non-project role assignments,
            #     plus all admin role assignments (for projects and orgs)
            if is_global_admin(user):
                all_ras = [ra for ra in orb.get_by_type('RoleAssignment')
                           if ra.oid not in same_dts]
                non_proj_ras = [ra for ra in all_ras
                                if ra.role_assignment_context in orgs]
                ras += non_proj_ras
                admin_ras = orb.search_exact(cname='RoleAssignment',
                                             assigned_role=admin_role)
                ras += admin_ras
            if ras:
                # use set() to remove duplicates from ras ...
                szd_ras = serialize(orb, set(ras))
            return [szd_user, szd_orgs, szd_people, szd_ras, unknown_oids,
                    MINIMUM_CLIENT_VERSION]

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
            if is_global_admin(user) and data:
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
                    # NOTE: if 'name' is not provided but 'first_name' etc.
                    # are, generate a 'name'
                    name = data.get('name')
                    if not name:
                        name = ''
                        name_tuple = [data.get(n, '')
                                      for n in ['first_name', 'mi_or_name',
                                      'last_name'] if data.get(n)]
                        if name_tuple:
                            data['name'] = ' '.join(name_tuple)
                    Person = orb.classes['Person']
                    person = Person(create_datetime=dts, mod_datetime=dts,
                                    **data)
                    orb.save([person], recompute=False)
                    saved_objs.append(person)
                if public_key:
                    default_auth_db_path = os.path.join(orb.home, 'crossbar',
                                                        'principals.db')
                    auth_db_path = config.get('auth_db_path',
                                                        default_auth_db_path)
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
                        orb.log.info('    could not add public key.')
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
                if not is_global_admin(user):
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
    cert_help = 'crossbar host cert file name [default: "server_cert.pem"].'
    parser = argparse.ArgumentParser()
    parser.add_argument('--home', dest='home', type=str,
                        help=home_help)
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
    # command options override config settings; if neither, defaults are used
    home = options.home or ''
    read_config(os.path.join(home, 'config'))
    config['home'] = home
    config['test'] = options.test or config.get('test', True)
    config['debug'] = options.debug or config.get('debug', True)
    config['console'] = options.console or config.get('console', False)
    # TODO:  take "local_user" option with default "scred"; use in db_url
    db_url = options.db_url or config.get('db_url',
                    'postgresql://scred@localhost:5432/vgerdb')
    config['db_url'] = db_url
    cb_host = options.cb_host or config.get('cb_host', 'localhost')
    cb_port = options.cb_port or config.get('cb_port', 8080)
    cb_url = f'wss://{cb_host}:{cb_port}/ws'
    config['cb_host'] = cb_host
    config['cb_port'] = cb_port
    config['cb_url'] = cb_url
    # router can auto-choose the realm, so unnecessary to specify but ...
    realm = 'pangalactic-services'
    # write the new config file
    write_config(os.path.join(home, 'config'))
    # load crossbar host certificate (default: file 'server_cert.pem' in
    # home directory)
    cert_fname = 'server_cert.pem'
    cert_fpath = os.path.join(home, cert_fname)
    cert_content = crypto.load_certificate(crypto.FILETYPE_PEM,
                                           str(open(cert_fpath, 'r').read()))
    try:
        tls_options = CertificateOptions(
                    trustRoot=OpenSSLCertificateAuthorities([cert_content]))
    except:
        print("Could not get tls_options from server cert -- exiting.")
        sys.exit()
    comp = Component(session_factory=RepositoryService,
                     transports=[{
                        "type": "websocket",
                        "url": cb_url,
                        "endpoint": {
                            "type": "tcp",
                            "host": cb_host,
                            "port": cb_port,
                            "tls":  tls_options},
                        "serializers": ["json"]
                            }],
                     realm=realm)
    run([comp])

