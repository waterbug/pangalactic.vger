#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
The Virtual Galactic Entropy Reverser
"""
import argparse, atexit, json, os, six, sys
from uuid import uuid4

from twisted.internet.defer import inlineCallbacks
from twisted.internet._sslverify import OpenSSLCertificateAuthorities
from twisted.internet.ssl import CertificateOptions

from OpenSSL import crypto

# from autobahn.wamp.types import RegisterOptions, PublishOptions
from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.types import RegisterOptions

from pangalactic.core                  import __version__
from pangalactic.core                  import (config, state, read_config,
                                               write_config, write_state)
from pangalactic.core.utils.meta       import uncook_datetime
from pangalactic.core.access           import get_orgs_with_access
# from pangalactic.core.access           import get_perms
from pangalactic.core.mapping          import schema_maps
from pangalactic.core.serializers      import deserialize, serialize
from pangalactic.core.uberorb          import orb
from pangalactic.core.refdata          import ref_pd_oids
from pangalactic.core.test.utils       import (create_test_users,
                                               create_test_project)
from pangalactic.core.utils.datetimes  import dtstamp, earlier
from pangalactic.vger.userdir          import search


TICKETS = {
    u'service1': u'789secret',
    u'service2': u'987secret'
    }

gsfc_mel_parms = [
            'm', 'P', 'R_D', 'Vendor', 'Cost', 'TRL']


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
        NOTE:  orb home directory and database connection url must be specified
        by the ApplicationRunner, which sets the 'self.config.extra' dict from
        its 'extra' keyword arg.
        """
        super(RepositoryService, self).__init__(*args, **kw)
        orb.start(home=self.config.extra[u'home'], gui=False,
                  db_url=self.config.extra[u'db_url'],
                  debug=self.config.extra['debug'])
        atexit.register(self.shutdown)
        # always load test users steve, zaphod, buckaroo, whorfin
        if not state.get('test_users_loaded'):
            orb.log.info('* [vger] loading test users ...')
            deserialize(orb, create_test_users())
            state['test_users_loaded'] = True
        else:
            orb.log.info('* [vger] test users already loaded.')
        if self.config.extra['test']:
            # check whether test objects have been loaded
            if state.get('test_project_loaded'):
                orb.log.info('* [vger] H2G2 objects already loaded.')
            else:
                # set default parms for create_test_project
                if not config.get('default_parms'):
                    config['default_parms'] = gsfc_mel_parms[:]
                orb.log.info('* [vger] loading H2G2 objects ...')
                deserialize(orb, create_test_project())
                hw = orb.search_exact(cname='HardwareProduct', id_ns='test')
                orb.assign_test_parameters(hw)
                state['test_project_loaded'] = True
            write_state(os.path.join(orb.home, 'state'))

    def shutdown(self):
        """
        When the server is killed, serialize the database contents to a json or
        yaml file (db.json|yaml) in the `vault` directory.  If the server is
        updated and the update includes a schema change, the orb will read,
        convert, and import this data into a new database when the server is
        restarted.
        """
        orb.dump_db()

    def onConnect(self):
        # self.config is set up by ApplicationRunner when it "runs" the session
        realm = self.config.realm
        orb.log.info("* realm set to: '%s'" % str(realm))
        authid = self.config.extra[u'authid']
        orb.log.info("* authid set to: '%s'" % str(authid))
        orb.log.info("* RepositoryService connected.")
        orb.log.info("  - joining realm <{}> under authid <{}>".format(
                                realm if realm else 'not provided', authid))
        self.join(realm, [u'ticket'], authid)

    def onChallenge(self, challenge):
        orb.log.info("* RepositoryService challenge received: {}".format(
                                                                    challenge))
        if challenge.method == u'ticket':
            return TICKETS.get(self.config.extra[u'authid'], None)
        else:
            raise Exception("Invalid authmethod {}".format(challenge.method))

    def on_vger_msg(self, msg):
        """
        Handle messages from the 'vger.channel.public' channel.
        """
        for item in msg.items():
            subject, content = item
            orb.log.info("* on_vger_msg")
            orb.log.info("      subject: {}".format(str(subject)))
            if subject == u'deleted':
                orb.log.info("      content: {}".format(str(content)))
                orb.log.info("      (taking no action)")
            # elif subject == u'decloaked':
                # obj_oid, obj_id, actor_oid, actor_id = content
            # elif subject == u'modified':
                # obj_oid, obj_id, obj_mod_datetime = content
            # elif subject == u'organization':
                # obj_oid = content[u'oid']
                # obj_id = content[u'id']

    @inlineCallbacks
    def onJoin(self, details):
        orb.log.info("* session attached")
        try:
            # TODO: include other per-organization channels ...
            yield self.subscribe(self.on_vger_msg, u'vger.channel.public')
        except:
            orb.log.info("  subscription to vger.channel.public failed.")

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
            orb.log.info('[rpc] vger.assign_role() ...')
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
            if org_oid:
                # is user an Administrator for this org?
                org = orb.get(org_oid)
                admin_role = orb.get('pgefobjects:Role.Administrator')
                admin_ra = orb.select('RoleAssignment',
                                      assigned_role=admin_role,
                                      assigned_to=user_obj,
                                      role_assignment_context=org)
                if admin_ra:
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
                        channel = u'vger.channel.public'
                        self.publish(channel, {u'modified': content})
                        mod_ra_dts[mod_ra.oid] = str(mod_ra.mod_datetime)
                    for new_ra in output['new']:
                        orb.log.info('   new ra oid: {}'.format(new_ra.oid))
                        orb.log.info('           id: {}'.format(new_ra.id))
                        orb.log.info('   publishing new ra on public channel.')
                        channel = u'vger.channel.public'
                        self.publish(channel, {u'decloaked':
                                         [new_ra.oid, new_ra.id,
                                          '', '']})
                        new_ra_dts[new_ra.oid] = str(new_ra.mod_datetime)
                    return dict(new_obj_dts=new_ra_dts, mod_obj_dts=mod_ra_dts)
                else:
                    orb.log.info('  role assignment not authorized.')
            else:
                orb.log.info('  no role_assignment_context found.')
                return {'result': 'nothing saved.'}

        yield self.register(assign_role, u'vger.assign_role',
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
                                      }
            """
            orb.log.info('[rpc] vger.save() ...')
            if not serialized_objs:
                orb.log.info('  called with nothing; returning.')
                return {'result': 'success.'}
            orb.log.info('  called for objects with object ids:')
            sobjs_list = ''
            for so in serialized_objs:
                sobjs_list += '   + {} ({})\n'.format(so['id'], so['_cname'])
            orb.log.info(sobjs_list)
            userid = getattr(cb_details, 'caller_authid', 'unknown')
            orb.log.info('  caller authid: {}'.format(str(userid)))
            user_obj = orb.select('Person', id=userid)
            user_oid = getattr(user_obj, 'oid', None)
            authorized_objs = [so for so in serialized_objs
                               if so.get('creator') == user_oid]
            if not authorized_objs:
                orb.log.info('  called with no authorized objs; returning.')
                return {'result': 'nothing saved.'}
            output = deserialize(orb, authorized_objs, dictify=True)
            mod_obj_dts = {}
            new_obj_dts = {}
            for mod_obj in output['modified']:
                orb.log.info('   modified object oid: {}'.format(mod_obj.oid))
                orb.log.info('                    id: {}'.format(mod_obj.id))
                content = (mod_obj.oid, mod_obj.id,
                           str(mod_obj.mod_datetime))
                # determine who has access to the object
                orgs = get_orgs_with_access(mod_obj)
                # if the object does not have a 'public' attr*, it is public
                # NOTE:  * this includes Acu and ProjectSystemUsage objects
                if getattr(mod_obj, 'public', True):
                    orb.log.info('   modified object is public -- ')
                    orb.log.info('   publish "modified" on public channel...')
                    channel = u'vger.channel.public'
                    self.publish(channel, {u'modified': content})
                else:
                    if orgs:
                        orb.log.info('   publish "modified" message ...')
                    else:
                        orb.log.info('   no orgs have access:')
                        orb.log.info('   not publishing "modified" message.')
                    for org in orgs:
                        # publish 'modified' message on relevant channels
                        org_id = getattr(org, 'id', '')
                        if org_id:
                            channel = u'vger.channel.' + str(org_id)
                        orb.log.info('   + on channel: {}'.format(channel))
                        self.publish(channel, {u'modified': content})
                mod_obj_dts[mod_obj.oid] = str(mod_obj.mod_datetime)
            for new_obj in output['new']:
                # ** NOTE: no orgs have access to "SANDBOX" project, so that
                # prevents SANDBOX PSUs from being decloaked
                orgs = get_orgs_with_access(new_obj)
                # if the object does not have a 'public' attr*, it is public
                # NOTE:  * this includes Acu and ProjectSystemUsage objects
                if getattr(new_obj, 'public', False):
                    orb.log.info('   new object is public -- ')
                    orb.log.info('   publish "decloaked" on public channel...')
                    channel = u'vger.channel.public'
                    self.publish(channel, {u'decloaked':
                                     [new_obj.oid, new_obj.id,
                                      '', '']})
                elif isinstance(new_obj, orb.classes['ManagedObject']):
                    # ManagedObject introduces the 'public' flag ...
                    # any new non-public ManagedObject is considered "cloaked"
                    orb.log.info('   new Managed Object oid: {}'.format(
                                                                new_obj.oid))
                    orb.log.info('   new object is non-public -- ')
                    orb.log.info('   not publishing.')
                elif isinstance(new_obj, (orb.classes['Acu'],
                                          orb.classes['ProjectSystemUsage'])):
                    orb.log.info('   new Acu/PSU oid: {}'.format(new_obj.oid))
                    orb.log.info('                id: {}'.format(new_obj.id))
                    for org in orgs:
                        # publish 'decloaked' message
                        if org.id:
                            channel = u'vger.channel.' + str(
                                                          org.id)
                        else:
                            channel = u'vger.channel.public'
                        orb.log.info('   decloak msg on: {}'.format(
                                                            channel))
                        self.publish(channel, {u'decloaked':
                                     [new_obj.oid, new_obj.id,
                                      org.oid, org.id]})
                new_obj_dts[new_obj.oid] = str(new_obj.mod_datetime)
            return dict(new_obj_dts=new_obj_dts, mod_obj_dts=mod_obj_dts)

        yield self.register(save, u'vger.save',
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
            orb.log.info('* vger.delete()')
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
            # NOTE: objects without a 'creator' attribute cannot be deleted ->
            # only instances of subclasses of 'Modelable' can be deleted.
            auth_dels = {oid: obj for oid, obj in objs_found.items()
                         if getattr(obj, 'creator', None) is user}
            # check for RoleAssignments
            admin_role = orb.get('pgefobjects:Role.Administrator')
            for obj in objs_found.values():
                if isinstance(obj, orb.classes['RoleAssignment']):
                    # RoleAssignments can only be deleted by an Administrator
                    # for the Organization in which the Role was assigned
                    org = obj.role_assignment_context
                    admin = orb.select('RoleAssignment', assigned_to=user,
                                       assigned_role=admin_role,
                                       role_assignment_context=org)
                    if admin:
                        auth_dels[obj.oid] = obj
            oids_deleted = list(auth_dels.keys())
            orb.delete(auth_dels.values())
            for oid in oids_deleted:
                orb.log.info('   publishing "deleted" msg to public channel.')
                channel = u'vger.channel.public'
                self.publish(channel, {u'deleted': oid})
            return (oids_not_found, oids_deleted)

        yield self.register(delete, u'vger.delete',
                            RegisterOptions(details_arg='cb_details'))

        def decloak(obj_oid, actor_oid, cb_details=None):
            """
            Decloak a ManagedObject in the repository to the specified actor
            (usually an Organization or Project) and publish a message to the
            relevant actor channel.  (In some contexts, 'decloak' is referred
            to as 'release' or 'publish'.  'decloak' is chosen here since it is
            free of semantic baggage, relative to those other possible terms.)

            In terms of the PGEF standard message structures, 'decloaked' is
            the 'subject' of the published message and an `(object_oid,
            actor_oid)` tuple is the 'content'.

            Args:
                obj_oid:   oid of the Product to be decloaked
                actor_oid: oid of the Actor instance to receive access to it;
                    if the actor is None or an empty string, the object will be
                    decloaked globally.

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                The cloaking status of the Product -- i.e. the same data
                as is returned by 'get_cloaking_status':
                (1) a list (oids of actors the object has been decloaked to)
                (2) a message (empty if successful)
                (3) the submitted object oid
            """
            # TODO:
            #   - authorization:  is user permitted to decloak the objs
            #                     i.e. is user the creator (or admin?)
            orb.log.info('[rpc] vger.decloak() ...')
            orb.log.info('      object oid: {}'.format(str(obj_oid)))
            orb.log.info('      actor oid:  {}'.format(str(actor_oid)))
            userid = getattr(cb_details, 'caller_authid', None)
            orb.log.info('      caller authid: {}'.format(str(userid)))
            msg = ''
            actors = []
            if obj_oid:
                if actor_oid:
                    actor = orb.get(actor_oid)
                    if not actor:
                        msg = 'actor not found'
                        return actors, msg, obj_oid
                obj = orb.get(obj_oid)
                if obj:
                    if not isinstance(obj, orb.classes['ManagedObject']):
                        msg = 'object is not a Managed Object; not cloakable'
                        return actors, msg, obj_oid
                    if obj.public:
                        channel = u'vger.channel.public'
                        self.publish(channel, {u'decloaked':
                                     [obj.oid, obj.id, '', '']})
                        msg = 'object is public; not cloakable'
                        return actors, msg, obj_oid
                    oas = orb.search_exact(cname='ObjectAccess',
                                           accessible_object=obj)
                    if oas:
                        actors = [getattr(oa.grantee, 'oid', '') for oa in oas]
                    user = orb.select('Person', id=userid)
                    # TODO:  use 'get_perms' ... also, allow org Admin to
                    # decloak any object that is decloaked to their org, to
                    # (any?) parent org
                    if not user.oid == obj.creator.oid:
                        msg = 'user is not authorized to decloak this object'
                        return actors, msg, obj_oid
                    existing_oa = orb.select('ObjectAccess',
                                             accessible_object=obj,
                                             grantee=actor)
                    if existing_oa:
                        msg = 'object was already_decloaked to this actor'
                        return actors, msg, obj_oid
                    else:
                        ObjectAccess = orb.classes['ObjectAccess']
                        dts = dtstamp()
                        new_oid = str(uuid4())
                        new_id = actor.id + '.access_to.' + obj.id
                        oa = ObjectAccess(oid=new_oid,
                                          id=new_id,
                                          accessible_object=obj,
                                          grantee=actor,
                                          create_datetime=dts,
                                          mod_datetime=dts)
                        orb.save([oa])
                        channel = u'vger.channel.' + str(
                                        getattr(actor, 'id', 'public'))
                        self.publish(channel, {u'decloaked':
                                     [obj.oid, obj.id, actor.oid, actor.id]})
                        actors.append(actor_oid)
                        return actors, msg, obj_oid
                else:
                    msg = 'object not found'
                    return actors, msg, obj_oid
            else:
                msg = 'request did not reference an object'
            return actors, msg, obj_oid

        yield self.register(decloak, u'vger.decloak',
                            RegisterOptions(details_arg='cb_details'))

        def get_cloaking_status(obj_oid, cb_details=None):
            """
            Get information on the cloaking status of an object.

            Args:
                obj_oid:   oid of the object to be decloaked

            Keyword Args:
                cb_details:  added by crossbar; not included in rpc signature

            Returns:
                3-tuple consisting of
                (1) a list (oids of actors the object has been decloaked to)
                (2) a message (empty if successful)
                (3) the submitted object oid
            """
            orb.log.info('[rpc] vger.get_cloaking_status() ...')
            orb.log.info('  - object oid: {}'.format(str(obj_oid)))
            actors = []
            msg = ''
            if obj_oid:
                obj = orb.get(obj_oid)
                if obj:
                    if not isinstance(obj, orb.classes['ManagedObject']):
                        msg = 'Object is not a Managed Object, not cloakable'
                        actors = ['public']
                        return actors, msg, obj_oid
                    elif obj.public:
                        msg = 'Object is public'
                        actors = ['public']
                        return actors, msg, obj_oid
                    oas = orb.search_exact(cname='ObjectAccess',
                                           accessible_object=obj)
                    if oas:
                        actors = [getattr(oa.grantee, 'oid', '') for oa in oas]
                    else:
                        msg = 'cloaked'
                else:
                    msg = 'not found'
            else:
                msg = 'no oid'
            return actors, msg, obj_oid

        yield self.register(get_cloaking_status, u'vger.get_cloaking_status',
                            RegisterOptions(details_arg='cb_details'))

        def sync_parameter_definitions(data, cb_details=None):
            """
            Sync all ParameterDefinitions in the repository with the
            requestor's.

            Args:
                data (dict):  dict {oid: str(mod_datetime)}
                    for the requestor's set of ParameterDefinitions

            Return:
                result (list of lists):  list containing:
                    [0]:  server objects that either have later mod_datetime(s)
                          or are not represented in the data that was sent
                    [1]:  oids of server objects with same mod_datetime(s)
                    [2]:  oids of server objects with earlier mod_datetime(s)
                    [3]:  any oids in data that were not found on the server
            """
            orb.log.info('[rpc] vger.sync_parameter_definitions() ...')
            orb.log.info('   data: {}'.format(str(data)))
            pd_dts = orb.get_mod_dts('ParameterDefinition')
            server_pd_dts = {oid : dts for oid, dts in pd_dts.items()
                             if oid not in ref_pd_oids}
            if data:
                unknown_oids = []
                for oid in data:
                    if not orb.get(oid):
                        unknown_oids.append(oid)
                for oid in unknown_oids:
                    del data[oid]
                same_dts = []
                # all server pd dts that are not the same as those in data
                not_same_dts = {}
                for oid, dts_str in server_pd_dts.items():
                    # NOTE:  may need to convert strings to datetimes
                    if str(dts_str) == str(data.get(oid)):
                        same_dts.append(oid)
                    else:
                        not_same_dts[oid] = dts_str
                newer_pds = []
                older_dts = []
                for pd in orb.get(oids=list(not_same_dts.keys())):
                    if data.get(pd.oid):
                        data_dt = uncook_datetime(data[pd.oid])
                        if pd.mod_datetime > data_dt:
                            newer_pds.append(pd)
                        else:
                            older_dts.append(pd.oid)
                    else:
                        newer_pds.append(pd)
                return [serialize(orb, newer_pds), same_dts, older_dts,
                        unknown_oids]
            else:
                return [serialize(orb, orb.get(oids=list(server_pd_dts.keys()))),
                        [], [], []]

        yield self.register(sync_parameter_definitions,
                            u'vger.sync_parameter_definitions',
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

            Return:
                result (list of lists):  list containing:
                    [0]:  server objects that have later mod_datetime(s)
                    [1]:  oids of server objects with same mod_datetime(s)
                    [2]:  oids of server objects with earlier mod_datetime(s)
                    [3]:  any oids in data that were not found on the server
            """
            orb.log.info('[rpc] vger.sync_objects()')
            if not data:
                orb.log.info('  no data sent; returning empty.')
                return [[], [], [], []]
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
                orb.log.info('   result: {}'.format(str(result)))
                return result
            else:
                result = [[], same_oids, older_oids, unknown_oids]
                orb.log.info('   result: {}'.format(str(result)))
                return result

        yield self.register(sync_objects, u'vger.sync_objects',
                            RegisterOptions(details_arg='cb_details'))

        def sync_library_objects(data, cb_details=None):
            """
            Sync all objects to which the user has access.  (NOTE:
            `sync_objects()` should be called first with the user's local
            objects, so that any objects the user created since their last
            login will be added to the server.)

            Args:
                data (dict):  dict {oid: str(mod_datetime)}
                    containing the library objects that the user has (all
                    objects the user has that were not created by the user)

            Return:
                result (list of lists):  list containing:
                    [0]:  server objects that have later mod_datetime(s) or
                          were not found in data
                          (the user should add these to their local db)
                    [1]:  oids of server objects with same mod_datetime(s)
                          (the user can safely ignore these)
                    [2]:  oids of server objects with earlier mod_datetime(s)
                          (there should not be any!)
                    [3]:  any oids in data that were not found on the server --
                          the user should delete these from their local db if
                          they are either
                          [a] not created by the user or
                          [b] created by the user but are in 'trash'.
            """
            orb.log.info('[rpc] vger.sync_library_objects()')
            orb.log.info('  data: {}'.format(str(data)))

            # TODO: user object will be needed when more than "public" objects
            # are to be returned -- e.g., organizational product libraries to
            # which the user has access by having a role in the organization
            # user = None
            # userid = getattr(cb_details, 'caller_authid', '')
            # if userid:
                # user = orb.select('Person', id=userid)

            # oids of objects unknown to the server (these would be objects
            # in data that were deleted on the server) -- the user should
            # delete these from their local db (NOTE that this is the reverse
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
            # TODO:  objects decloaked to any org on which the user has a role
            #        (this will be, e.g., organizational product libraries)
            server_dts = {}
            same_oids = []
            older_oids = []
            public_oids = [o.oid for o in orb.search_exact(public=True)]
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
                # oids of server objects with same mod_datetime as submitted oids
                same_oids = [oid for oid, dt in dts_by_oid.items()
                             if dt == server_dts.get(oid)]
                # oids of older objects on the server
                older_oids = [oid for oid, dt in dts_by_oid.items()
                              if (server_dts.get(oid)
                                  and (dt > server_dts.get(oid)))]
            if newer_oids:
                newer_sobjs = serialize(orb, orb.get(oids=newer_oids),
                                        include_components=True)
                result = [newer_sobjs, same_oids, older_oids, unknown_oids]
                orb.log.info('   result: {}'.format(str(result)))
                return result
            else:
                result = [[], same_oids, older_oids, unknown_oids]
                orb.log.info('   result: {}'.format(str(result)))
                return result

        yield self.register(sync_library_objects, u'vger.sync_library_objects',
                            RegisterOptions(details_arg='cb_details'))

        def sync_project(project_oid, data, cb_details=None):
            """
            Sync all objects for the specified project in the repository.

            Args:
                project_oid (str):  oid of the project to be synced
                data (dict):  dict {oid: str(mod_datetime)}
                    for known objects of the project to be synced

            Return:
                result (list of lists):  list containing:
                    [0]:  server objects that either have later mod_datetime(s)
                          or are not represented in the data that was sent
                    [1]:  oids of server objects with same mod_datetime(s)
                    [2]:  oids of server objects with earlier mod_datetime(s)
                    [3]:  any oids in data that were not found on the server
            """
            orb.log.info('[rpc] vger.sync_project() ...')
            orb.log.info('   project oid: {}'.format(str(project_oid)))
            orb.log.info('   data: {}'.format(str(data)))
            if not project_oid or project_oid == 'pgefobjects:SANDBOX':
                return [[], [], [], []]
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
                    return result
                else:
                    result = [[], same_oids, older_oids, unknown_oids]
                    orb.log.info('   result: {}'.format(str(result)))
                    return result
            else:
                orb.log.info('   ** project not found on server **')
                return [[], [], [], []]

        yield self.register(sync_project, u'vger.sync_project',
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
            orb.log.info('[rpc] vger.search_exact() ...')
            return serialize(orb, orb.search_exact(**kw))

        yield self.register(search_exact, u'vger.search_exact')

        def get_version():
            """
            Return the curent version of pangalactic and whether a schema
            change is involved. 

            Returns:
                tuple:  version (str), schema_change (bool)
            """
            orb.log.info('[rpc] vger.get_version() ...')
            schema_change = bool(__version__ in schema_maps)
            return __version__, schema_change

        yield self.register(get_version, u'vger.get_version')

        def get_object(oid, include_components=False, cb_details=None):
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
            orb.log.info('[rpc] vger.get_object({}) ...'.format(oid))
            # TODO: determine authorization for user
            # userid = getattr(cb_details, 'caller_authid', '')
            # if userid:
                # user = orb.select('Person', id=userid)
            obj = orb.get(oid)
            if obj is not None:
                # TODO:  if include_components is True, get_perms() should be
                # used to determine the user's access to the components ...
                return serialize(orb, [obj],
                                 include_components=include_components)
            else:
                return []

        yield self.register(get_object, u'vger.get_object',
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
            orb.log.info('[rpc] vger.get_mod_dts() ...')
            return orb.get_mod_dts(cname=cname, oids=oids)

        yield self.register(get_object, u'vger.get_mod_dts')

        # OLD CODE for get_role_assignments ... (was for OMB-compatibility)
        def get_role_assignments(userid, **kw):
            """
            Retrieves the RoleAssignment objects for the specified userid.

            Args:
                userid (str):  userid of a person (Person.id)

            Returns:
                role_assignments:  Instances of RoleAssignment
            """
            orb.log.info('[rpc] vger.get_role_assignments({}) ...'.format(userid))
            user = orb.select('Person', id=userid)
            if user:
                ras = orb.search_exact(cname='RoleAssignment',
                                       assigned_to=user)
                orgs = set([ra.role_assignment_context for ra in ras
                            if ra.role_assignment_context])
                # yes, this is wildly inefficient ...
                org_dicts = [dict(oid=o.oid, id=o.id, name=o.name,
                             description=o.description,
                             parent_organization=getattr(
                                        o.parent_organization, 'oid', None))
                             for o in orgs]
                user_dicts = [dict(oid=p.oid, id=p.id, name=p.name)
                              for p in set([ra.assigned_to for ra in ras])]
                role_dicts = [dict(oid=r.oid, id=r.id, name=r.name)
                              for r in set([ra.assigned_role for ra in ras])]
                ra_dicts = [dict(oid=ra.oid, assigned_role=ra.assigned_role.oid,
                    assigned_to=ra.assigned_to.oid,
                    role_assignment_context=getattr(
                                        ra.role_assignment_context, 'oid', None))
                    for ra in ras]
                return {u'organizations': org_dicts,
                        u'users': user_dicts,
                        u'roles': role_dicts,
                        u'roleAssignments': ra_dicts 
                        }
            else:
                return {}

        yield self.register(get_role_assignments, u'vger.get_role_assignments')

        def get_user_roles(userid):
            """
            Get the RoleAssignment objects that have the user with the
            specified userid as their 'assigned_to' (Person) attribute,
            and return the serialized user (Person) and RoleAssignment objects.

            Args:
                userid (str):  userid of a person (Person.id)

            Returns:
                tuple of lists:  [0] serialized user (Person) object,
                                 [1] serialized RoleAssignment objects
            """
            orb.log.info('[rpc] vger.get_user_roles({}) ...'.format(userid))
            user = orb.select('Person', id=userid)
            if user:
                ras = orb.search_exact(cname='RoleAssignment',
                                       assigned_to=user)
                szd_ras = serialize(orb, ras)
                szd_user = serialize(orb, [user])
                return [szd_user, szd_ras]
            else:
                return [[], []]

        yield self.register(get_user_roles, u'vger.get_user_roles')

        def get_roles_in_org(org_oid):
            """
            Get all RoleAssignment objects that have the Organization with the
            specified oid as their 'role_assignment_context' attribute.

            Args:
                org_oid (str):  oid of the Organization

            Returns:
                list:  list of serialized RoleAssignment objects
            """
            orb.log.info('[rpc] vger.get_roles_in_org() ...')
            org = orb.get(org_oid)
            if org:
                return serialize(orb,
                                 orb.search_exact(cname='RoleAssignment',
                                                  role_assignment_context=org))
            else:
                return []

        yield self.register(get_roles_in_org, u'vger.get_roles_in_org')

        def get_user_object(userid):
            """
            Retrieves the Person object for the specified userid.

            Args:
                userid (str):  userid of a person (Person.id)

            Returns:
                list:  list containing a serialized Person object
            """
            orb.log.info('[rpc] vger.get_user_object()')
            return serialize(orb, [orb.select('Person', id=userid)])[0]

        yield self.register(get_user_object, u'vger.get_user_object')

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
            orb.log.info('[rpc] vger.search_ldap')
            return search(**kw)

        yield self.register(search_ldap, u'vger.search_ldap')

        ###### json procedures: call the rpc and json.dump the output
        ###### (for use with crossbar's "REST Bridge")

        def json_search_exact(**kw):
            """
            Call search_exact as a http rest call
            """
            objs = search_exact(**kw)
            return json.dumps(objs).decode('utf-8')
        yield self.register(json_search_exact, u'vger.json_search_exact')

        def json_get_object(oid):
            """
            Call get_object as a http rest call
            """
            # returns a list containing one serialized object or None
            res = get_object(oid)
            return json.dumps(res).decode('utf-8')
        yield self.register(json_get_object, u'vger.json_get_object')

        def json_delete(oid):
            """
            Call delete as a http rest call
            """
            try:
                obj = orb.get(oid)
                orb.delete([obj])
                return json.dumps(True)
            except:
                return json.dumps(False)
        yield self.register(json_delete, u'vger.json_delete')

        # end of backend setup
        orb.log.info("procedures registered")

if __name__ == '__main__':

    home_help = 'home directory (used by orb) [default: current directory]'
    config_help = 'initial config file name [default: "config"]'
    cert_help = 'crossbar host cert file name [default: "server_cert.pem"].'
    parser = argparse.ArgumentParser()
    parser.add_argument('--authid', dest='authid', type=six.text_type,
                        help='id to connect as (required)')
    parser.add_argument('--home', dest='home', type=six.text_type,
                        help=home_help)
    parser.add_argument('--config', dest='config', type=six.text_type,
                        help=config_help)
    parser.add_argument('--db_url', dest='db_url', type=six.text_type,
                        help='db connection url (used by orb)')
    parser.add_argument('--cb_host', dest='cb_host', type=six.text_type,
                        help='crossbar host [default: localhost].')
    parser.add_argument('--cb_port', dest='cb_port', type=six.text_type,
                        help='crossbar port [default: 8080].')
    parser.add_argument('--cert', dest='cert', type=six.text_type,
                        default='server_cert.pem', help=cert_help)
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Set logging level to DEBUG')
    parser.add_argument('-t', '--test', dest='test', action='store_true',
                        help='Loads test data at startup')
    options = parser.parse_args()

    from autobahn.twisted.wamp import ApplicationRunner

    # command options override config settings; if neither, defaults are used
    home = options.home or ''
    if os.path.exists(options.config):
        read_config(options.config)
    else:
        read_config(os.path.join(home, 'config'))
    authid = options.authid or config.get('authid', 'service2')
    if type(authid) is not str:
        authid = str(authid, 'utf-8')
    # unix domain socket connection to db:  socket located in home dir
    domain_socket = home + '/vgerdb_socket'
    db_url = options.db_url or config.get('db_url',
             'postgresql://scred@:5432/vgerdb?host={}'.format(domain_socket))
    test = options.test or config.get('test', False)
    debug = options.debug or config.get('debug', False)
    extra = {
        'authid': authid,
        'home': home,
        'db_url': db_url,
        'debug': debug,
        'test': test
        }
    cb_host = options.cb_host or config.get('cb_host', 'localhost')
    cb_port = options.cb_port or config.get('cb_port', '8080')
    cb_url = 'wss://{}:{}/ws'.format(cb_host, cb_port)
    # router can auto-choose the realm, so not necessary to specify
    realm = None
    config['authid'] = authid
    config['db_url'] = db_url
    config['debug'] = debug
    config['test'] = test
    config['cb_host'] = cb_host
    config['cb_port'] = cb_port
    # write the new config file
    write_config(os.path.join(home, 'config'))
    print("vger starting with")
    print("   home directory:  '{}'".format(home))
    print("   connecting to crossbar at:  '{}'".format(cb_url))
    print("       realm:  '{}'".format(realm or "not specified (auto-choose)"))
    print("       authid: '{}'".format(authid))
    print("   db url: '{}'".format(options.db_url))
    print("   test: '{}'".format(str(test)))
    print("   debug: '{}'".format(str(debug)))
    if authid not in TICKETS:
        print("Given authid <{}> is not in my tickets database!".format(
                                                                    authid))
        sys.exit(1)
    # load crossbar host certificate (default: file 'server_cert.pem' in
    # home directory)
    cert_fpath = os.path.join(home, options.cert)
    cert_content = crypto.load_certificate(crypto.FILETYPE_PEM,
                                           six.u(open(cert_fpath, 'r').read()))
    tls_options = CertificateOptions(
                    trustRoot=OpenSSLCertificateAuthorities([cert_content]))
    runner = ApplicationRunner(url=cb_url, realm=realm, ssl=tls_options,
                               extra=extra)
    runner.run(RepositoryService, auto_reconnect=True)

