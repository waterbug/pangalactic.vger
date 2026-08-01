# -*- coding: utf-8 -*-
"""
Shared fixtures for the vger tests that need a *real* orb.

The tests in test_vger.py and test_userdir.py replace "orb" with a mock, which
is right for them:  they exercise rpc registration, argument handling and the
LDAP paths, none of which need a database.

The regression tests for the findings in vger_review.md are different -- they
are about authorization and persistence, so they need real objects, real
RoleAssignments and a real get_perms().  This module starts one orb, once, on
a scratch home populated with the standard test fixtures, and shares it across
those test modules.

NOTE:  orb.start() is effectively a singleton -- starting it a second time in
the same process is not supported, so start_test_orb() is idempotent and every
module that needs the orb calls it rather than starting its own.
"""
import atexit
import os
import shutil
import tempfile

# set the orb
import pangalactic.core.set_uberorb

from pangalactic.core import orb
from pangalactic.core.access import get_perms
from pangalactic.core.serializers import deserialize
from pangalactic.core.test.utils import create_test_users, create_test_project


_started = False
_home_root = None


def start_test_orb():
    """
    Start the orb on a scratch home with the standard test data loaded.

    Idempotent:  the first call does the work, later calls are no-ops, so any
    number of test modules can depend on it.

    NOTE:  the home directory is created first, deliberately.  orb.start() with
    a home that does not exist writes the reference db *as a file* at that path
    and then raises NotADirectoryError (see the deferred orb.start() finding in
    pangalactic.core/pangalactic_core_review_scoped.md).

    Returns:
        the orb
    """
    global _started, _home_root
    if _started:
        return orb
    _home_root = tempfile.mkdtemp(prefix='vger_test_')
    home = os.path.join(_home_root, 'home')
    os.makedirs(home)
    atexit.register(_cleanup)
    orb.start(home=home, debug=False, console=False)
    deserialize(orb, create_test_users())
    deserialize(orb, create_test_project())
    _started = True
    return orb


def _cleanup():
    if _home_root and os.path.isdir(_home_root):
        shutil.rmtree(_home_root, ignore_errors=True)


def get_test_user(userid='zaphod'):
    """
    Get one of the standard test users.

    Keyword Args:
        userid (str):  'id' of the Person wanted

    Returns:
        Person
    """
    return orb.select('Person', id=userid)


def all_of_class(cname):
    """
    Get every instance of a class, including subclasses.

    NOTE:  orb.search_exact(cname=...) returns [] unless at least one real
    attribute is also given, so it cannot be used for "all instances of".

    Args:
        cname (str):  class name

    Returns:
        list of objects
    """
    return orb.db.query(orb.classes[cname]).all()


def find_unmodifiable_by(user, cname='HardwareProduct'):
    """
    Find an existing object the user does NOT have 'modify' permission on.

    This is the object an authorization test needs:  one that is genuinely in
    the repository and genuinely off limits to the caller.

    Args:
        user (Person):  the user

    Keyword Args:
        cname (str):  class of object wanted

    Returns:
        the object, or None if the test data has no such object
    """
    for obj in all_of_class(cname):
        if 'modify' not in get_perms(obj, user=user):
            return obj
    return None


def find_modifiable_by(user, cname='HardwareProduct'):
    """
    Find an existing object the user DOES have 'modify' permission on.

    Args:
        user (Person):  the user

    Keyword Args:
        cname (str):  class of object wanted

    Returns:
        the object, or None if the test data has no such object
    """
    for obj in all_of_class(cname):
        if 'modify' in get_perms(obj, user=user):
            return obj
    return None


class FakeDetails:
    """
    Stand-in for the "cb_details" object crossbar passes to an rpc.

    Every vger rpc reads the caller's identity as
    getattr(cb_details, 'caller_authid', <default>), so this is all that is
    needed to exercise the authorization logic.

    NOTE:  this asserts nothing about whether the identity is *trustworthy* --
    that is a property of WAMP-cryptosign and authenticator.py, verified
    separately (see "Verified assumption" in vger_review.md).
    """
    def __init__(self, caller_authid=None):
        if caller_authid is not None:
            self.caller_authid = caller_authid
