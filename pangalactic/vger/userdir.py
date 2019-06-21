"""
Generic search interface to an LDAP user directory.
"""
import ldap
from pangalactic.core import config

def search_by_filterstring(ldap_url, base_dn, filterstring, sizelimit=0):
    """
    Do an LDAP search the specified filter string.

    NOTE: for python 3, python-ldap returns the result as a UTF-8 encoded
    string except for the field values which are typed as bytes.

    Args:
        filterstring (str): LDAP filter expression
        ldap_url (str): url of an LDAP service
        base_dn (str): LDAP expression for the base domain to be searched

    Keyword Args:
        sizelimit (int): maximum size allowed for result

    Returns:
        result_set (list): result of ldap search (see python-ldap docs for the
            result format), which will be parsed by _get_dir_info()
    """
    # NOTE: all of these fields are ok as python 3 'strings' (unicode)
    l = ldap.initialize(ldap_url)
    l.simple_bind_s('','')
    l.protocol_version = ldap.VERSION3
    search_scope = ldap.SCOPE_SUBTREE
    retrieve_attributes = None
    if sizelimit:
        ldap_result_id = l.search_ext_s(base_dn, search_scope,
                                        filterstr=filterstring,
                                        sizelimit=sizelimit)
    else:
        ldap_result_id = l.search(base_dn, search_scope, filterstring,
                                  retrieve_attributes)
    result_set = []
    while 1:
        result_type, result_data = l.result(ldap_result_id, 0)
        if result_data == []:
            break
        else:
            if result_type == ldap.RES_SEARCH_ENTRY:
                result_set.append(result_data)
    return result_set

def _get_dir_info(res):
    """
    Return a dict containing relevant info from an LDAP search result.

    [NOTE that the search result field values are expressed as bytes and must
    be decoded to get strings.]
    """
    # NOTE that all the field values here will be bytes
    rawdict = res[1]
    # this code assumes that a user's listing might not have some of the
    # properties specified in the configured LDAP schema
    uupic_list = rawdict.get('employeeNumber', [b''])
    initials_list = rawdict.get('initials', [b''])
    mail_list = rawdict.get('nasaPrimaryEmail', [b''])
    # code may or may not have a dot -- allow for both cases:
    org_code_str = rawdict['nasaorgCode'][0].decode()
    nodotcode = ''.join(org_code_str.split('.'))
    if len(nodotcode) == 4:
        org_code = '.'.join([nodotcode[0:3], nodotcode[3]])
    else:
        org_code = rawdict['nasaorgCode'][0].decode()
    # decode each field value so dir_info values are strings ...
    dir_info = dict(auid=rawdict['agencyUID'][0].decode(),
                    uupic=uupic_list[0].decode(),
                    first_name=rawdict['givenName'][0].decode(),
                    middle_initials=initials_list[0].decode(),
                    last_name=rawdict['sn'][0].decode(),
                    employer=rawdict['nasaEmployer'][0].decode(),
                    email=mail_list[0].decode(),
                    org=org_code)
    return dir_info

def search_ldap_directory(ldap_url, base_dn, test=False, **kw):
    """
    NOTE: if search is under-specified, result may exceed maximum allowed
    size.  Find personnel in the LDAP directory using the specified properties.
    Default return format is 'dir_info' (a dictionary).

    NOTE:
    'center' = physical center, or "campus" (WFF, GSFC [aka "GRB"])
    'nasa_paid_center' = "logical" center (GSFC contains WFF and GSFC)
    """
    # the search string, f, is ok as a python 3 string (unicode)
    schema = config.get('ldap_schema')
    if schema:
        f = '(nasaIdentityStatus=Active)'
        valid_fields = {schema[a]:a for a in schema}
        if kw and valid_fields:
            valid_values = [(valid_fields.get(a), kw[a])
                            for a in list(kw.keys())
                            if a in valid_fields]
            for ldap_field, value in valid_values:
                f += '({}={})'.format(ldap_field, value)
    else:
        # don't do the search if we didn't get kw args or don't have a schema
        return []
    f = '(&'+f+'(objectClass=person))'
    if test:
        return f
    # NOTE: the *field values* in res will be bytes
    res = search_by_filterstring(ldap_url, base_dn, f)
    people = []
    if res:
        for r in res:
            people.append(_get_dir_info(r[0]))
    return people

