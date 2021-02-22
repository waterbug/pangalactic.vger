"""
Generic search interface to an LDAP user directory.
"""
import ldap
from pangalactic.core         import config
from pangalactic.core.uberorb import orb

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
    Return a dict containing relevant info from an LDAP search result mapped
    into Person class attributes except for "employer_name" and "org_code",
    which need to be mapped to Org objects for "employer" and "org" attributes.

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
    # code may or may not have a dot -- return "dotless" format for both cases:
    org_code_list = rawdict.get('nasaorgCode', [b''])
    if org_code_list:
        org_code_str = org_code_list[0].decode()
        nodotcode = ''.join(org_code_str.split('.'))
        if len(nodotcode) == 4:
            org_code = '.'.join([nodotcode[0:3], nodotcode[3]])
        else:
            org_code = rawdict.get('nasaorgCode', [b''])[0].decode()
    else:
        org_code = ''
    # decode each field value so dir_info values are strings ...
    dir_info = dict(id=rawdict['agencyUID'][0].decode(),
                    oid=uupic_list[0].decode(),
                    first_name=rawdict['givenName'][0].decode(),
                    mi_or_name=initials_list[0].decode(),
                    last_name=rawdict['sn'][0].decode(),
                    employer_name=rawdict['nasaEmployer'][0].decode(),
                    email=mail_list[0].decode(),
                    org_code=org_code)
    return dir_info

def search_ldap_directory(ldap_url, base_dn, test=None, **kw):
    """
    NOTE: if search is under-specified, result may exceed maximum allowed
    size.  Find personnel in the LDAP directory using the specified properties.
    Default return format is 'dir_info' (a dictionary).

    NOTE:
    'center' = physical center, or "campus" (WFF, GSFC [aka "GRB"])
    'nasa_paid_center' = "logical" center (GSFC contains WFF and GSFC)

    Args:
        ldap_url (str): url of the LDAP service
        base_dn (str): LDAP base domain to search

    Keyword Args:
        test (str):  default is None; values can be "search" to return the
            search string for inspection or "result" to return an example
            result to test the client's handling of it

    Return:
        tuple: (LDAP search string (str), result records (list of dict))
    """
    orb.log.info('* userdir: search_ldap_directory()')
    orb.log.info('  ldap_url = {}'.format(ldap_url))
    orb.log.info('  base_dn  = {}'.format(base_dn))
    orb.log.info('  kw = {}'.format(str(kw)))
    # the search string, f, is ok as a python 3 string (unicode)
    schema = config.get('ldap_schema', {})
    orb.log.info('  ldap_schema = {}'.format(str(schema)))
    ldap_req_fields = config.get('ldap_req_fields', '')
    orb.log.info('  ldap_req_fields = {}'.format(ldap_req_fields))
    if schema:
        f = ''
        valid_fields = {schema[a]:a for a in schema}
        if kw and valid_fields:
            valid_values = [(valid_fields.get(a), kw[a])
                            for a in list(kw.keys())
                            if a in valid_fields]
            for ldap_field, value in valid_values:
                f += '({}={})'.format(ldap_field, value)
        f += ldap_req_fields
    else:
        # if we don't have a schema, set as test
        test = 'search'
    # create a valid LDAP search string ...
    f = '(&'+f+')'
    if test == 'search':
        # return only the search string and an empty result
        return (f, [])
    if test == 'result':
        # return an example result (Red Lectroids :)
        return (f,
                [dict(oid='test:carefulwalker', id='carefulwalker',
                     last_name='Carefulwalker', first_name='John',
                     mi_or_name='D', name='John Carefulwalker',
                     org_code='890.0', employer_name='Yoyodyne',
                     email='John.Carefulwalker@yoyodyne.com'),
                 dict(oid='test:thornystick', id='thornystick',
                      last_name='Thornystick', first_name='John',
                      mi_or_name='T', name='John Thornystick',
                      org_code='890.0', employer_name='Yoyodyne',
                      email='John.Thornystick@yoyodyne.com'),
                 dict(oid='test:smallberries', id='smallberries',
                      last_name='Smallberries', first_name='John',
                      mi_or_name='R', name='John Smallberries',
                      org_code='890.0', employer_name='Yoyodyne',
                      email='John.Smallberries@yoyodyne.com'),
                 dict(
                     oid='test:buckaroo', id='buckaroo',
                     first_name='Buckaroo', mi_or_name='', last_name='Banzai',
                     name='Buckaroo Banzai', org_code='890.0',
                     email='buckaroo@banzai.earth.milkyway.univ',
                     employer_name='Banzai'),
                 dict(
                     oid='test:whorfin', id='whorfin',
                     first_name='John', mi_or_name='', last_name='Whorfin',
                     name='John Whorfin (Dr. Emilio Lizardo)', org_code='890.0',
                     email='whorfin@redlectroids.planet10.univ',
                     employer_name='Yoyodyne'),
                 dict(
                     oid='test:bigboote', id='bigboote',
                     first_name='John', mi_or_name='', last_name='Bigboote',
                     name='John Bigboote', org_code='890.0',
                     email='bigboote@redlectroids.planet10.univ',
                     employer_name='Yoyodyne'),
                 dict(
                     oid='test:manyjars', id='manyjars',
                     first_name='John', mi_or_name='', last_name='Manyjars',
                     name='John Manyjars', org_code='890.0',
                     email='manyjars@redlectroids.planet10.univ',
                     employer_name='Yoyodyne')
                     ])
    # NOTE: the *field values* in res will be bytes
    res = search_by_filterstring(ldap_url, base_dn, f)
    people = []
    if res:
        for r in res:
            people.append(_get_dir_info(r[0]))
    return (f, people)

