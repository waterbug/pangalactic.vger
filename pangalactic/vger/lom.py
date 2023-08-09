#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Interface to the Linear Optical Model
"""
from scipy.io import loadmat

from pangalactic.core.clone       import clone
from pangalactic.core.names       import (get_acu_id, get_acu_name,
                                          get_next_ref_des)
from pangalactic.core.uberorb     import orb
from pangalactic.core.utils.datetimes import dtstamp


def get_lom_data(fpath):
    """
    Read a LOM data set from a .mat file.

    Args:
        fpath (str): path to the .mat file

    Return:
        a dict data structure
    """
    return loadmat(fpath, simplify_cells=True)

def get_optical_surface_names(data):
    """
    Get the names of all surfaces in a LOM data set.

    Args:
        data (dict): LOM data set read from .mat file

    Return:
        a list of strings
    """
    return [surface['surfacelabel'] for surface in data["lomdata"]["LOM"]]

def get_LOM(data):
    """
    Return the LOM data structure as a list of dicts.

    Args:
        data (dict): LOM data set read from .mat file

    Return:
        a list of dicts, one for each surface, with the
        following keys:

        ['surfacelabel', 'rdy', 'rdx', 'thi', 'map', 'mav', 'ape', 'k',
        'surfacetype', 'coefs_note', 'coefs_vals', 'glass', 'rindex',
        'refractmode', 'decenter_type', 'decenter_vals', 'return_surf',
        'refrays', 'globalcoord_ref', 'globalcoord_xyz', 'globalcoord_abc',
        'globalcoord_abc_note', 'globalcoord_ROT', 'dRR_dRBM', 'dFIR_dRBM',
        'dWFE_dRBM', 'MASK_dRBM', 'dRR_dRK', 'dFIR_dRK', 'dWFE_dRK',
        'MASK_dRK', 'dRR_dZFE', 'dFIR_dZFE', 'dWFE_dZFE', 'MASK_dZFE', 'Sag_m',
        'Sag_mask', 'Sag_xy', 'dSag_dZFE_m']
    """
    return list(data["lomdata"]["LOM"])

def extract_lom_structure(lom, fpath):
    """
    Extract the optical system structure from a Linear Optical Model matlab
    (.mat) file.

    Args:
        lom (Model): Model instance representing the LOM
        fpath (str): path to the LOM file
    """
    orb.log.debug('* extract_lom_structure()')
    data = None
    try:
        data = get_lom_data(fpath)
    except:
        # TODO: log the traceback
        orb.log.debug(f' - file "{fpath}" could not be opened.')
        return
    if data:
        new_objs = []
        system = lom.of_thing
        surface_names = get_optical_surface_names(data)
        comps_by_name = {acu.component.name : acu.component
                         for acu in system.components}
        acus_by_name = {acu.component.name : acu
                        for acu in system.components}
        optical_component_type = orb.get(
                              'pgefobjects:ProductType.optical_component')
        if surface_names:
            NOW = dtstamp()
            user = orb.get('pgefobjects:admin')
            for name in surface_names:
                if name not in comps_by_name:
                    # create a HW product and Acu for each surface that is
                    # not found among the system components ...
                    opt_comp = clone('HardwareProduct', id=name,
                                     name=name, owner=lom.owner,
                                     product_type=optical_component_type,
                                     create_datetime=NOW, mod_datetime=NOW,
                                     creator=user, modifier=user)
                    new_objs.append(opt_comp)
                    oc_ref_des = get_next_ref_des(system, opt_comp)
                    oc_acu_id = get_acu_id(system.id, oc_ref_des)
                    oc_acu_name = get_acu_name(system.name, oc_ref_des)
                    oc_usage = clone('Acu',
                                assembly=system, component=opt_comp,
                                id=oc_acu_id, name=oc_acu_name,
                                reference_designator=oc_ref_des,
                                create_datetime=NOW, mod_datetime=NOW,
                                creator=user, modifier=user)
                    new_objs.append(oc_usage)
            # NOTE: any existing HW objects that are found among the system
            # components but whose names are not in surface_names will be
            # flagged to be deleted -- the assumption is that the LOM is
            # authoritative as to system structure
            objs_to_delete = []
            for name in comps_by_name:
                if name not in surface_names:
                    hw = comps_by_name[name]
                    acu = acus_by_name[name]
                    objs_to_delete += [hw, acu]
            if new_objs:
                orb.save(new_objs)
            return (new_objs, objs_to_delete)
        else:
            orb.log.debug('  no surface names were found.')
            return ([], [])
    else:
        orb.log.debug('  no data was found.')
        return ([], [])

