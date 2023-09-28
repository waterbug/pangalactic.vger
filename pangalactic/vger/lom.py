#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Interface to the Linear Optical Model
"""
from scipy.io import loadmat

from pangalactic.core                 import orb
from pangalactic.core.clone           import clone
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

        ['surfacelabel', 'surfaceunits', 'rdy', 'rdx', 'k', 'prxy', 'thi',
         'map', 'mav', 'mnr', 'ape_mask', 'ape_maskxy', 'ape_rad',
         'ape_xy_edge', 'ape_xy_center', 'surfacetype', 'coefs_note',
         'coefs_vals', 'glass', 'rindex', 'refractmode', 'decenter_type',
         'decenter_vals', 'refrays', 'global_surfacelabel', 'local2global_4x4',
         'global2local_4x4', 'dRR_dRBM', 'dFIR_dRBM', 'dWFE_dRBM', 'dRR_dRK',
         'dFIR_dRK', 'dWFE_dRK', 'dSAG_dRK', 'dRR_dZFE', 'dFIR_dZFE',
         'dWFE_dZFE', 'SAG_MASK', 'SAG_XY', 'SAG', 'dSAG_dZFE', 'dRMS_dRBM',
         'dRMS_dRK', 'dRMS_dZFE']
    """
    return list(data["lomdata"]["LOM"])

def get_surfaces_drms_drk(data):
    """
    Return a dict mapping surface names to dicts of dRMS_dRK worst-case
    values for each degree of freedom (dx, dy, dz, rx, ry, rz).

    Args:
        data (dict): LOM data set read from .mat file

    Return:
        a dict of dicts:

            {'surface_1' : {'dRMS_dRoC' : dRMS_dRoC,
                            'dRMS_dK'   : dRMS_dK},
             'surface_2' : {'dRMS_dRoC' : dRMS_dRoC,
                            ...},
             ...}
    """
    surfaces = get_optical_surface_names(data)
    lom = get_LOM(data)
    comps = ['dRMS_dRoC',
             'dRMS_dK']
    d = {}
    for i, surface in enumerate(surfaces):
        drdr = lom[i]['dRMS_dRK']
        d[surface] = {comps[j] : float(max(v)) for j, v in enumerate(drdr)}
    return d

def get_surfaces_drms_drbm(data):
    """
    Return a dict mapping surface names to dicts of dRMS_dRBM worst-case
    values for each degree of freedom (dx, dy, dz, rx, ry, rz).

    Args:
        data (dict): LOM data set read from .mat file

    Return:
        a dict of dicts:

            {'surface_1' : {'dRMS_dRBM_dx' : dRMS_dRBM/dx,
                            'dRMS_dRBM_dy' : dRMS_dRBM/dy,
                            'dRMS_dRBM_dz' : dRMS_dRBM/dz,
                            'dRMS_dRBM_rx' : dRMS_dRBM/rx,
                            'dRMS_dRBM_ry' : dRMS_dRBM/ry,
                            'dRMS_dRBM_rz' : dRMS_dRBM/rz},
             'surface_2' : {'dRMS_dRBM_dx' : dRMS_dRBM/dx,
                            ...},
             ...}
    """
    surfaces = get_optical_surface_names(data)
    lom = get_LOM(data)
    comps = ['dRMS_dRBM_dx',
             'dRMS_dRBM_dy',
             'dRMS_dRBM_dz',
             'dRMS_dRBM_rx',
             'dRMS_dRBM_ry',
             'dRMS_dRBM_rz']
    d = {}
    for i, surface in enumerate(surfaces):
        drdr = lom[i]['dRMS_dRBM']
        # NOTE: cast is needed because ndarray values are uint8, which
        # are not json-serializable ...
        d[surface] = {comps[j] : float(max(v)) for j, v in enumerate(drdr)}
    return d

def extract_lom_structure(LOM, fpath):
    """
    Extract the optical system structure from a Linear Optical Model matlab
    (.mat) file.

    Args:
        LOM (Model): Model instance representing the LOM
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
        system = LOM.of_thing
        if system.components:
            orb.log.debug('  system already has components.')
            return []
        surface_names = get_optical_surface_names(data)
        optical_component_type = orb.get(
                              'pgefobjects:ProductType.optical_component')
        if surface_names:
            NOW = dtstamp()
            user = orb.get('pgefobjects:admin')
            for i, name in enumerate(surface_names):
                # create a HW product and Acu for each surface
                opt_comp = clone('HardwareProduct', id=name,
                                 name=name, owner=LOM.owner,
                                 product_type=optical_component_type,
                                 create_datetime=NOW, mod_datetime=NOW,
                                 creator=user, modifier=user)
                new_objs.append(opt_comp)
                oc_ref_des = f'oc-{i:03}'
                oc_acu_id = f'{system.id}-{oc_ref_des}'
                oc_acu_name = f'{system.id}-{opt_comp.id}-{i:03}'
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
            orb.save(new_objs)
            return new_objs
        else:
            orb.log.debug('  no surface names were found.')
            return []
    else:
        orb.log.debug('  no data was found.')
        return []

def get_lom_parm_data(fpath):
    """
    Extract the optical system sensitivity parameters from a Linear Optical
    Model matlab (.mat) file.

    Args:
        fpath (str): path to the LOM file
    """
    orb.log.debug('* get_lom_parm_data()')
    data = None
    try:
        data = get_lom_data(fpath)
    except:
        # TODO: log the traceback
        orb.log.debug(f' - file "{fpath}" could not be opened.')
        return
    if data:
        lom_parm_data = {}
        lom_parm_data['drms_drbm'] = get_surfaces_drms_drbm(data)
        lom_parm_data['drms_drk'] = get_surfaces_drms_drk(data)
        return lom_parm_data
    else:
        orb.log.debug('  no data was found.')
        return {}

