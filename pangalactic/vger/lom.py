#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Interface to the Linear Optical Model
"""
import os

from louie import dispatcher

from scipy.io import loadmat

from pangalactic.core             import state
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
    return list(data["lomdata"]["SURFACES"])

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

def import_lom_assembly(fpath):
    """
    Import the model assembly structure from a Linear Optical Model (.mat)
    file.
    """
    orb.log.debug('* import_lom_assembly()')
    data = None
    sys_name = ''
    try:
        data = get_lom_data(fpath)
        fname = os.path.basename(fpath)
        sys_name = fname.split('.')[0]
    except:
        # TODO: log the traceback
        orb.log.debug(f' - file "{fpath}" could not be opened.')
        return
    if data:
        new_objs = []
        surface_names = get_optical_surface_names(data)
        comp_names = []
        optics = orb.search_exact(cname='Discipline', name='Optics')
        LOM = orb.get('pgefobjects:ModelType.Optics.LOM')
        optical_component = orb.get(
                              'pgefobjects:ProductType.optical_component')
        if surface_names:
            NOW = dtstamp()
            user = orb.get(state.get('local_user_oid'))
            if sys_name:
                # TODO: have a discussion about versioning etc. ... if
                # versioning is used, the search for existing items should
                # reference version(s), and versions should be added to any
                # new objects created from the data.
                opt_sys = orb.search_exact(cname='HardwareProduct',
                                           name=sys_name)
                opt_sys_model = orb.search_exact(
                                    cname='Model', name=sys_name,
                                    of_thing=opt_sys,
                                    model_definition_context=optics,
                                    type_of_model=LOM)
                if opt_sys_model:
                    comp_names = [acu.component.name
                                  for acu in opt_sys_model.components]
                else:
                    # create a HardwareProduct and a Model representing the
                    # system
                    opt_sys = clone('HardwareProduct', id=sys_name,
                                    name=sys_name, create_datetime=NOW,
                                    mod_datetime=NOW, creator=user,
                                    modifier=user)
                    opt_sys_model = clone('Model', id=sys_name,
                                        name=sys_name,
                                        of_thing=opt_sys,
                                        model_definition_context=optics,
                                        type_of_model=LOM,
                                        create_datetime=NOW,
                                        mod_datetime=NOW, creator=user,
                                        modifier=user)
                    new_objs += [opt_sys, opt_sys_model]
            for name in surface_names:
                if name not in comp_names:
                    # create a HW product and Acu for each surface that is
                    # not found among the system components ...
                    opt_comp = clone('HardwareProduct', id=name, name=name,
                                     product_type=optical_component,
                                     create_datetime=NOW, mod_datetime=NOW,
                                     creator=user, modifier=user)
                    surface = clone('Model', id=name, name=name,
                                    of_thing=opt_comp,
                                    model_definition_context=optics,
                                    type_of_model=LOM,
                                    create_datetime=NOW, mod_datetime=NOW,
                                    creator=user, modifier=user)
                    new_objs.append(surface)
                    # TODO: add id and name to each Acu
                    hw_ref_des = get_next_ref_des(opt_sys, opt_comp)
                    hw_acu_id = get_acu_id(opt_sys.id, hw_ref_des)
                    hw_acu_name = get_acu_name(opt_sys.name, hw_ref_des)
                    hw_usage = clone('Acu', assembly=opt_sys,
                                id=hw_acu_id, name=hw_acu_name,
                                reference_designator=hw_ref_des,
                                component=opt_comp, create_datetime=NOW,
                                mod_datetime=NOW, creator=user,
                                modifier=user)
                    model_ref_des = get_next_ref_des(opt_sys_model, surface)
                    model_acu_id = get_acu_id(opt_sys_model.id, hw_ref_des)
                    model_acu_name = get_acu_name(opt_sys_model.name,
                                                  hw_ref_des)
                    model_usage = clone('Acu', assembly=opt_sys_model,
                                    id=model_acu_id, name=model_acu_name,
                                    reference_designator=model_ref_des,
                                    component=surface, create_datetime=NOW,
                                    mod_datetime=NOW, creator=user,
                                    modifier=user)
                    new_objs += [hw_usage, model_usage]
            if new_objs:
                orb.save(new_objs)
                dispatcher.send(signal="new objects", objs=new_objs)
        else:
            orb.log.debug('  no surface names were found.')
    else:
        orb.log.debug('  no data was found.')

