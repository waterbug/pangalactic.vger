#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Interface to the Linear Optical Model
"""
from scipy.io import loadmat


def get_lom_data(fpath):
    """
    Read a LOM data set from a .mat file.

    Args:
        fpath (str): path to the .mat file
    """
    return loadmat(fpath, simplify_cells=True)

def get_optical_surface_names(data):
    """
    Get the names of all surfaces in a LOM data set.

    Args:
        data (dict): LOM data set read from .mat file
    """
    return list(data["lomdata"]["SURFACES"])

def get_LOM(data):
    """
    Get the LOM data as a list of dicts, one for each surface, with the
    following keys:

        ['surfacelabel', 'rdy', 'rdx', 'thi', 'map', 'mav', 'ape', 'k',
        'surfacetype', 'coefs_note', 'coefs_vals', 'glass', 'rindex',
        'refractmode', 'decenter_type', 'decenter_vals', 'return_surf',
        'refrays', 'globalcoord_ref', 'globalcoord_xyz', 'globalcoord_abc',
        'globalcoord_abc_note', 'globalcoord_ROT', 'dRR_dRBM', 'dFIR_dRBM',
        'dWFE_dRBM', 'MASK_dRBM', 'dRR_dRK', 'dFIR_dRK', 'dWFE_dRK',
        'MASK_dRK', 'dRR_dZFE', 'dFIR_dZFE', 'dWFE_dZFE', 'MASK_dZFE', 'Sag_m',
        'Sag_mask', 'Sag_xy', 'dSag_dZFE_m']

    Args:
        data (dict): LOM data set read from .mat file
    """
    return list(data["lomdata"]["LOM"])

