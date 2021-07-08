#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Script to transform serialized data to a specified schema.
"""
import argparse
import ruamel_yaml as yaml


def to_2_0_0(sos):
    """
    Convert any serialized Acu objects that reference Activity instances to
    serialized ActCompRel objects.

    Args:
        sos (list):  serialized objects to be inspected
    """
    by_oids = {so.get('oid') : so for so in sos}
    for so in sos:
        if (so.get('_cname') == 'Acu' and
            so.get('assembly') in by_oids and
            by_oids[so.get('assembly')].get('_cname') in ['Activity',
            'Mission']):
            print('* Acu found with Mision/Activity (oid: "{}") ...')
            so['_cname'] = 'ActCompRel'
            so['composite_activity'] = so['assembly']
            so['sub_activity'] = so['component']
            so['sub_activity_role'] = so['reference_designator']
            del so['assembly']
            del so['component']
            print('  ... transformed to ACR.')
    return sos


if __name__ == '__main__':
    desc = """Script to transform serialized data."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('file_path',
                        help='path to the serialized data (yaml) file to read')
    # parser.add_argument("schema",
                        # help="the schema version to be targeted")
    args = parser.parse_args()
    f = open(args.file_path)
    data = f.read()
    f.close()
    sos = yaml.safe_load(data)
    sos_out = to_2_0_0(sos)
    data_out = yaml.safe_dump(sos_out, default_flow_style=False)
    f = open('x-data.yaml', 'w')
    f.write(data_out)
    f.close()

