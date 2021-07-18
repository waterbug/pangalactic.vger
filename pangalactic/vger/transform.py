#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Script to transform serialized data to a specified schema.
"""
import argparse
import ruamel_yaml as yaml

from pangalactic.core.mapping import to_2_0_0


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
    f = open('x-2.0.0-data.yaml', 'w')
    f.write(data_out)
    f.close()

