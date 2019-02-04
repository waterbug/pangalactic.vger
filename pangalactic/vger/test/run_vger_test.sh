#!/bin/bash

# NOTE:  this test script assumes:
# [1] a crossbar server has been started locally, using the
#     script crossbar_for_test_vger.sh
#     (this can be on a separate host, e.g. pangalactic.us,
#     if --cb_host is set to that host
# [2] the server cert file for the crossbar host has been
#     copied to ~/vger_home/server_cert.pem
#     (otherwise, use the --cert option to specify where
#     the crossbar host cert file is)
# [3] postgresql has been installed, initialized, and started by the local user
#     'user':
#     pg_ctl init -D ~/vger_home/vgerdb
#     pg_ctl start -D ~/vger_home/vgerdb

# for crossbar running on a remote server:
# --cert ~/remote_server_cert.pem
# --cb_host [remote server address]
# --cb_port [nnnn] [default port is 8080]

python ~/gitlab/pangalactic.vger/pangalactic/vger/vger.py \
    --home ~/vger_home \
    --db_url postgresql://user@localhost:5432/vgerdb \
    --debug --test

