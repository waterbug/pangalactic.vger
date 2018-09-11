#!/bin/bash

$PYTHON setup.py install 

# Add more build steps here, if they are necessary.

# install the repo script here (easier than setup.py)
cp pangalactic/vger/vger.py $PREFIX/bin/

# See
# http://docs.continuum.io/conda/build.html
# for a list of environment variables that are set during the build process.

