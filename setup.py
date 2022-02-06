"""
Setup script for pangalactic.vger, the Virtual Galactic Engineering Repository.
"""
from setuptools import setup, find_packages

VERSION = open('VERSION').read()[:-1]

long_description = (
    "pangalactic.vger implements the Virtual Galactic Engineering Repository, "
    "the repository service for the Pan Galactic Engineering Framework.")

setup(
    name='pangalactic.vger',
    version=VERSION,
    description="The Virtual Galactic Engineering Repository",
    long_description=long_description,
    author='Stephen Waterbury',
    author_email='stephen.c.waterbury@nasa.gov',
    maintainer="Stephen Waterbury",
    maintainer_email='waterbug@pangalactic.us',
    license='TBD',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'vger = vger.bin']
        },
    zip_safe=False
)

