#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

readme = open("README.rst").read()

requirements = open("requirements.txt").read().split("\n")
test_requirements = []

setup(
    name="drmr",
    version="1.1.0",
    description="A tool for submitting pipeline scripts to distributed resource managers.",
    long_description=readme + "\n\n",
    author="The Parker Lab, Vivek Rai",
    author_email="parkerlab-software@umich.edu, mail@raivivek.in",
    url="https://github.com/raivivek/drmr",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'drmr=scripts.drmr:main',
            'drmrc=scripts.drmrc:main',
            'drmrarray=scripts.drmrarray:main',
            'drmrm=scripts.drmrm:main',
        ],
    },
    include_package_data=True,
    install_requires=requirements,
    python_requires='>=3.8',
    license="GPLv3+",
    zip_safe=False,
    keywords="DRM distributed resource workload manager pipeline",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    test_suite="tests",
)