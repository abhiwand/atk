from setuptools import setup
import os

setup(
    # Application name:
    name="intelanalytics",

    # Version number (initial):
    version=u"0.9-" + BUILD_NUMBER,

    # Application author details:
    author="Intel",
    author_email="bleh@intel.com",

    # Packages
    packages=["intelanalytics","intelanalytics/core","intelanalytics/rest","intelanalytics/tests"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://analyticstoolkit.intel.com",

    #
    license="LICENSE.txt",
    description="Intel Analytics Toolkit",

    long_description=open("README").read(),

    # Dependent packages (distributions)
    install_requires=[
        'bottle >= 0.12',
        'numpy >= 1.8.1',
        'requests >= 2.2.1',
        'ordereddict >= 1.1',
        'decorator >= 3.4.0',
        'pandas >= 0.15.0',
    ],
)
