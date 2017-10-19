#!/usr/bin/env python

from setuptools import setup

requirements = [
    'boto3',
    'pandas<0.20',
    'psycopg2',
]

test_requirements = [
    'pytest',
]

setup(
    name='demae',
    version='0.8.1',
    description="",
    author="Kazato Sugimoto",
    author_email='kazato.sugimoto@gmail.com',
    url='https://github.com/uiureo/demae',
    packages=[
        'demae',
        'demae.source',
        'demae.dest',
    ],
    package_dir={'demae':
                 'demae'},
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='demae',
    classifiers=[
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
