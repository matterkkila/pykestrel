
from setuptools import setup, find_packages

setup(
    name='pykestrel',
    version='0.0.1',
    description='A python kestrel client',
    long_description='A python kestrel client',
    keywords='queues kestrel memcached',
    author='Matt Erkkila',
    author_email='matt@matterkkila.com',
    url='http://github.com/empower/pykestrel',
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'python-memcached>=1.45',
    ]
)