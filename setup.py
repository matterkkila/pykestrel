
from setuptools import setup, find_packages

setup(
    name='pykestrel',
    version='0.5.1',
    description='A python kestrel client',
    long_description='A python kestrel client based on the python-memcached library.',
    keywords='queues kestrel memcached',
    author='Matt Erkkila',
    author_email='matt@matterkkila.com',
    maintainer='Matt Erkkila',
    maintainer_email='matt@matterkkila.com',
    url='https://github.com/matterkkila/pykestrel',
    download_url='https://github.com/matterkkila/pykestrel/tarball/0.5.1',
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'python-memcached>=1.45',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python',
    ],
)