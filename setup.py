# -*- encoding: utf-8 -*-

try:

    from setuptools import setup

except ImportError:

    from distutils.core import setup

setup(
    name='TagCache',
    version='0.1',
    description='A small file-based cache library with tag support',
    long_description=open("README.md").read(),
    classifiers=[
	"Programming Language :: Python :: 2",
	"Programming Language :: Python :: 2.7",
	"Topic :: Software Development :: Libraries",
    ],
    author='Huang junwen',
    author_email='kassarar@gmail.com',
    url='https://github.com/huangjunwen/tagcache',
    license='MIT',
    keywords='python cache tag file-based',
    packages=['tagcache'],
    zip_safe=False,
)

