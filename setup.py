# -*- encoding: utf-8 -*-

try:

    from setuptools import setup

except ImportError:

    from distutils.core import setup

from tagcache import metadata

setup(
    name='TagCache',
    version=metadata.version,
    description=metadata.description,
    long_description=open("README.md").read(),
    classifiers=[
	"Programming Language :: Python :: 2",
	"Programming Language :: Python :: 2.7",
	"Topic :: Software Development :: Libraries",
    ],
    author=metadata.authors[0],
    author_email=metadata.emails[0],
    url=metadata.url,
    license=metadata.license,
    keywords='python cache tag file-based',
    packages=['tagcache'],
    zip_safe=False,
)

