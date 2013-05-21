import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()
CHANGES = "" # open(os.path.join(here, 'CHANGES.txt')).read()

requires = [
    #'flexihash',
    #'pystun',
    #'PyDispatcher',
    'pydht==0.0.3',
    'pystun==0.0.2.1-shish',
    'pycrypto',
    'pyinotify',
    'pynetinfo',
    'web.py',

    # testing
    'nose',
    'coverage',
    'unittest2',
    'mock',
    ]

setup(name='Chunker',
      version='0.0',
      description='Chunker',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        ],
      author='',
      author_email='',
      url='',
      keywords='p2p',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='chunker',
      install_requires=requires,
      dependency_links = [
        'http://github.com/shish/pydht/tarball/master#egg=pydht-0.0.3',
        'http://github.com/shish/pystun/tarball/master#egg=pystun-0.0.2.1-shish',
      ],
      entry_points="""\
      [console_scripts]
      chunker = chunker.int_cli:main
      chunker-web = chunker.int_web:main
      """,
      )
