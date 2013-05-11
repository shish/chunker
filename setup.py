import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()
CHANGES = "" # open(os.path.join(here, 'CHANGES.txt')).read()

requires = [
    #'flexihash',
    #'pystun',
    #'PyDispatcher',
    'pydht',
    'pystun',
    'pycrypto',

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
      entry_points="""\
      [console_scripts]
      chunker = chunker.main:main
      """,
      )
