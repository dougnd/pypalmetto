from setuptools import setup

setup(
    name='pypalmetto',
    packages=['pypalmetto'],
    version='0.1.0',
    install_requires=[
                  'cloudpickle', 'dataset', 'sh', 'tabulate'
                        ],
    description = 'Palmetto Python Module',
    author = 'Douglas Dawson',
    author_email = 'dougnd@gmail.com',
    url = 'https://github.com/dougnd/pypalmetto',
    keywords = ['pypalmetto', 'pbs', 'palmetto'],
    classifiers = [],
)
