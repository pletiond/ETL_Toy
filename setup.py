from setuptools import setup

setup(
    name='ETL_Toy',
    version='1.0',
    packages=['ETL_Toy', 'ETL_Toy.cli', 'ETL_Toy.data', 'ETL_Toy.jobs'],
    url='',
    license='',
    author='Ondřej Pleticha',
    author_email='o.pleticha@seznam.cz',
    description='ETL_Toy is simple CLI tool for ETL operations. It can be used for moving data from/to CSV file, Excel workbook and Postgresql database.',
    install_requires=[
        'click',
        'configparser',
        'numpy',
        'openpyxl',
        'psycopg2'
    ],
    entry_points={
        'console_scripts': [
            'ETL_Toy = ETL_Toy.cli.cli:main',
        ]
    }
)
