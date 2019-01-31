======================
ETL_Toy
======================

.. toctree::
:maxdepth: 5

.. contents:: Table of Contents
    :depth: 3

About ETL_Toy
=========================

What it is
--------------
**ETL_Toy** is simple CLI tool for ETL operations. It can be used for moving data from/to **CSV file**, **Excel workbook** and **Postgresql database**.

Installation
-------------
Tool can be installed by command:

.. code-block:: none

    python setup.py install

ETL_Toy description
---------------------
ETL_Toy is based on ordered working steps. All steps are stored in transformation. Data is passed between steps. There three types of steps:

- input step,
- transformation step,
- load step.

First user has to create new transformation in **Python** and save it into file. Then **specify data targets** for running transformation and finally **run transformation**.


Using ETL_Toy
==============

Making transformation
-----------------------
First lets make transformation. Main class for it is **Transformation**. All the steps you created, you have to add to transformation.

**Example**

.. code-block:: Python

    trans= Transformation()

    read_csv = Read_csv('name', 'path/to/data/targets',';')
    trans.add_step(read_csv)

    write_to_excel = Write_To_Excel('name2', 'path/to/data/targets')
    trans.add_step(write_to_excel)

Saving/Loading
---------------
After you created whole transformation, you can directly run it or save it and use it later by CLI tool. You can later load transformation in Python, change it and save it again.

.. code-block:: Python

    trans = Transformation.load('path/to/file')
    trans.add_step(dummy)
    trans.save('path/to/file')

Data targets
-------------
All information about file paths and database are stored in one separated file, so you can change information there and you need not change whole transformation.
Each data target has unique name. Files data targets contains only **path**, Excel **path** and **sheet name** and database **hostname**, **database name**, **port**, **username** and **password**.

**Example**

.. code-block:: none

    [INPUT_CSV]
    file = absolute/path/csv_example.csv

    [INPUT_EXCEL]
    file = absolute/path/excel_example.xlsx
    sheet = sheet1

    [POSTGRESQL]
    dbname = postgres
    host = localhost
    port = 5454
    user = postgres
    password = tmp

Running
---------
User can run ETL_Toy by command:

.. code-block:: none

    python -m ETL_Toy -l <logging file> -d <data targets file>  <target file with transformation>

Or easier by command:

.. code-block:: none

    ETL_Toy -l <logging file> -d <data targets file>  <target file with transformation>


Information about running transformation are stored in log file.

**Log example**

.. code-block:: none

    2019-01-31 13:50:49,451 - INFO - transformation - ETL_Toy started!
    2019-01-31 13:50:49,451 - INFO - read_csv - Starting new read_csv job - reader1!
    2019-01-31 13:50:49,451 - INFO - read_csv - Read_csv job - reader1 ended - 3 lines, 5 columns.
    2019-01-31 13:50:49,451 - INFO - sort_data - Starting new sorting job - sort1!
    2019-01-31 13:50:49,451 - INFO - sort_data - Sort_data job - sort1 ended - 3 lines, 5 columns.
    2019-01-31 13:50:49,451 - INFO - write_to_excel - Starting new write_to_excel job - excel_write2!
    2019-01-31 13:50:49,467 - INFO - write_to_excel - Write_to_excel job - excel_write2 ended - 3 lines, 5 columns.
    2019-01-31 13:50:49,467 - INFO - transformation - ETL_Toy finished successfully!


Jobs
-----
**Input steps**:

- **Read_excel** - extract data from Excel workbook
- **Read_csv** - extract data from CSV file
- **Read_Postgresql** - extract data from Postgresql database table

**Transformation steps**:

- **Filter_Columns** - remove columns based on blacklist/whitelist
- **Mapping** - map values in selected column
- **Sort_Data** - sort dataset by selected column
- **String_operations** - string lower/upper/capitalize
- **Dummy** - do nothing

**Loading  steps**:

- **Write_To_Excel** - write data to new Excel workbook
- **Write_To_CSV** - write data into file with CSV format
- **Write_To_Postgresql** - insert data into existing database table



ETL_Toy library
==================

ETL_Toy.data.data
------------------

.. automodule:: ETL_Toy.data.data
:members:
        :undoc-members:
        :show-inheritance:

ETL_Toy.jobs
------------------

.. automodule:: ETL_Toy.jobs.steps
:members:
        :undoc-members:
        :show-inheritance:
