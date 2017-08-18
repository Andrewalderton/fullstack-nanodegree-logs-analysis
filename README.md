Logs Analysis
=============

## Description

Project for the Udacity Full Stack Nanodegree:

> You've been hired onto a team working on a newspaper site. The user-facing newspaper site frontend itself, and the database behind it, are already built and running. You've been asked to build an **internal reporting tool** that will use information from the database to discover what kind of articles the site's readers like.

The database contains three tables: *articles, authors,* and a *log* table which tracks certain information each time an article is accessed. The reporting tool has been designed to answer three questions:

* What are the most popular three articles of all time?
* Who are the most popular article authors of all time?
* On which days did more than 1% of requests lead to errors?

## Setup

You will need Vagrant and Virtualbox, as well as Python, installed in order to run the program.

Download or clone the repository.

Launch the Vagrant virtual machine by running `vagrant up` in the command line, then log in using `vagrant ssh`.

[Download the SQL data here.](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip) The file inside is called `newsdata.sql`. Put this file into the vagrant directory, which is shared with your virtual machine.

To load the data, run the command: `psql -d news -f newsdata.sql`. This will connect to the installed database server and execute the SQL commands in the downloaded file, creating tables and populating them with data.

## Usage

Log in to the Vagrant virtual machine using the steps outlined above. Follow any prompts to change into the Vagrant shared directory.

To run the program, enter `python3 logs_analysis.py`. The formatted results will display in the terminal.
