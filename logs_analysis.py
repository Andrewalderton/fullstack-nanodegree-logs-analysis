#!/usr/bin/env python3

import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect("dbname=news")
cursor = conn.cursor()

# What are the most popular three articles of all time?
cursor.execute(
    """SELECT title, count(*) FROM articles
    LEFT JOIN log ON log.path
    LIKE CONCAT ('%', articles.slug)
    GROUP BY articles.title
    ORDER BY count(*)
    DESC LIMIT 3;"""
)
results = cursor.fetchall()
print("\nMost popular 3 articles of all time:\n")
for i in range(0, len(results), 1):
    print("\'" + results[i][0] + "\' - " + str(results[i][1]) + " views")

# Who are the most popular article authors of all time?
cursor.execute(
    """SELECT name, count(*) FROM authors
    LEFT JOIN articles ON articles.author = authors.id
    LEFT JOIN log ON log.path
    LIKE CONCAT ('%', articles.slug)
    GROUP BY authors.name
    ORDER BY count(*) DESC;"""
)
results = cursor.fetchall()
print("\n\nMost popular article authors of all time:\n")
for i in range(0, len(results), 1):
    print(results[i][0] + " - " + str(results[i][1]) + " views")

# On which days did more than 1% of requests lead to errors?
cursor.execute(
    """SELECT DATE(time) as date, sum(case when status = '404 NOT FOUND' then 1
    else 0 end) / (count(log.time)/100)::float as error
    FROM log GROUP BY date
    HAVING (count(log.time) / 100) < sum(case when status = '404 NOT FOUND'
    then 1 else 0 end)
    ORDER BY error DESC;"""
)
results = cursor.fetchall()
print("\n\nDays where more than 1% of requests led to errors:\n")
for i in range(0, len(results), 1):
    print(str(results[i][0]) + " - " + str(round(results[i][1], 2)) +
          "% errors\n")

# Close the database connection
conn.close()
