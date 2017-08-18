#!/usr/bin/env python3

import psycopg2

query_one_title = "\nMost popular 3 articles of all time:\n"
query_two_title = "\n\nMost popular article authors of all time:\n"
query_three_title = "\n\nDays where more than 1% of requests led to errors:\n"

# What are the most popular three articles of all time?
query_one = (
    """SELECT title, count(*) FROM articles
    LEFT JOIN log ON log.path
    LIKE CONCAT ('%', articles.slug)
    GROUP BY articles.title
    ORDER BY count(*)
    DESC LIMIT 3;"""
)

# Who are the most popular article authors of all time?
query_two = (
    """SELECT name, count(*) FROM authors
    LEFT JOIN articles ON articles.author = authors.id
    LEFT JOIN log ON log.path
    LIKE CONCAT ('%', articles.slug)
    GROUP BY authors.name
    ORDER BY count(*) DESC;"""
)

# On which days did more than 1% of requests lead to errors?
query_three = (
    """SELECT DATE(time) as date, sum(case when status = '404 NOT FOUND' then 1
    else 0 end) / (count(log.time)/100)::float as error
    FROM log GROUP BY date
    HAVING (count(log.time) / 100) < sum(case when status = '404 NOT FOUND'
    then 1 else 0 end)
    ORDER BY error DESC;"""
)


def connect(database_name="news"):
    """Connect to the PostgreSQL database"""
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print("Unable to connect to database.")


def run_query(query):
    """Get the result of a given query"""
    db, cursor = connect()
    cursor.execute(query)
    results = cursor.fetchall()
    db.close()  # Close the database connection
    return results


def print_results(results, title):
    """Format and print results for first two queries"""
    print(title)
    for i in range(0, len(results), 1):
        print("\'" + results[i][0] + "\' - " + str(results[i][1]) + " views")


def print_error_results(results, title):
    """Format and print results for error percentage query"""
    print(title)
    for i in range(0, len(results), 1):
        print(str(results[i][0]) + " - " + str(round(results[i][1], 2)) +
              "% errors\n")


if __name__ == '__main__':
    # Get the query results
    popular_articles = run_query(query_one)
    popular_authors = run_query(query_two)
    error_days = run_query(query_three)

    # Print the results
    print_results(popular_articles, query_one_title)
    print_results(popular_authors, query_two_title)
    print_error_results(error_days, query_three_title)
