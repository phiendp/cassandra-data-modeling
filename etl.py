import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster


def process_data(filename):
    """
    Process the event file and create a smaller data file
    """
    filepath = os.getcwd() + '/event_data'

    # Create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, '*'))

    full_data_rows_list = []
    for f in file_path_list:
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    # Create a smaller event data csv file called event_datafile_full CSV
    csv.register_dialect(
        'myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    smaller_file_name = 'smaller_event_datafile.csv'
    with open(smaller_file_name, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6],
                             row[7], row[8], row[12], row[13], row[16]))


def design_database(session):
    """
    Create songs, song_playlist_session and song_listened_by_users
    """
    query = "CREATE TABLE IF NOT EXISTS songs"
    query = query + "(sessionId int, itemInSession int, artist text, song text, length float, \
                PRIMARY KEY (sessionId, itemInSession))"
    try:
        session.execute(query)
    except Exception as e:
        print(e)

    query = "CREATE TABLE IF NOT EXISTS song_playlist_session"
    query = query + "(userId int, sessionId int, itemInSession int, artist text, \
            song text, firstName text, lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"
    try:
        session.execute(query)
    except Exception as e:
        print(e)

    query = "CREATE TABLE IF NOT EXISTS song_listened_by_users"
    query = query + "(song text, userId int, firstName text, lastName text,\
                PRIMARY KEY ((song), userId))"
    try:
        session.execute(query)
    except Exception as e:
        print(e)

    print("Created tables")


def inserting_data(session, filename):
    """
    Insert data into the created databases
    """
    query = "SELECT artist, song, length FROM music_library WHERE sessionId='338' and itemInSession='4'"

    with open(filename, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            query = "INSERT INTO songs (sessionId, itemInSession, artist, song, length)"
            query = query + " VALUES (%s, %s, %s, %s, %s)"

            query2 = "INSERT INTO song_playlist_session (userId, sessionId, itemInSession, \
                                                    artist, song, firstName, lastName)"
            query2 = query2 + " VALUES (%s, %s, %s, %s, %s, %s, %s)"

            query3 = "INSERT INTO song_listened_by_users (song, userId, firstName, lastName)"
            query3 = query3 + " VALUES (%s, %s, %s, %s)"

            session.execute(query, (int(line[8]), int(
                line[3]), line[0], line[9], float(line[5])))
            session.execute(query2, (int(line[10]), int(line[8]), int(line[3]),
                                     line[0], line[9], line[1], line[4]))
            session.execute(query3, (line[9], int(line[10]), line[1], line[4]))

    print("Inserted data!")


def querying(session, query):
    """
    Querying data
    """
    print("Query: ", query)
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print("Result: ", row)


def drop_tables(session):
    query1 = "DROP TABLE IF EXISTS songs"
    query2 = "DROP TABLE IF EXISTS song_playlist_session"
    query3 = "DROP TABLE IF EXISTS song_listened_by_users"
    try:
        session.execute(query1)
        session.execute(query2)
        session.execute(query3)
    except Exception as e:
        print(e)
    print("Dropped tables!")


def run():
    # Create a cluster and connected session
    cluster = Cluster()
    session = cluster.connect()

    # Create a Keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
    except Exception as e:
        print(e)

    # Set Keyspace
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)

    # Processing data
    filename = 'event_datafile_new.csv'
    process_data(filename)
    design_database(session)
    inserting_data(session, filename)

    # SELECT statements
    query = "SELECT artist, song, length FROM songs WHERE sessionId=338 and itemInSession=4"
    querying(session, query)
    query = "SELECT artist, song, firstName, lastName FROM song_playlist_session WHERE userid=10 and sessionId=182"
    querying(session, query)
    query = "SELECT firstName, lastName FROM song_listened_by_users WHERE song='All Hands Against His Own'"
    querying(session, query)
    drop_tables(session)


if __name__ == '__main__':
    run()
