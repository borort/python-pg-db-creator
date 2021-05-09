#!/usr/bin/python
import psycopg2
import os
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import config

def create_db(db_name, db_username, db_password):

      conn = None
      try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            query = '''CREATE DATABASE {} ;'''
            
            # terminate any process that use the template database
            cur.execute(f"""SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = 'template1'
                        AND pid <> pg_backend_pid();""")
            
            DB_NAME = db_name
            DB_USERNAME = db_username
            DB_PASSWORD = db_password

            # create a new database
            cur.execute(sql.SQL(query).format(sql.Identifier(DB_NAME)))
            cur.execute(f"""CREATE USER {DB_USERNAME} WITH PASSWORD '{DB_PASSWORD}';""")
            cur.execute(f"""GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} TO {DB_USERNAME};""")

            cur.close()
            conn.close()

            params['dbname'] = DB_NAME
            conn = psycopg2.connect(**params)
            
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            cur.execute(f"""REVOKE CONNECT ON DATABASE {DB_NAME} FROM PUBLIC;
                        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {DB_USERNAME};
                        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {DB_USERNAME};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {DB_USERNAME};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {DB_USERNAME};""")

            # open sql fild and execute sql statements in the file
            sql_file = open(os.path.join(os.path.dirname(__file__),"db.sql"))
            cur.execute(sql_file.read())

            cur.close()
            conn.close()

      except (Exception, psycopg2.DatabaseError) as error:
            print(error)

      finally:
            if conn is not None:
                  conn.close()
                  print('Database connection closed.')


if __name__ == "__main__":
      create_db('testdb001', 'testdba001', 'test123')