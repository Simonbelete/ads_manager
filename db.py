import os
import psycopg2

from dotenv import load_dotenv


load_dotenv()  # take environment variables from .env.

"""
    Global vars
"""
PARAM = {
        'host': os.getenv('DB_HOST'),                               # type: string, Database Host Name from `.env` file
        'port': os.getenv('DB_PORT'),                               # type: string, Database Host Name from `.env` file                                              
        'user': os.getenv('DB_USER'),                               # type: string, Database Host Name from `.env` file
        'password': os.getenv('DB_PASSWORD'),                       # type: string, Database Host Name from `.env` file
        'database': os.getenv('DB_DATABSE')                         # type: string, Database Host Name from `.env` file
    }
    
def connect():
    """ Connect to postgresql.
        On the event of no connection close the script.

        Returns
        -------
            void
    """
    try:
        print('Connecting to the PostgreSQL databse...')
        connection = psycopg2.connect(**PARAM)
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(0)