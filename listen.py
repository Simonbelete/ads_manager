import os
import sys
import time
import select
import psycopg2
import socket

from logger import logger
from dotenv import load_dotenv
from blender import bcolors

class Listen(object):
    def __init__(self):
        load_dotenv()  # take environment variables from .env.
        self.NOTIFY_NAME = 'table_update' # change this to the postgresql notification name
        self.param = {
            'host': os.getenv('DB_HOST'),                               # type: string, Database Host Name from `.env` file
            'port': os.getenv('DB_PORT'),                               # type: string, Database Host Name from `.env` file                                              
            'user': os.getenv('DB_USER'),                               # type: string, Database Host Name from `.env` file
            'password': os.getenv('DB_PASSWORD'),                       # type: string, Database Host Name from `.env` file
            'database': os.getenv('DB_DATABSE')                         # type: string, Database Host Name from `.env` file
        }

    def connect(self):
        """ Connect to postgresql.
            On the event of no connection close the script.

            Returns
            -------
                void
        """
        try:
            print(f'{bcolors.OKBLUE}INFO: Connecting to the PostgreSQL databse... {bcolors.ENDC}')
            self.connection =  psycopg2.connect(**self.param)

            if self.connection != None:
                print(f'{bcolors.OKGREEN}SUCESS: Successful connected to PostgreSQL{bcolors.ENDC}')
                self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        except (psycopg2.DatabaseError, psycopg2.OperationalError) as ps_error:
            print(f'{bcolors.FAIL}ERROR: Failed to connect to PostgreSQL{bcolors.ENDC}')
            logger.exception(str(ps_error))
            
            # Wait sometime before re-connecting
            print(f'{bcolors.HEADER}INFO: Retrying...{bcolors.ENDC}')
            time.sleep(10) # Wait 10sec
            self.connect()

    def run_forever(self):
        """ """
        try:
            # Connect to postgresql databse
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f'LISTEN {self.NOTIFY_NAME};')

            print(f'{bcolors.OKBLUE}INFO: Waiting for notifications on channel {self.NOTIFY_NAME} {bcolors.ENDC}')

            while True:
                # This is a blocking call that will return if and when any file descriptor in the list has new data
                # We see that there is fresh information on our connection, better poll and read!
                select.select([self.connection], [], [], 10) # Timeout 10sec
                # Needed to get the actual message
                self.connection.poll()
                if self.connection.notifies != []:
                    # Pop notification from list
                    notify = self.connection.notifies.pop(0)
                    j_notify = json.loads(notify.payload)
                    print(notify)
        except (socket.error, select.error) as s_ex:
            # Inorder to limit the number of open connection to 1
            # close the open connection
            self.connection.close()
            logger.exception(str(s_ex))
            print(f'{bcolors.FAIL}ERROR: Error occured in waiting for notifications {bcolors.ENDC}')
            # restart it
            self.run_forever()
        except KeyboardInterrupt as k_ex:
            print('Caught Keyboard Interrupt...')
            logger.exception('Caught keyboard interrupt from postgresql listening')
            sys.exit(1)
        except Exception as ex:
            # Inorder to limit the number of open connection to 1
            # close the open connection
            self.connection.close()
            print(f'{bcolors.FAIL}Error: Unknown error occured{bcolors.ENDC}\n\n')
            print(f'{bcolors.WARNING}*** Restarting Server ***{bcolors.ENDC}\n\n')
            logger.exception(str(ex))
            self.run_forever()
        finally:
            # Inorder to limit the number of open connection to 1
            # close the open connection
            self.connection.close()
            