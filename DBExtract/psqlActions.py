import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def connect_psql(dbdetail):

    try:
        conn = psycopg2.connect(database = dbdetail['database'],
                            user = dbdetail['user'],
                            host=  dbdetail['host'],
                            password = dbdetail['password'],
                            port = dbdetail['port'])

        cursor = conn.cursor()
        logger.info('Module: {} Method: {} - DB connection Established'.format(__name__, connect_psql.__name__))
        return conn, cursor

    except psycopg2.DatabaseError as exp:
        logger.error('Module: {} Method: {} - Connection error'.format(__name__, extract_data.__name__))
        logger.error('Module: {} Method: {} - {}'.format(__name__, extract_data.__name__, str(exp)))
        quit()

    except Exception as e:
        logger.error('Module: {} Method: {} - Error:Connection error'.format(__name__, connect_psql.__name__))
        logger.error('Module: {} Method: {} - Exception:{}'.format(__name__, connect_psql.__name__, str(e)))
        quit()

def extract_data(cursor, query):

    try:
        cursor.execute(query)
        logger.info('Module: {} Method: {} - Query executed'.format(__name__, extract_data.__name__))
        column_names = tuple([desc[0] for desc in cursor.description])
        result = cursor.fetchall()
        result.insert(0, column_names)
        logger.info('Module: {} Method: {} - Fetch successful'.format(__name__, extract_data.__name__))

        return result

    except Exception as e:
        logger.error('Module: {} Method: {} - Connection error'.format(__name__, extract_data.__name__))
        logger.error('Module: {} Method: {} - {}'.format(__name__, extract_data.__name__, str(e)))


def connect_alchemy(dbdetail, query):

    try:
        psql_dtl = 'postgresql://' + dbdetail['user'] + ':' + dbdetail['password'] + '@' + dbdetail['host'] + '/' +  \
                    dbdetail['database']
        engine = create_engine(psql_dtl)
        df = pd.read_sql_query(query, engine)
        return df

    except Exception as e:
        logger.error('Module: {} Method: {} - Error:Connection error'.format(__name__, connect_alchemy.__name__))
        logger.error('Module: {} Method: {} - Exception:{}'.format(__name__, connect_alchemy.__name__, str(e)))

    finally:
        engine.dispose()
