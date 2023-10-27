import csv
from psqlActions import *
import logging


def write_csv(data, file):

    try:
        with open(file, 'w') as ofile:
            csvwriter = csv.writer(ofile)
            csvwriter.writerows(data)

        logger.info('Module: {} Method: {} - File {} successfully written'.format(__name__, write_csv.__name__, ofile.name))

    except Exception as e:
        logger.critical('Module: {} Method: {} - {}'.format(__name__, write_csv.__name__, str(e)))



def main():

    try:
        dbdetail = {'database': 'dvdrental',
                    'user': 'arun',
                    'host': 'localhost',
                    'password': '12345',
                    'port': 5432}

        conn, cursor = connect_psql(dbdetail)

        # query = ''' select count(*) from actor '''
        path = '/Users/arun/PycharmProjects/PocS/ofiles/'

        # table extracts

        # Actor table
        query = ''' select * from actor order by first_name asc'''
        result = extract_data(cursor, query)
        file = 'actorTable.csv'
        write_csv(result, path+file)

        # Address Table
        query = ''' select * from address order by address_id asc'''
        result = extract_data(cursor, query)
        file = 'addressTable.csv'
        write_csv(result, path + file)

        # category Table
        query = ''' select * from category order by name asc'''
        result = extract_data(cursor, query)
        file = 'categoryTable.csv'
        write_csv(result, path + file)

        # Country Table using sql alchemy and dataframes

        query = '''select * from country order by country asc'''
        result_df = connect_alchemy(dbdetail, query)
        file = 'countryTable.csv'
        result_df.to_csv(path+file, index=False)
        logger.info('Module: {} Method: {} - File {} successfully written'.format(__name__, main.__name__, path+file))

    except Exception as e:
        logger.critical('Module: {} Method: {} - {}'.format(__name__, main.__name__, str(e)))
        quit()

    finally:
        cursor.close()
        conn.close()
        logger.info('Module: {} Method: {} - Connection and cursor closed'.format(__name__, main.__name__))


if __name__ == '__main__':

    logging.basicConfig(filename='../logs/Postgres2csv.log', filemode='w',
                        format='Postgres2csv - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    main()
