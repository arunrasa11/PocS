from google.cloud import storage
import os
import logging

logging.basicConfig(filename='../logs/cpy2GCS.log', filemode='w',
                    format='cpy2GCS - %(levelname)s - %(message)s')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def main(filepath, details):

    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = details['key']

        client = storage.Client(details['project_id'])
        bucket = client.get_bucket(details['bucket_name'])
        logger.info('Module: {} Method: {} - GCS Client created and verified bucket name'.format(__name__, main.__name__))

        for file in os.listdir(filepath):
            if file.endswith('.csv'):
                logger.info('Module: {} Method: {} - Uploading file {}'.format(__name__, main.__name__, file))
                blob = bucket.blob(details['bucket_file'] + file)
                blob.upload_from_filename(filepath + file)
                logger.info('Module: {} Method: {} - upload successful {}'.format(__name__, main.__name__, file))

    except Exception as e:
        logger.error('Module: {} Method: {} - Connection error'.format(__name__, main.__name__))
        logger.error('Module: {} Method: {} - {}'.format(__name__, main.__name__, str(e)))
        quit()


if __name__ == '__main__':

    filepath = '../ofiles/'

    details = {'project_id': 'soy-reporter-399414',
               'bucket_name': 'arun-data-staging-bucket',
               'bucket_file': 'csvFiles/',
               'key': '../Keys/soy-reporter-399414-422a57a9d10d.json'}

    main(filepath, details)