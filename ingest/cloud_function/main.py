import logging
from ingest import *
from flask import escape


def ingest_flights(request):
    try:
        logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)
        json = request.get_json(force=True)

        if escape(json['token']) != 'DI8TWPzTedNF0b3B8meFPxXSWw6m3bKG':
            logging.info('Ignoring request without valid token')
            return

        year = escape(json['year']) if 'year' in json else None
        month = escape(json['month']) if 'month' in json else None
        bucket = escape(json['bucket'])  # required

        if year is None or month is None or len(year) == 0 or len(month) == 0:
            year, month = (2015, 2)
        logging.debug('Ingesting year={} month={}'.format(year, month))
        gcsfile = ingest(year, month, bucket)
        logging.info('Success ... ingested to {}'.format(gcsfile))

    except DataUnavailable as e:
        logging.info('Try again latter {}'.format(e.message))