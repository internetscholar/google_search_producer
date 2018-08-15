import configparser
import os
from urllib.parse import urlencode, quote_plus
import json

import boto3
import psycopg2
from psycopg2 import extras

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    c = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    c.execute("""select * from aws_credentials;""")
    aws_credential = c.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')
    google_queue = sqs.get_queue_by_name(QueueName='google')
    google_queue.purge()

    c.execute("""select
                  *
                from
                  (select
                     *
                     , generate_series(initial_date::timestamp, final_date::timestamp, '1 day')::date as query_date
                   from
                     google_search) as google_dates
                where
                  not exists (select
                                *
                              from
                                google_subquery
                              where
                                google_subquery.query_date = google_dates.query_date
                                and google_subquery.success
                                and google_dates.query_alias = google_subquery.query_alias)""")
    google_queries = c.fetchall()
    for google_query in google_queries:
        parameters = dict()
        parameters['q'] = google_query['query']
        # set language and country of search results (https://sites.google.com/site/tomihasa/google-language-codes)
        if google_query['language_results'] is not None:
            parameters['lr'] = google_query['language_results']
        if google_query['language_interface'] is not None:
            parameters['hl'] = google_query['language_interface']
        if google_query['country_results'] is not None:
            parameters['cr'] = google_query['country_results']
        # choose time frame
        parameters['tbs'] = 'cdr:1,cd_min:{0},cd_max:{1}'.format(
            google_query['query_date'].strftime('%-m/%-d/%Y'),
            google_query['query_date'].strftime('%-m/%-d/%Y'))
        # sort by date or by relevance
        if google_query['sort_by_date']:
            parameters['tbs'] = parameters['tbs'] + ',sbd:1'
        # 100 results per page
        parameters['num'] = '100'
        # show omitted results
        parameters['filter'] = '0'
        # disable autocorrect
        parameters['nfpr'] = '1'
        # encode parameters and assemble URL
        encoded_parameters = urlencode(parameters, quote_via=quote_plus)
        # set the region where the request is made (http://www.isearchfrom.com/)
        if google_query['geo_tci'] is not None and google_query['geo_uule'] is not None:
            encoded_parameters = encoded_parameters +\
                                 '&uule={0}&tci={1}'.format(google_query['geo_uule'], google_query['geo_tci'])
        final_url = 'http://{0}/search?{1}'.format(google_query['google_domain'], encoded_parameters)

        google_subquery = {
            'query_alias': google_query['query_alias'],
            'query_date':  str(google_query['query_date']),
            'query_url': final_url
        }
        google_queue.send_message(MessageBody=json.dumps(google_subquery))

    conn.close()