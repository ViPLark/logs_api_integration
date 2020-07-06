from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import logging

import requests
import urllib3

import utils

config = utils.get_config()
CH_HOST = config['clickhouse']['host']
CH_USER = config['clickhouse']['user']
CH_PASSWORD = config['clickhouse']['password']
CH_VISITS_TABLE = config['clickhouse']['visits_table']
CH_HITS_TABLE = config['clickhouse']['hits_table']
CH_DATABASE = config['clickhouse']['database']
SSL_VERIFY = (config['disable_ssl_verification_for_clickhouse'] == 0)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger('logs_api')


def _process_response(r):
    result = r.text
    if r.status_code != 200:
        raise ValueError(result)
    return result


def _get_auth():
    return None if (CH_USER == '') and (CH_PASSWORD == '') else (CH_USER, CH_PASSWORD)


def get_clickhouse_data(query, host=CH_HOST):
    """Returns ClickHouse response"""
    logger.debug(query)
    r = requests.post(host, data=query, auth=_get_auth(), verify=SSL_VERIFY)
    return _process_response(r)


def upload(table, content, host=CH_HOST):
    """Uploads data to table in ClickHouse"""
    content = content.encode('utf-8')
    query_dict = {
         'query': 'INSERT INTO ' + table + ' FORMAT TabSeparatedWithNames '
    }
    r = requests.post(host, data=content, params=query_dict, auth=_get_auth(), verify=SSL_VERIFY)
    return _process_response(r)


def get_source_table_name(source, with_db=True):
    """Returns table name in database"""
    if source == 'hits':
        table = CH_HITS_TABLE
    elif source == 'visits':
        table = CH_VISITS_TABLE
    else:
        raise ValueError('Allowed values for the @source are: hits|visits')
    return f'{CH_DATABASE}.{table}' if with_db else table


def get_tables():
    """Returns list of tables in database"""
    return get_clickhouse_data('SHOW TABLES FROM {db}'.format(db=CH_DATABASE))\
        .strip().split('\n')


def get_dbs():
    """Returns list of databases"""
    return get_clickhouse_data('SHOW DATABASES').strip().split('\n')


def is_table_present(source):
    """Returns whether table for data is already present in database"""
    return get_source_table_name(source, with_db=False) in get_tables()


def is_db_present():
    """Returns whether a database is already present in clickhouse"""
    return CH_DATABASE in get_dbs()


def create_db():
    """Creates database in clickhouse"""
    return get_clickhouse_data('CREATE DATABASE {db}'.format(db=CH_DATABASE))


def get_ch_field_name(field_name):
    """Converts Logs API parameter name to ClickHouse column name"""
    prefixes = ['ym:s:', 'ym:pv:']
    for prefix in prefixes:
        field_name = field_name.replace(prefix, '')
    return field_name[0].upper() + field_name[1:]


def drop_table(source):
    """Drops table in ClickHouse"""
    query = 'DROP TABLE IF EXISTS {table}'.format(table=get_source_table_name(source))
    get_clickhouse_data(query)


def create_table(source, fields):
    """Creates table in ClickHouse for hits/visits with particular fields"""
    tmpl = '''
        CREATE TABLE {table_name} (
            {fields}
        ) ENGINE = {engine}
    '''
    field_tmpl = '{name} {type}'
    field_statements = []

    table_name = get_source_table_name(source)
    engine = 'Log'
    if any((
            source == 'hits' and 'ym:pv:date' in fields and 'ym:pv:clientID' in fields,
            source == 'visits' and 'ym:s:date' in fields and 'ym:s:clientID' in fields,
    )):
        engine = 'MergeTree(Date, intHash32(ClientID), (Date, intHash32(ClientID)), 8192)'
    else:
        raise ValueError('Allowed values for the @source are: hits|visits')

    ch_field_types = utils.get_ch_fields_config()
    ch_fields = [get_ch_field_name(field) for field in fields]
    
    for i in range(len(fields)):
        field_statements.append(
            field_tmpl.format(name=ch_fields[i], type=ch_field_types[fields[i]])
        )
    
    field_statements = sorted(field_statements)
    query = tmpl.format(table_name=table_name,
                        engine=engine,
                        fields=',\n'.join(sorted(field_statements)))

    get_clickhouse_data(query)


def save_data(source, fields, data):
    """Inserts data into ClickHouse table"""
    if not is_db_present():
        logger.info('Database created')
        create_db()

    if not is_table_present(source):
        logger.info('Table created')
        create_table(source, fields)

    upload(get_source_table_name(source), data)


def is_data_present(start_date_str, end_date_str, source):
    """Returns whether there is a records in database for particular date range and source"""
    if not is_db_present():
        return False

    if not is_table_present(source):
        return False

    table_name = get_source_table_name(source)
    query = '''
        SELECT count()
        FROM {table}
        WHERE Date >= '{start_date}' AND Date <= '{end_date}'
    '''.format(table=table_name,
               start_date=start_date_str,
               end_date=end_date_str)

    visits = get_clickhouse_data(query)
    is_null = (visits == '') or (visits.strip() == '0')
    return not is_null
