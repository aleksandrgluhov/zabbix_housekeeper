#!/usr/bin/python


"""
zabbix_housekeeper.py - program for external cron-based and mutex-protected
database partitioning.

author: Aleksandr Gluhov
"""

program = 'zabbix_housekeeper'
version = '1.0'

# imports
import os
import sys
import errno
import logging
import datetime
import MySQLdb
from dateutil.relativedelta import relativedelta


# constants
dbms_ge_56 = True
zbx_le_22 = False

db_host = ''
db_user = ''
db_password = ''
db_schema = ''	# zabbix database name
db_chset = 'utf8'

partition_name_format = 'p{}'

tables = [
    {'name': 'history', 'period': 'day', 'keep': 30},
    {'name': 'history_log', 'period': 'day', 'keep': 30},
    {'name': 'history_str', 'period': 'day', 'keep': 30},
    {'name': 'history_text', 'period': 'day', 'keep': 30},
    {'name': 'history_uint', 'period': 'day', 'keep': 30},
    {'name': 'trends', 'period': 'month', 'keep': 12},
    {'name': 'trends_uint', 'period': 'month', 'keep': 12}
]

tables_le_22 = [
    {'name': 'acknowledges', 'period': 'day', 'keep': 30},
    {'name': 'alerts', 'period': 'day', 'keep': 30},
    {'name': 'auditlog', 'period': 'day', 'keep': 30},
    {'name': 'events', 'period': 'day', 'keep': 30},
    {'name': 'service_alarms', 'period': 'day', 'keep': 30}
]

if zbx_le_22:
    tables = tables + tables_le_22

log_dir = '/var/log/{}'.format(program)
var_dir = '/var/local/{}'.format(program)
logfile_format = '{}_{}.log'
lockfile = os.path.join(var_dir, '.lock')

#tz = 'Asia/Yekaterinburg'
now = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0))
timestamp_format = '%Y_%m_%d'
timestamp = now.strftime(timestamp_format)

logfile = os.path.join(log_dir, logfile_format.format(program, timestamp))
logging_format = u'%(levelname)-8s [%(asctime)s] %(message)s'
logging_level = logging.INFO
logging.basicConfig(format=logging_format, level=logging_level, filename=logfile)


# procedures and functions
def generate_path(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def touch(filename, times=None):
    with open(filename, 'a'):
        os.utime(filename, times)


def set_lock(lockfile):
    ret_val = False
    if not get_lock(lockfile):
        touch(lockfile)
        ret_val = True

    return ret_val


def get_lock(lockfile):
    ret_val = False
    if os.path.isfile(lockfile):
        ret_val = True

    return ret_val


def remove_lock(lockfile):
    ret_val = False
    if get_lock(lockfile):
        os.remove(lockfile)
        ret_val = True

    return ret_val


def run_sql(connection, sql, ret_data=False):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        if ret_data:
            ret_val = cursor.fetchall()
        else:
            ret_val = True
    except:
        raise
        logging.error('SQL error!')
        ret_val = False

    cursor.close()
    return ret_val


def check_partition_ability(connection, dbms_ge_56):
    ret_val = False

    query_55 = 'SELECT variable_value FROM information_schema.global_variables ' \
               'WHERE variable_name = \'have_partitioning\''
    query_56 = 'SELECT plugin_status FROM information_schema.plugins ' \
               'WHERE plugin_name = \'partition\''

    result55 = 'YES'
    result56 = 'ACTIVE'

    if dbms_ge_56:
        sql = query_56
        sval = result56
    else:
        sql = query_55
        sval = result55

    data = run_sql(connection, sql, True)

    for line in data:
        if sval in line:
            ret_val = True

    return ret_val


def generate_period_date(period):
    ret_val_query, ret_val_label = None, None

    if period == 'day':
        ret_val_query, ret_val_label = now + datetime.timedelta(days=1), now

    elif period == 'week':
        weekdate = now + datetime.timedelta(days=(7 - now.weekday()))
        ret_val_query, ret_val_label = weekdate, weekdate - datetime.timedelta(days=1)

    elif period == 'month':
        monthdate = now.replace(day=1) + relativedelta(months=1)
        ret_val_query, ret_val_label = monthdate, monthdate - datetime.timedelta(days=1)

    return ret_val_query, ret_val_label


def generate_next_part_name(period):
    ret_val = ''
    period_date = generate_period_date(period)

    if not (period_date is None):
        ret_val = partition_name_format.format(period_date.strftime(timestamp_format))

    return ret_val


def generate_partition(connection, tablename, partname, partdate):
    query = 'ALTER TABLE `{}` ADD PARTITION (PARTITION {} VALUES LESS THAN (UNIX_TIMESTAMP(\'{}\') DIV 1))'
    sql = query.format(tablename, partname, partdate)
    #print sql
    ret_val = run_sql(connection, sql)
    return ret_val


def flush_partition(connection, tablename, part):
    query = 'ALTER TABLE `{}` DROP PARTITION {};'
    sql = query.format(tablename, part)
    #print sql
    ret_val = run_sql(connection, sql)
    return ret_val


def remove_old_data(connection):
    sessions_flush = 'DELETE FROM sessions WHERE lastaccess < UNIX_TIMESTAMP(NOW() - INTERVAL 1 MONTH)'
    housekeeper_flush = 'TRUNCATE housekeeper'
    auditlog_flush = 'DELETE FROM auditlog_details ' \
                        'WHERE NOT EXISTS (SELECT NULL FROM auditlog ' \
                                            'WHERE auditlog.auditid = auditlog_details.auditid)'

    ret_val = False
    return ret_val


# program entry point
if __name__ == '__main__':
    if not os.path.exists(var_dir):
        generate_path(var_dir)

    if not os.path.exists(log_dir):
        generate_path(log_dir)

#    remove_lock(lockfile)
    if set_lock(lockfile):
        logging.info('Mutex: {}, successfully obtained!'.format(lockfile))
        logging.info('Zabbix database partitioning initiated')

        db_connection = MySQLdb.connect(host=db_host, user=db_user, passwd=db_password, db=db_schema, charset=db_chset)
        pability = check_partition_ability(db_connection, dbms_ge_56)

        if pability:
            list_query = 'SELECT table_name, partition_name, lower(partition_method) as partition_method, ' \
                                'rtrim(ltrim(partition_expression)) as partition_expression, ' \
                                'partition_description, table_rows ' \
                            'FROM information_schema.partitions ' \
                            'WHERE partition_name IS NOT NULL AND table_schema = \'{}\''.format(db_schema)

            cursor = db_connection.cursor()
            cursor.execute(list_query)

            partitions = cursor.fetchall()
            part_len = len(partitions)

            if part_len > 0:
                logging.info('Found: {} active partitions'.format(part_len))

                for table in tables:
                    tablename = table['name']

                    partset = filter(lambda part: part[0] == tablename, partitions)
                    partset_len = len(partset)

                    if partset_len > 0:
                        query_date, label_date = generate_period_date(table['period'])
                        next_part_name = partition_name_format.format(label_date.strftime(timestamp_format))

                        # check if new name doesn't exist in partset
                        if len(filter(lambda next_part: next_part[1] == next_part_name, partset)) == 0:
                            gen = generate_partition(db_connection, tablename, next_part_name, query_date)

                            if gen:
                                logging.info('Partition: {} for table: {} successfully created'.format(next_part_name,
                                                                                                           tablename))
                            else:
                                logging.warning('Creation of partition: {} for table: {} failed'.format(next_part_name,
                                                                                                       tablename))

                            for part in partset:
                                age_delta = relativedelta(now, datetime.datetime.strptime(part[1], 'p%Y_%m_%d'))
                                td = now - datetime.datetime.strptime(part[1], 'p%Y_%m_%d')
                                age_d = td.days
                                age_w = age_d // 7
                                age_y = age_delta.years
                                age_m = age_delta.months + age_y*12                                

                                if (table['period'] == 'day' and table['keep'] < age_d)\
                                        or (table['period'] == 'week' and table['keep'] < age_w)\
                                        or (table['period'] == 'month' and table['keep'] < age_m):

                                    fl = flush_partition(db_connection, tablename, part[1])

                                    if fl:
                                        logging.info('Flush partitions for table: {} was successfull'.format(tablename))
                                    else:
                                        logging.warning('Flush partitions for table: {} was failed'.format(tablename))

                        else:
                            logging.info('Table: {} partition: {} is already exist'.format(tablename, next_part_name))


                    else:
                        logging.warning('Table: {} might not be partitioned!'.format(tablename))

            else:
                logging.warning('DB schema {} doesn\'t partitioned yet!'.format(db_schema))

        else:
            logging.error('DBMS doesn\'t supports the partitioning technology!')

        db_connection.close()
        remove_lock(lockfile)

        logging.info('Mutex: {}, successfully released!'.format(lockfile))
        logging.info('Zabbix database partitioning task ended')
    else:
        logging.error('Could not obtain a mutex: {}'.format(lockfile))
