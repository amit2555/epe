#!/usr/bin/env python

import mysql.connector
import traceback
import logging


logger = logging.getLogger(__name__)

DB_HOST = 'localhost'
DB_NAME = 'sdn'
DB_USER = 'root'
DB_PASS = 'root'

SCHEMA = ('''
    CREATE TABLE IF NOT EXISTS events (
        id              BIGINT(64) NOT NULL AUTO_INCREMENT,
        timestamp       BIGINT(64) NOT NULL,
        device          VARCHAR(128) NOT NULL,
        interface       VARCHAR(128) NOT NULL,
        status          SMALLINT(8) NOT NULL DEFAULT 0,
        result          SMALLINT(8) NOT NULL DEFAULT 0,
        PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1
''')


class Event:
    """ Represents data for an event or fault that is stored into a database and
        acted upon.
    """
    def _reverse_dict(mapping):
        temp = {}
        for name, value in mapping.items():
            temp[value] = name
        return temp

    STATUS_CODES = {
        'QUEUED': 1,
        'ACTIVE': 2,
        'PROCESSED': 3}
    STATUS_NAMES = _reverse_dict(STATUS_CODES)

    RESULT_CODES = {
        'UNKNOWN': 1,
        'SUCCESS': 2,
        'FAILURE': 3,
        'EXCEPTION': 4}
    RESULT_NAMES = _reverse_dict(RESULT_CODES)


    def __init__(self, timestamp, device, interface, status=None, result=None, event_id=0): 
        """ 
        :param timestamp:       a datetime formatted string
        :param device:          a device name
        :param interface:       an interface with high utilization 
        :param status:          a value of self.STATUS_CODES which is
                                used to distiguish new events that need
                                to be acted upon vs past ones
        :param result:          a value of self.RESULT_CODES which
                                represents the status of this event
                                after attempting remediation
        :param event_id:        an auto-incrementing id for new events
        """
        self._validate_params(status, result)
        self.timestamp = int(timestamp)
        self.device = device
        self.interface = interface
        if not status:
            status = self.STATUS_CODES['QUEUED']
        self.status = status
        if not result:
            result = self.RESULT_CODES['UNKNOWN']
        self.result = result
        self.event_id = event_id


    def _validate_params(self, status, result):
        if status:
            assert status in self.STATUS_CODES.values(), (
                'Invalid status code of {} passed into Event()'.format(status))
        if result:
            assert result in self.RESULT_CODES.values(), (
                'Invalid result code of {} passed into Event()'.format(result))


    def __str__(self):
        event_details = (
            'status={}, timestamp={}, device={}, interface={}, result={}'.format( 
            self.STATUS_NAMES[self.status], self.timestamp, self.device,
            self.interface, self.RESULT_NAMES[self.result])) 
        if self.event_id:
            event_details = (
                'event_id={}, {}'.format(self.event_id, event_details))
        return event_details


class Db(object):
    """ Manages database interactions for connectivity to a MySQL-based backend

    def __init__(self, db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_name=DB_NAME):
        """
        :param db_user:     database username
        :param db_pass:     database password
        :param db_host:     an IP address or dns name
        :param db_name:     database name
        """
    
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_name = db_name
        self.session = None


    def __enter__(self):
        """ Handles automatic opening of a connection to the database backend
            when instantiated using the "with" statement.
        """
        self.open_session()
        return self


    def __exit__(self, ex_type, ex_value, traceback):
        """ Handles automatic closing of a connection to the database backend.
        """
        self.close_session()


    def __str__(self):
        return (
            'Db:  db_host={}, db_name={}'.format(self.db_host, self.db_name))


    def open_session(self):
        """ Opens a database connection and saves that connection object onto self.session."""
        if not self.session:
            self.session = mysql.connector.connect(
                user=self.db_user, password=self.db_pass,
                host=self.db_host, database=self.db_name)
            if self.session:
                logger.debug('Connected to DB {}'.format(self.db_name))
            else:
                logger.debug('Failed to connect to DB {}'.format(self.db_name))


    def close_session(self):
        """ Gracefully terminates a connection to the database backend. """
        self.session.close()
        logger.debug('DB connection closed')


    def create_database(self, db_name=None):
        """ Creates a database by the name provided if one does not already
            exist by the same name.
        """
        if not db_name:
            db_name = self.db_name
        sql = 'CREATE DATABASE IF NOT EXISTS {}'.format(db_name)
        cursor = self.session.cursor()
        cursor.execute(sql)
        self.session.commit()
        logger.debug('Database {} created'.format(db_name))


    def create_schema(self, schema=SCHEMA):
        """ Creates a database schema. """
        cursor = self.session.cursor()
        cursor.execute(schema)


    def insert_event(self, event):
        """ Creates a new event based on the parameters provided.

            :param event:         an Event object
            :returns event_id:    the unique database id for the event
        """
        params = (event.timestamp, event.device, event.interface, event.status, event.result)
        if not all(params):
            raise Exception(
                'Unable to save event - one or more required values were '
                'missing from:  {}'.format(event))
        sql = ('''
            INSERT INTO events (
                timestamp, device, interface, status, result)
            VALUES (
                %s, %s, %s, %s, %s, %s)
        ''')
        cursor = self.session.cursor()
        cursor.execute(sql, params)
        event_id = cursor.lastrowid
        self.session.commit()
        return event_id


    def get_event(self, event_id):
        """ Gets an event by its unique id."""
        sql = ('''
            SELECT timestamp, device, interface, status, result, id
            FROM events
            WHERE id=%s
        ''')
        cursor = self.session.cursor(named_tuple=True)
        cursor.execute(sql, (int(event_id),))
        row = cursor.fetchone()  # Returns a tuple
        event = Event(*row)
        return event


    def get_event_id(self, event):
        """ Gets an event by it's attributes.  This is primarily used to check
            for duplicates prior to creating a new entry with insert_event().
        """
        params = (event.device, event.interface, event.status, event.result)
        sql = ('''
            SELECT id
            FROM events
            WHERE (
                device="%s" AND interface="%s" AND status=%s AND result=%s)
        ''')
        cursor = self.session.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()  # Returns a tuple
        if row:
            return row[0]
        return 0


    def get_events_by_status(self, status, limit=1000):
        """ Gets events by status up to the limit specified. """
        sql = ('''
            SELECT timestamp, device, interface, status, result, id
            FROM events
            WHERE status = %s
            LIMIT %s
        ''')
        cursor = self.session.cursor(named_tuple=True)
        cursor.execute(sql, (status, limit))
        rows = cursor.fetchall()  # Returns a list of named tuples

        events = []
        for row in rows:
            event = Event(*row)
            events.append(event)
        return events


    def update_status(self, event_id, status):
        """ Updates an event's status.

            :param event_id:    a unique event ID
            :param status:      a value of Event.STATUS_CODES
            :returns None:
        """
        sql = ('''
            UPDATE events
            SET status=%s
            WHERE id=%s
        ''')
        cursor = self.session.cursor()
        cursor.execute(sql, (status, event_id))
        self.session.commit()


    def update_result(self, event_id, result):
        """ Updates an event's result.

            :param event_id:    a unique event ID
            :param status:      a value of Event.RESULT_CODES
            :returns None:
        """
        sql = ('''
            UPDATE events
            SET result=%s
            WHERE id=%s
        ''')
        cursor = self.session.cursor()
        cursor.execute(sql, (result, event_id))
        self.session.commit()


if __name__ == '__main__':
    import time
    print('Running DB connection tests...')

    with Db() as db:
        # Create the database
        try:
            db.create_database()
        except Exception:
            print('Failed to create database')
            raise
        else:
            print('Database exists or created successfully')

        # Build the tables
        try:
            db.create_schema()
        except Exception:
            print('Failed to create schema')
            raise
        else:
            print('Schema exists or created successfully')

        # Insert an event
        event = Event(
            timestamp=time.time(),
            status=Event.STATUS_CODES['QUEUED'],
            device='rtr01.example.com',
            error_code='IF_DOWN',
            error_message='Interface Ethernet1/1 is Down',
            result=Event.RESULT_CODES['UNKNOWN'])
        try:
            event_id = db.insert_event(event)
        except Exception:
            print('Failed to insert event')
            raise
        else:
            print('Created event #{}'.format(event_id))
            event = db.get_event(event_id)
            print('    {}'.format(event))

        # Update an event result
        try:
            db.update_result(event_id, Event.RESULT_CODES['SUCCESS'])
        except Exception:
            print('Failed to update an event result')
            raise
        else:
            print('Successfully updated an event result')

        # Update an event status
        try:
            db.update_status(event_id, Event.STATUS_CODES['PROCESSED'])
        except Exception:
            print('Failed to update an event status')
            raise
        else:
            print('Successfully updated an event status')

        # Fetch the last event
        try:
            event = db.get_event(event_id)
        except Exception:
            print('Failed to fetch an event')
            raise
        else:
            print('Successfully fetched one event:\n'
                  '    {}'.format(event))

        # Fetch many events
        try:
            events = db.get_events_by_status(
                status=Event.STATUS_CODES['PROCESSED'],
                limit=5)
        except Exception:
            print('Failed to fetch multiple events')
            raise
        else:
            print('Successfully fetched multiple events:')
            for event in events:
                print('    {}'.format(event))

