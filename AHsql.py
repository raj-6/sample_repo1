#==============================================================================
# # Pull in necessary libs
#==============================================================================
"""
Author:Earnin
Document containing functions for database connection and manipulation
"""

from os import getenv
from sqlalchemy import create_engine
import pymysql
import pandas.io.sql as sql

class AHsql(object):
    """
    Parent Class for all methods
    """
    def __init__(self, server='replica'):
        """
        Database initialization
        """
        server_flavor = {'syw':'my', 'replica':'ms', 'clientstate':'my', 'production':'ms'}
        if server == 'syw':
            connection_parameters = self.get_connection_parameters_syw()
        elif server == 'replica':
            connection_parameters = self.get_connection_parameters()
#==============================================================================
#             # Not generalized at all, but I think we're getting rid of MS SQL soon anyway
#==============================================================================
            write_connection_parameters = self.get_write_connection_parameters()
            write_connection_string = self.get_engine_string_ms(**write_connection_parameters)
            write_engine = create_engine(write_connection_string)
            self.write_connection = write_engine.connect()
#==============================================================================
#         # elif server == 'production':
#         #     connection_parameters = self.get_connection_parameters_prod()
#==============================================================================
        elif server == 'production':
            connection_parameters = self.get_connection_parameters_prod()
        elif server == 'clientstate':
            connection_parameters = self.get_conn_pmtr_client_state()
        else:
            raise ValueError('Sorry, invalid server name.')
        if server_flavor[server] == 'ms':
            connection_string = self.get_engine_string_ms(**connection_parameters)
            engine = create_engine(connection_string)
            self.connection = engine.connect()
        else:
#==============================================================================
#             # connection_string = self.get_engine_string_my(**connection_parameters)
#==============================================================================
            self.connection = pymysql.connect(**connection_parameters)
#==============================================================================
#         #TODO: Move mysql connection over to using SQL alchemy
#==============================================================================
    # pylint: disable=R0201
    def disconnect(self):
        """
        DB disconnect
        """
        self.connection.close()
#==============================================================================
#         # Only create write connection for some connections above
#==============================================================================
        try:
            self.write_connection.close()
        except pymysql.Error:
            pass

    def get_engine_string_ms(self, server, port, user, password, database):
        """
        Get connection string MS SQL
        """
        #server = k[0]
        #port = k[1]
        #user = k[2]
        #password = k[3]
        #database = k[4]
        engine_string = ('mssql+pymssql://' + user + ':'
                         + password + '@' + server + ':'
                         + str(port) + '/' + database)
        return engine_string
    # pylint: disable=R0201
    def get_connection_parameters(self):
        """
        Get connection parameters
        """
        connection_parameters = {
            'server': getenv("SERVER"),
            'port': 1433,
            'user': getenv("USER_NAME"),
            'password': getenv("PASSWORD"),
            'database': 'AH'
        }
        return connection_parameters
    # pylint: disable=R0201
    def get_write_connection_parameters(self):
        """
        Get write connection parameters
        """
        connection_parameters = {
            'server':getenv("SERVER"),
            'port':1433,
            'user': getenv("USER_NAME"),
            'password': getenv("PASSWORD"),
            'database': 'AH_Analysis'
        }
        return connection_parameters

    def get_connection_parameters_prod(self):
        """
        Get connection parameters Prod
        """
        connection_parameters = {
            'server': getenv("SERVER_PROD"),
            'port': 1433,
            'user': getenv("USER_NAME_PROD"),
            'password': getenv("PASSWORD_PROD"),
            'database': 'AH'
        }
        return connection_parameters

    def get_connection_parameters_syw(self):
        """
        Get connection parameters SYW
        """
        connection_parameters = {
            #'host': getenv("SERVER_SYW"),
            'user': getenv("USER_NAME_SYW"),
            'password': getenv("PASSWORD_SYW"),
            #'db': getenv("DB_SYW"),
            #'charset': 'utf8mb4',
            #'cursorclass': pymysql.cursors.DictCursor
        }
        return connection_parameters

    def get_conn_pmtr_client_state(self):
        """
        Get connection parameters client state
        """
        connection_parameters = {
            'host': getenv("SERVER_CLIENT"),
            'user': getenv("USER_NAME_CLIENT"),
            'password': getenv("PASSWORD_CLIENT"),
            'db': getenv("DB_CLIENT"),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        return connection_parameters

    def sql_data_from_text(self, query):
        """
        Query to read data
        """
        data = sql.read_sql(query, self.connection)
        return data

    def execute_to_write(self, query):
        """
        Query to write data
        """
        try:
            self.write_connection.execute(query)
        except pymysql.Error:
            return False
        return True

    def write_data_frame_to_sql(self, dataframe, table_name, if_exists='fail'):
        """
        Write from dataframe to DB
        TODO: make this error if it's not pointed at replica, or more generally
        somewhere we feel comfortable writing
        """
        dataframe.to_sql(table_name, self.write_connection, schema='Analysis',
                         chunksize=10000, if_exists=if_exists, index=False)
#==============================================================================
# flavor=None,
#TODO: generalize this to any bulk write situation and change in
#GeneralAssignmentImporter script
#Need to have a stored procedure already set up in order to use this
#def bulkWriteTestAssignmentDataFrameToSql(self, dataframe):
#  data =
#  chunks = [data[i:i + 100000] for i in range(0, len(data), 100000)]
#  with pytds.connect(getenv("SERVER"), 'AH_Analysis',
            #getenv("USER_NAME"), getenv("PASSWORD")) as conn:
#     #         with conn.cursor() as cur:
#     #             for chunk in chunks:
#     #  tvp = pytds.TableValuedParam(type_name='Analysis.TestAssignmentsType',
                                         #rows=chunk)
#     #       cur.execute('exec Analysis.ImportTestAssignmentsData %s', (tvp,))
#     #                 conn.commit()
#     #             cur.close()
#     #             conn.close()
# # #TODO: don't use the following hack. _canonical_string_
#encodings is protected.
#  # datashape.coretypes._canonical_string_encodings.update
#({"SQL_Latin1_General_CP1_CI_AS": "U8"})
#       # write_connection_parameters = self.get_write_connection_parameters()
#       # uri = self.get_engine_string_ms(**write_connection_parameters)+ '::'
#+ table_name
#         # odo(dataframe, uri)
#==============================================================================

