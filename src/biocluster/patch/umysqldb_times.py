import umysqldb
from umysqldb.times import mysql_timestamp_converter
import types


def patch_mysql_timestamp_converter(s):
    """Convert a MySQL TIMESTAMP to a Timestamp object."""
    # MySQL>4.1 returns TIMESTAMP in the same format as DATETIME
    if isinstance(s, types.StringTypes):
        mysql_timestamp_converter(s)
    else:
        return s


umysqldb.times.mysql_timestamp_converter = patch_mysql_timestamp_converter
