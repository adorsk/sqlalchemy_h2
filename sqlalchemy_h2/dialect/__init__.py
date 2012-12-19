from sqlalchemy.dialects.h2 import base, zxjdbc


# default dialect
base.dialect = zxjdbc.dialect

#from sqlalchemy.dialects.h2.base import \
#    INTEGER, BIGINT, SMALLINT, VARCHAR, CHAR, TEXT, NUMERIC, FLOAT, REAL, INET, \
#    CIDR, UUID, BIT, MACADDR, DOUBLE_PRECISION, TIMESTAMP, TIME,\
#    DATE, BYTEA, BOOLEAN, INTERVAL, ARRAY, ENUM, dialect
#
#__all__ = (
#'INTEGER', 'BIGINT', 'SMALLINT', 'VARCHAR', 'CHAR', 'TEXT', 'NUMERIC', 'FLOAT', 'REAL', 'INET',
#'CIDR', 'UUID', 'BIT', 'MACADDR', 'DOUBLE_PRECISION', 'TIMESTAMP', 'TIME',
#'DATE', 'BYTEA', 'BOOLEAN', 'INTERVAL', 'ARRAY', 'ENUM', 'dialect'
#)