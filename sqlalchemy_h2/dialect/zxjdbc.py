"""Support for the H2 database via Jython's zxjdbc JDBC connector.

JDBC Driver
-----------

The official H2 database and JDBC driver is at
http://http://www.h2database.com/.

Character Sets
--------------

SQLAlchemy zxjdbc dialects pass unicode straight through to the
zxjdbc/JDBC layer.

"""
import re

from sqlalchemy import pool
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy_h2.dialect.base import H2Dialect, H2ExecutionContext


class H2ExecutionContext_zxjdbc(H2ExecutionContext):
    def get_lastrowid(self):
        cursor = self.create_cursor()
        cursor.execute("SELECT LAST_INSERT_ID()")
        lastrowid = cursor.fetchone()[0]
        cursor.close()
        if isinstance(lastrowid, long):
            lastrowid = int(lastrowid)
        return lastrowid


class H2_zxjdbc(ZxJDBCConnector, H2Dialect):
    jdbc_db_name = 'h2'
    jdbc_driver_name = 'org.h2.Driver'

    execution_ctx_cls = H2ExecutionContext_zxjdbc

    def _create_jdbc_url(self, url):
        """Create a JDBC url from a :class:`~sqlalchemy.engine.url.URL`"""
        return 'jdbc:%s:%s;MODE=PostgreSQL' % (self.jdbc_db_name, url.database)

    def _driver_kwargs(self):
        """return kw arg dict to be sent to connect()."""
        return {}
        #return dict(characterEncoding='UTF-8', yearIsDateType='false')

    def _extract_error_code(self, exception):
        # e.g.: DBAPIError: (Error) Table 'test.u2' doesn't exist
        # [SQLCode: 1146], [SQLState: 42S02] 'DESCRIBE `u2`' ()
        m = re.compile(r"\[SQLCode\: (\d+)\]").search(str(exception.orig.args))
        c = m.group(1)
        if c:
            return int(c)

    @classmethod
    def get_pool_class(cls, url):
        if url.database and not url.database.startswith('mem:'):
            return pool.NullPool
        else:
            return pool.SingletonThreadPool

dialect = H2_zxjdbc
