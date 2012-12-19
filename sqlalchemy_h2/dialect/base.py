# h2.py
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the H2 database.

For information on connecting using a specific driver, see the documentation
section regarding that driver.

Heavily based on code by sunwei415,
see: http://code.google.com/p/sqlalchemy-jython/

author: adorsk

"""

import re

from sqlalchemy import sql
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.sql import case, cast
from sqlalchemy.sql import compiler


class H2Compiler(compiler.SQLCompiler):
    extract_map = compiler.SQLCompiler.extract_map.copy()

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_sequence(self, seq):
        return "%s.nextval" % self.preparer.format_sequence(seq)

    def for_update_clause(self, select):
        return ''

    def visit_case(self, clause, **kwargs):
        """
        Adjust case clause to use explicit casts for 'THEN' expressions.
        """

        def sanitized_cast(elem):
            """ Cast elements, casting NullType to string. """
            if isinstance(elem.type, sqltypes.NullType):
                return cast(elem, sqltypes.String)
            else:
                return cast(elem, elem.type)

        whens_with_cast = [(when_, sanitized_cast(result))
                           for when_, result in clause.whens]
        else_with_cast = None
        if clause.else_ is not None:
            else_with_cast = sanitized_cast(clause.else_)

        case_with_cast = case(
            whens=whens_with_cast,
            else_=else_with_cast,
            value=clause.value
        )
        return super(H2Compiler, self).visit_case(case_with_cast, **kwargs)

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text += "\n LIMIT " + self.process(sql.literal(select._limit))
        if select._offset is not None:
            if select._limit is None:
                text += "\n LIMIT NULL"
            text += " OFFSET " + self.process(sql.literal(select._offset))
        return text

    def visit_mod(self, binary, **kw):
        return "mod(%s, %s)" % (
            self.process(binary.left),
            self.process(binary.right)
        )


class H2DDLCompiler(compiler.DDLCompiler):

    def get_column_specification(self, column, **kwargs):
        colspec = "%s %s" % (
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(column.type)
        )
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"

        if column.primary_key and column is column.table._autoincrement_column:
            colspec += " AUTO_INCREMENT"

        return colspec


class H2TypeCompiler(compiler.GenericTypeCompiler):
    def visit_null(self, type_):
        return "NULL"


class H2IdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = set([
        'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
        'attach', 'autoincrement', 'before', 'begin', 'between', 'by',
        'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
        'conflict', 'constraint', 'create', 'cross', 'current_date',
        'current_time', 'current_timestamp', 'database', 'default',
        'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
        'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
        'exists', 'explain', 'false', 'fail', 'for', 'foreign', 'from',
        'full', 'glob', 'group', 'having', 'if', 'ignore', 'immediate', 'in',
        'index', 'indexed', 'initially', 'inner', 'insert', 'instead',
        'intersect', 'into', 'is', 'isnull', 'join', 'key', 'left', 'like',
        'limit', 'match', 'minus', 'natural', 'not', 'notnull', 'null', 'of',
        'offset', 'on', 'or', 'order', 'outer', 'plan', 'pragma', 'primary',
        'query', 'raise', 'references', 'reindex', 'rename', 'replace',
        'restrict', 'right', 'rollback', 'row', 'rownum', 'select', 'set',
        'sysdate', 'systime', 'systimestamp', 'table', 'temp', 'temporary',
        'then', 'to', 'today', 'transaction', 'trigger', 'true', 'union',
        'unique', 'update', 'using', 'vacuum', 'values', 'view', 'virtual',
        'when', 'where',
        ])

    def __init__(self, dialect, initial_quote='"',
                 escape_quote='"', **kwargs):
        super(H2IdentifierPreparer, self).__init__(
            dialect, initial_quote=initial_quote,
            escape_quote=escape_quote, **kwargs)

    def _unquote_identifier(self, identifier):
        return identifier.strip('"')


class H2ExecutionContext(default.DefaultExecutionContext):

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            ("select %s.nextval" % (
                self.dialect.identifier_preparer.format_sequence(seq)
            )), type_)


class H2Dialect(default.DefaultDialect):
    name = 'h2'
    supports_alter = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    supports_default_values = True
    supports_empty_insert = False
    supports_cast = True
    supports_native_boolean = True

    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = True

    default_paramstyle = 'qmark'
    statement_compiler = H2Compiler
    ddl_compiler = H2DDLCompiler
    type_compiler = H2TypeCompiler
    preparer = H2IdentifierPreparer

    ischema_names = {}
    for type_name in [
        'BIGINT',
        'BINARY',
        'BLOB',
        'BOOLEAN',
        'CHAR',
        'CLOB',
        'DATE',
        'DATETIME',
        'DECIMAL',
        'INTEGER',
        'SMALLINT',
        'TIME',
        'TIMESTAMP',
        'VARCHAR',
    ]:
        ischema_names[type_name] = getattr(sqltypes, type_name)
    ischema_names['DOUBLE'] = sqltypes.NUMERIC

    colspecs = {}

    requires_name_normalize = True

    def normalize_name(self, name):
        if name is None:
            return None

        if isinstance(name, unicode):
            name = name.encode(self.encoding)

        if name.upper() == name and \
              not self.identifier_preparer._requires_quotes(name.lower()):
            name = name.lower()

        return name

    def denormalize_name(self, name):
        if name is None:
            return None

        if not self.supports_unicode_binds:
            name = name.encode(self.encoding)
        elif not isinstance(name, unicode):
            name = name.decode(self.encoding)

        if name.lower() == name and \
           not self.identifier_preparer._requires_quotes(name.lower()):
            name = name.upper()

        return name

    def _get_bindparams(self, **kwargs):
        bindparams = []
        for k, v in kwargs.items():
            bindparams.append(
                sql.bindparam(
                    k,
                    unicode(self.denormalize_name(v)),
                    type_=sqltypes.Unicode
                )
            )
        return bindparams

    def table_names(self, connection, schema):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        s = sql.text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_type='TABLE' AND table_schema=:schema
            ORDER BY table_name
            """,
            bindparams=self._get_bindparams(schema=schema)
        )

        rs = connection.execute(s)
        return [self.normalize_name(row[0]) for row in rs]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
            s = sql.text(
                """
                SELECT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                """
            )
            rp = connection.execute(s)
            schema_names = [self.normalize_name(row[0]) for row in rp]
            return schema_names

    def _get_default_schema_name(self, connection):
        return self.normalize_name('public')

    def has_table(self, connection, table_name, schema=None):
        if schema is None:
            schema = self._get_default_schema_name(connection)
        s = sql.text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_type='TABLE' AND table_schema=:schema
            AND table_name=:table
            """,
            bindparams=self._get_bindparams(schema=schema, table=table_name)
        )
        rs = connection.execute(s)
        row = rs.fetchone()

        return (row is not None)

    def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        s = sql.text(
            """
            SELECT sequence_name FROM information_schema.sequences
            WHERE sequence_schema=:schema
            AND sequence_name=:sequence
            """,
            bindparams=self._get_bindparams(
                schema=schema,
                sequence=sequence_name
            )
        )
        rs = connection.execute(s)
        row = rs.fetchone()

        return (row is not None)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        return self.table_names(connection, schema)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)
            s = sql.text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
        rs = connection.execute(s)

        return [self.normalize_name(row[0]) for row in rs]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        s = sql.text("SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS")
        rs = connection.execute(s)
        result = rs.fetchall()
        if result:
            return result[0]

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):

        if schema is None:
            schema = self._get_default_schema_name(connection)

        s = sql.text(
            """
            SELECT
            C.COLUMN_NAME,
            C.TYPE_NAME,
            C.COLUMN_DEFAULT,
            C.IS_NULLABLE,
            (Select T.AUTO_INCREMENT
            from INFORMATION_SCHEMA.TYPE_INFO T
            where T.DATA_TYPE = C.DATA_TYPE
            and T.TYPE_NAME=C.TYPE_NAME ) AS AUTO_INCREMENT,
            C.CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS C
            WHERE TABLE_NAME =:table
            AND TABLE_SCHEMA =:schema
            """,
            bindparams=self._get_bindparams(schema=schema, table=table_name),
            typemap={
                'COLUMN_NAME': sqltypes.Unicode,
                'TYPE_NAME': sqltypes.Unicode,
                'COLUMN_DEFAULT': sqltypes.Unicode,
                'IS_NULLABLE': sqltypes.Unicode,
                'AUTO_INCREMENT': sqltypes.BOOLEAN,
                'CHARACTER_MAXIMUM_LENGTH': sqltypes.INTEGER
            }
        )

        c = connection.execute(s)
        rows = c.fetchall()

        # format columns
        columns = []
        for column_name, type_name, default, nullable, autoincrement, charlen\
                in rows:
            args = ()
            kwargs = {}
            if default is not None:
                # @TODO: in the future this should be more rigorous, but
                # just get it working for now.
                match = re.search('NEXT VALUE FOR .*\.SYSTEM_SEQUENCE_.*',
                                  default)
                if match is not None:
                    default = None
                    autoincrement = True
            name = self.normalize_name(column_name)
            nullable = (nullable == 'YES')

            if type_name == 'DOUBLE':
                args = (53, )
            elif type_name in [
                'INT',
                'INTEGER',
                'DATE',
                'TIMESTAMP',
                'CLOB',
            ]:
                args = ()
            elif charlen:
                args = (int(charlen),)

            if type_name in self.ischema_names:
                coltype = self.ischema_names[type_name]
            else:
                coltype = None

            if coltype:
                coltype = coltype(*args, **kwargs)
            else:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (type_name, name))
                coltype = sqltypes.NULLTYPE

            column_info = dict(name=name, type=coltype,
                               nullable=nullable, default=default,
                               autoincrement=autoincrement
                              )
            columns.append(column_info)
        return columns

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        s = sql.text(
            """
            select COLUMN_NAME from
            INFORMATION_SCHEMA.INDEXES
            where PRIMARY_KEY = 'TRUE'
            AND TABLE_NAME =:table
            AND TABLE_SCHEMA =:schema
            """,
            bindparams=self._get_bindparams(schema=schema, table=table_name),
            typemap={'COLUMN_NAME': sqltypes.Unicode}
        )

        c = connection.execute(s)
        primary_keys = [self.normalize_name(r[0]) for r in c.fetchall()]

        return primary_keys

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):

        if schema is None:
            schema = self._get_default_schema_name(connection)

        cols = self.get_primary_keys(connection, table_name,
                                     schema=schema, **kw)

        s = sql.text(
            """
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.CONSTRAINTS
            WHERE TABLE_NAME =:table
            and TABLE_SCHEMA =:schema
            and CONSTRAINT_TYPE = 'PRIMARY_KEY'
            """,
            bindparams=self._get_bindparams(schema=schema, table=table_name),
            typemap={'CONSTRAINT_NAME': sqltypes.Unicode}
        )

        c = connection.execute(s)
        name = self.normalize_name(c.scalar())
        return {
            'constrained_columns': cols,
            'name': name
        }

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):

        default_schema_name = self._get_default_schema_name(connection)
        if schema is None:
            schema = default_schema_name

        preparer = self.identifier_preparer

        s = sql.text(
            """
            SELECT CONSTRAINT_NAME, SQL as condef
            FROM INFORMATION_SCHEMA.CONSTRAINTS
            WHERE TABLE_NAME =:table
            and TABLE_SCHEMA =:schema
            and CONSTRAINT_TYPE = 'REFERENTIAL'
            """,
            bindparams=self._get_bindparams(schema=schema, table=table_name),
            typemap={
                'CONSTRAINT_NAME': sqltypes.Unicode,
                'condef': sqltypes.Unicode}
        )

        c = connection.execute(s)
        fkeys = []
        for conname, condef in c.fetchall():
            m = re.search(
                'FOREIGN KEY\((.*?)\).*?REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)',
                condef).groups()

            raw_constrained_columns = m[0]
            raw_referred_schema = m[1]
            raw_referred_table = m[2]
            raw_referred_columns = m[3]

            def _prepare_name(raw_name):
                return self.normalize_name(
                    preparer._unquote_identifier(
                        preparer._unescape_identifier(raw_name)
                    )
                )

            constrained_columns = []
            for raw_col in re.split(r'\s*,\s*', raw_constrained_columns):
                constrained_columns.append(_prepare_name(raw_col))

            if raw_referred_schema:
                referred_schema = _prepare_name(raw_referred_schema)
            else:
                referred_schema = schema
            if referred_schema == default_schema_name:
                referred_schema = None

            referred_table = _prepare_name(raw_referred_table)

            referred_columns = []
            for raw_col in re.split(r'\s*,\s', raw_referred_columns):
                referred_columns.append(_prepare_name(raw_col))

            fkey_d = {
                'name': conname,
                'constrained_columns': constrained_columns,
                'referred_schema': referred_schema,
                'referred_table': referred_table,
                'referred_columns': referred_columns
            }
            fkeys.append(fkey_d)
        return fkeys

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):

        if schema is None:
            schema = self._get_default_schema_name(connection)

        include_auto_indexes = kw.pop('include_auto_indexes', False)

        s = sql.text(
            """
            SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, INDEX_TYPE_NAME
            FROM INFORMATION_SCHEMA.INDEXES
            WHERE TABLE_NAME= :table
            AND TABLE_SCHEMA = :schema
            """,
            bindparams=self._get_bindparams(schema=schema, table=table_name),
            typemap={
                'INDEX_NAME': sqltypes.Unicode,
                'NON_UNIQUE': sqltypes.BOOLEAN,
                'COLUMN_NAME': sqltypes.Unicode,
                'INDEX_TYPE_NAME': sqltypes.Unicode,
            }
        )

        c = connection.execute(s)
        index_names = {}
        indexes = []
        for row in c.fetchall():
            idx_name, unique, col, idx_type = row
            if not include_auto_indexes:
                idx_type = idx_type.encode(self.encoding)
                if idx_type == 'PRIMARY KEY':
                    continue
            col = self.normalize_name(col)
            idx_name = self.normalize_name(idx_name)
            if idx_name in index_names:
                index_d = index_names[idx_name]
            else:
                index_d = {'column_names': []}
                indexes.append(index_d)
                index_names[idx_name] = index_d

            index_d['name'] = idx_name
            index_d['column_names'].append(col)
            index_d['unique'] = not unique
        return indexes

    def do_begin_twophase(self, connection, xid):
        self.do_begin(connection.connection)

    def do_prepare_twophase(self, connection, xid):
        connection.execute("PREPARE COMMIT %s" % xid)

    def do_rollback_twophase(self, connection, xid,
                             is_prepared=True, recover=False):
        if is_prepared:
            connection.execute("ROLLBACK TRANSACTION %s" % xid)
            self.do_rollback(connection.connection)
        else:
            self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid,
                                is_prepared=True, recover=False):
        if is_prepared:
            connection.execute("COMMIT TRANSACTION %s" % xid)
            self.do_rollback(connection.connection)
        else:
            self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        resultset = connection.execute(
            sql.text("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT"))
        return [row[0] for row in resultset]
