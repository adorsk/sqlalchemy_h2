__version__ = '0.1'

from sqlalchemy.dialects import registry

registry.register("h2", "sqlalchemy_h2.dialect", "H2Dialect")
