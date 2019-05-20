from pathlib import Path

from tshistory.schema import sqlfile, tsschema


SCHEMAFILE = Path(__file__).parent / 'schema.sql'


class alias_schema(tsschema):

    def create(self, engine):
        super().create(engine)
        with engine.begin() as cn:
            cn.execute(sqlfile(SCHEMAFILE, ns=self.namespace))

