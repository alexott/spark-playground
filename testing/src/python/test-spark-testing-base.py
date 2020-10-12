import unittest2
from sparktestingbase.sqltestcase import SQLTestCase

from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType

class SimpleSQLTest(SQLTestCase):
    """A simple test."""

    def test_empty_expected_equal(self):
        allTypes = self.sc.parallelize([])
        df = self.sqlCtx.createDataFrame(allTypes, StructType([]))
        self.assertDataFrameEqual(df, df)

    def test_simple_expected_equal(self):
        allTypes = self.sc.parallelize([Row(
            i=1, s="string", d=1.0, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        df = allTypes.toDF()
        self.assertDataFrameEqual(df, df)

    @unittest2.expectedFailure
    def test_dif_schemas_unequal(self):
        allTypes1 = self.sc.parallelize([Row(d=1.0)])
        allTypes2 = self.sc.parallelize([Row(d="1.0")])
        self.assertDataFrameEqual(allTypes1.toDF(), allTypes2.toDF(), 0.0001)

if __name__ == "__main__":
    unittest2.main()
