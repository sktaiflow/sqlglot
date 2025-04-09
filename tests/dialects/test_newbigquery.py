from tests.dialects.test_dialect import Validator
from sqlglot.optimizer.annotate_types import annotate_types


class TestNewBigQuery(Validator):
    dialect = "aidpbigquery"

    def test_load(self):
        # from my import newbigquery  # noqa: F401
        from my import aidpbigquery  # noqa: F401

        sql = """
        LOAD DATA INTO mydataset.table1 FROM FILES( format='CSV', uris = ['gs://bucket/path/file.csv'])
        """
        statement = self.parse_one(sql)
        import sqlglot

        statement = sqlglot.optimizer.qualify.qualify(
            statement,
            dialect="aidpbigquery",
            qualify_columns=False,
            validate_qualify_columns=False,
            allow_partial_qualification=False,
            identify=False,
        )
        find_result = statement.find_all(sqlglot.exp.LoadData)
        for expr in find_result:
            self.assertEqual(expr.this.db, "mydataset")
            self.assertEqual(expr.this.this.this, "table1")
