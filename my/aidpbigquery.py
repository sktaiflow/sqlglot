from sqlglot import TokenType, exp
from sqlglot.dialects.bigquery import BigQuery


class AidpBigQuery(BigQuery):

    class Parser(BigQuery.Parser):

        # STATEMENT_PARSERS = {
        #     **BigQuery.Parser.STATEMENT_PARSERS,
        #     TokenType.LOAD: lambda self: self._parse_load(),
        # }

        def _parse_load(self):
            self._match_text_seq("DATA")
            overwrite = self._match(TokenType.OVERWRITE)
            if not overwrite:
                i = self._match_text_seq("INTO")

            temp = self._match_set({"TEMP", TokenType.TEMPORARY})
            if temp:
                self._match(TokenType.TABLE)

            table = self._parse_table()

            columns = None
            if self._match(TokenType.L_PAREN):
                columns = self._parse_csv(self._parse_identifier)
                self._match(TokenType.R_PAREN)

            partition_by = None
            cluster_by = None
            if self._match(TokenType.PARTITION):
                if self._match_text_seq("BY"):
                    partition_by = self._parse_expression()

            if self._match_text_seq("CLUSTER"):
                self._match_text_seq("BY")
                cluster_by = self._parse_csv(self._parse_identifier)

            options = None
            if self._match_text_seq("OPTIONS"):
                self._match(TokenType.L_PAREN)
                options = self._parse_csv(self._parse_expression)
                self._match(TokenType.R_PAREN)

            fr = self._match_text_seq("FROM")
            fff = self._match_text_seq("FILES")
            self._match(TokenType.L_PAREN)
            files = self._parse_csv(self._parse_expression)
            self._match(TokenType.R_PAREN)

            with_partition_columns = None
            if self._match(TokenType.WITH):
                if self._match(TokenType.PARTITION):
                    self._match_text_seq("COLUMNS")
                    if self._match(TokenType.L_PAREN):
                        with_partition_columns = self._parse_csv(self._parse_identifier)
                        self._match(TokenType.R_PAREN)

            connection = None
            if self._match(TokenType.WITH):
                self._match_text_seq("CONNECTION")
                connection = self._parse_identifier()

            print("LOAD")

            return self.expression(
                exp.LoadData,
                this=table,
                overwrite=overwrite,
            )
