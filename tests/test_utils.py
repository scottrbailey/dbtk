import pytest

from dbtk.utils import process_sql_parameters, ParamStyle


class TestProcessSqlParametersLiterals:
    """String literals and comments must not be scanned for bind placeholders."""

    def test_oracle_format_mask_colon_ignored(self):
        sql = ("SELECT to_char(act.start_date, 'FMHH:FMMIAM') AS start_time "
               "FROM activities act WHERE act.id = :act_id")
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('act_id',)
        assert "'FMHH:FMMIAM'" in result_sql

    def test_format_mask_colon_ignored_pyformat_target(self):
        sql = ("SELECT to_char(act.start_date, 'FMHH:FMMIAM') AS start_time "
               "FROM activities act WHERE act.id = :act_id")
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.PYFORMAT)
        assert param_names == ('act_id',)
        assert result_sql == (
            "SELECT to_char(act.start_date, 'FMHH:FMMIAM') AS start_time "
            "FROM activities act WHERE act.id = %(act_id)s"
        )

    def test_colon_in_line_comment_ignored(self):
        sql = "SELECT x FROM t -- filter by :not_a_param\nWHERE id = :real_id"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('real_id',)

    def test_colon_in_block_comment_ignored(self):
        sql = "SELECT x FROM t /* uses :also_not_a_param here */ WHERE id = :real_id"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('real_id',)

    def test_escaped_quote_in_literal(self):
        sql = "SELECT 'it''s :fake' AS c FROM t WHERE id = :real_id"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('real_id',)
        assert "'it''s :fake'" in result_sql

    def test_no_params_in_literal_only_query(self):
        sql = "SELECT to_char(sysdate, 'FMHH:FMMIAM') FROM dual"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ()
        assert result_sql == sql

    def test_qmark_and_numeric_targets_skip_literals(self):
        sql = "SELECT to_char(d, 'HH:MI') FROM t WHERE a = :x AND b = :y"
        qmark_sql, names = process_sql_parameters(sql, ParamStyle.QMARK)
        assert names == ('x', 'y')
        assert qmark_sql == "SELECT to_char(d, 'HH:MI') FROM t WHERE a = ? AND b = ?"

        numeric_sql, names = process_sql_parameters(sql, ParamStyle.NUMERIC)
        assert names == ('x', 'y')
        assert numeric_sql == "SELECT to_char(d, 'HH:MI') FROM t WHERE a = :1 AND b = :2"

    def test_space_before_colon_inside_literal_still_ignored(self):
        # A colon preceded by whitespace *inside* a literal is only caught by
        # literal stripping - the word-boundary guard alone wouldn't exclude it.
        sql = "SELECT to_char(d, 'Day, FMMonth DD, YYYY :FMMIAM') FROM t WHERE id = :real_id"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('real_id',)


class TestProcessSqlParametersWordBoundary:
    """Colons glued to a preceding identifier/digit are not bind placeholders,
    but unspaced binds after punctuation (=, (, ,) still are."""

    def test_format_mask_colon_glued_to_letter_ignored(self):
        # Belt-and-suspenders check on the raw pattern behavior, independent
        # of literal stripping: a colon directly preceded by a word character
        # is never treated as a placeholder.
        sql = "SELECT x FROM t WHERE a = :real_id AND b = H:FMMIAM"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('real_id',)
        assert 'H:FMMIAM' in result_sql

    def test_unspaced_bind_after_equals_still_detected(self):
        sql = "SELECT * FROM users WHERE id=:id"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('id',)

    def test_unspaced_binds_in_values_list_still_detected(self):
        sql = "INSERT INTO t (a, b) VALUES(:a,:b)"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('a', 'b')

    def test_leading_bind_still_detected(self):
        sql = ":leading_param FROM dual"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('leading_param',)

    def test_postgres_cast_still_excluded(self):
        sql = "SELECT col::text FROM t WHERE id = :id"
        result_sql, param_names = process_sql_parameters(sql, ParamStyle.NAMED)
        assert param_names == ('id',)
        assert 'col::text' in result_sql
