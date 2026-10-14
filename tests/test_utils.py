import os
import time
import pytest

from dbtk.utils import process_sql_parameters, ParamStyle, expire_files


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


def _age_file(path, days):
    """Backdate a file's mtime/atime by `days` (plus a small buffer past midnight)."""
    aged = time.time() - (days * 86400 + 3600)
    os.utime(path, (aged, aged))


class TestExpireFiles:
    """Tests for expire_files() - the shared delete-or-archive-by-age utility."""

    def test_deletes_files_older_than_cutoff(self, tmp_path):
        old_file = tmp_path / 'old.txt'
        new_file = tmp_path / 'new.txt'
        old_file.write_text('old')
        new_file.write_text('new')
        _age_file(old_file, days=10)

        result = expire_files(str(tmp_path), days_old=5)

        assert result == [str(old_file)]
        assert not old_file.exists()
        assert new_file.exists()

    def test_archives_instead_of_deleting_when_archive_dir_given(self, tmp_path):
        src = tmp_path / 'src'
        archive = tmp_path / 'archive'
        src.mkdir()
        old_file = src / 'old.txt'
        old_file.write_text('old')
        _age_file(old_file, days=10)

        result = expire_files(str(src), days_old=5, archive_dir=str(archive))

        assert result == [str(old_file)]
        assert not old_file.exists()
        assert (archive / 'old.txt').exists()
        assert (archive / 'old.txt').read_text() == 'old'

    def test_creates_archive_dir_if_missing(self, tmp_path):
        src = tmp_path / 'src'
        archive = tmp_path / 'does_not_exist_yet' / 'archive'
        src.mkdir()
        old_file = src / 'old.txt'
        old_file.write_text('old')
        _age_file(old_file, days=10)

        expire_files(str(src), days_old=5, archive_dir=str(archive))

        assert (archive / 'old.txt').exists()

    def test_pattern_filters_which_files_are_affected(self, tmp_path):
        old_txt = tmp_path / 'old.txt'
        old_log = tmp_path / 'old.log'
        old_txt.write_text('x')
        old_log.write_text('x')
        _age_file(old_txt, days=10)
        _age_file(old_log, days=10)

        result = expire_files(str(tmp_path), days_old=5, pattern='*.log')

        assert result == [str(old_log)]
        assert old_txt.exists()
        assert not old_log.exists()

    def test_dry_run_reports_without_acting(self, tmp_path):
        old_file = tmp_path / 'old.txt'
        old_file.write_text('old')
        _age_file(old_file, days=10)

        result = expire_files(str(tmp_path), days_old=5, dry_run=True)

        assert result == [str(old_file)]
        assert old_file.exists()

    def test_collision_on_move_is_skipped(self, tmp_path):
        src = tmp_path / 'src'
        archive = tmp_path / 'archive'
        src.mkdir()
        archive.mkdir()
        old_file = src / 'old.txt'
        old_file.write_text('source version')
        _age_file(old_file, days=10)
        (archive / 'old.txt').write_text('already archived')

        result = expire_files(str(src), days_old=5, archive_dir=str(archive))

        assert result == []
        assert old_file.exists()
        assert old_file.read_text() == 'source version'
        assert (archive / 'old.txt').read_text() == 'already archived'

    def test_subdirectories_are_not_touched(self, tmp_path):
        subdir = tmp_path / 'old_subdir'
        subdir.mkdir()
        _age_file(subdir, days=10)

        result = expire_files(str(tmp_path), days_old=5)

        assert result == []
        assert subdir.exists()

    def test_nonexistent_src_dir_returns_empty(self, tmp_path):
        assert expire_files(str(tmp_path / 'nope'), days_old=5) == []

    def test_files_within_retention_are_left_alone(self, tmp_path):
        recent_file = tmp_path / 'recent.txt'
        recent_file.write_text('recent')
        _age_file(recent_file, days=2)

        result = expire_files(str(tmp_path), days_old=5)

        assert result == []
        assert recent_file.exists()
