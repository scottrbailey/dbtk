# tests/test_table.py
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from dbtk.etl.table import Table, validate_identifier
from dbtk.database import ParamStyle


@pytest.fixture
def test_config_file():
    """Path to test config file."""
    return Path(__file__).parent / 'test.yml'


@pytest.fixture
def mock_cursor():
    """Mock cursor for testing ETL operations."""
    cursor = MagicMock()
    cursor.connection.interface._paramstyle = ParamStyle.NAMED
    cursor.connection.interface.DatabaseError = Exception
    return cursor


@pytest.fixture
def airbender_table():
    """Table configuration for Air Nomad training records."""
    col_def = {
        'nomad_id': {'field': 'trainee_id', 'primary_key': True},
        'name': {'field': 'monk_name', 'required': True},
        'temple': {'field': 'home_temple', 'nullable': False},
        'airbending_level': {'field': 'mastery_rank', 'fn': lambda x: int(x) if x else 0},
        'sky_bison': {'field': 'bison_companion'},
        'meditation_score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0},
        'training_date': {'db_fn': 'CURRENT_TIMESTAMP'},
        'instructor': {'value': 'Monk Gyatso', 'no_update': True}}
    return Table('air_nomad_training',
                 columns=col_def,
                 paramstyle=ParamStyle.NAMED)


@pytest.fixture
def fire_nation_table():
    """Table configuration for Fire Nation army records."""
    col_def = {
        'nomad_id': {'field': 'trainee_id', 'primary_key': True},
        'name': {'field': 'monk_name', 'required': True},
        'temple': {'field': 'home_temple', 'nullable': False},
        'airbending_level': {'field': 'mastery_rank', 'fn': lambda x: int(x) if x else 0},
        'sky_bison': {'field': 'bison_companion'},
        'meditation score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0},
        'training_date': {'db_fn': 'CURRENT_TIMESTAMP'},
        'instructor': {'value': 'Monk Gyatso', 'no_update': True}}
    return Table('fire_nation_army',
        columns=col_def,
        paramstyle=ParamStyle.FORMAT)


class TestValidateIdentifier:
    """Test identifier validation for table and column names."""

    def test_valid_simple_identifier(self):
        """Test valid simple identifiers like airbender temple names."""
        assert validate_identifier('eastern_air_temple') == 'eastern_air_temple'
        assert validate_identifier('fire_nation') == 'fire_nation'
        assert validate_identifier('ba_sing_se') == 'ba_sing_se'

    def test_valid_dotted_identifier(self):
        """Test valid dotted identifiers like schema.table references."""
        assert validate_identifier('air_temple.monks') == 'air_temple.monks'
        assert validate_identifier('fire_nation.army') == 'fire_nation.army'

    def test_invalid_identifier_starts_with_number(self):
        """Test invalid identifiers that start with numbers."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('100_year_war')

    def test_invalid_identifier_special_chars(self):
        """Test invalid identifiers with special characters."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('fire-nation-army')

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('sozins comet')

    def test_disallow_dots_when_specified(self):
        """Test that dots are rejected when allow_dots=False."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('air_temple.monks', allow_dots=False)


class TestTableInitialization:
    """Test Table class initialization and configuration."""

    def test_airbender_table_init(self, airbender_table):
        """Test Air Nomad training table initialization."""
        assert airbender_table.name == 'air_nomad_training'
        assert 'nomad_id' in airbender_table.columns
        assert 'nomad_id' in airbender_table.req_fields
        assert 'nomad_id' in airbender_table.key_fields
        assert airbender_table._paramstyle == ParamStyle.NAMED

    def test_fire_nation_table_init(self, fire_nation_table):
        """Test Fire Nation army table initialization."""
        assert fire_nation_table.name == 'fire_nation_army'
        assert 'soldier_id' in fire_nation_table.columns
        assert 'soldier_id' in fire_nation_table.req_fields
        assert 'soldier_id' in fire_nation_table.key_fields
        assert fire_nation_table._paramstyle == ParamStyle.PYFORMAT

    def test_table_with_invalid_name(self):
        """Test that invalid table names are rejected."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            Table('123_invalid_table', {})

    def test_table_with_invalid_column_name(self):
        """Test that invalid column names are rejected."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            Table('valid_table', {'invalid-column': {'field': 'test'}})


class TestSQLGeneration:
    """Test SQL statement generation for different operations."""

    def test_insert_statement_generation(self, airbender_table):
        """Test INSERT statement for Air Nomad training records."""
        sql = airbender_table._sql_insert
        assert 'INSERT INTO air_nomad_training' in sql
        assert ':nomad_id' in sql
        assert ':name' in sql
        assert 'CURRENT_TIMESTAMP' in sql  # For training_date
        assert ':training_date' not in sql  # training_date is not used to do db_fn

    def test_select_statement_generation(self, airbender_table):
        """Test SELECT statement for finding existing Air Nomads."""
        sql = airbender_table._sql_select
        assert 'SELECT t.* FROM air_nomad_training t' in sql
        assert 'WHERE' in sql
        assert 't.nomad_id = :nomad_id' in sql

    def test_update_statement_creation(self, fire_nation_table):
        """Test UPDATE statement generation for Fire Nation records."""
        # Force update statement creation
        fire_nation_table._sql_update = fire_nation_table._create_update()
        sql = fire_nation_table._sql_update

        assert 'UPDATE fire_nation_army t' in sql
        assert 'SET' in sql
        assert 'WHERE' in sql
        assert 't.soldier_id = %(soldier_id)s' in sql  # Key field in WHERE
        assert 't.name = %(name)s' in sql  # Regular field in SET

    def test_delete_statement_creation(self, airbender_table):
        """Test DELETE statement for removing Air Nomad records."""
        sql = airbender_table._create_delete()
        assert 'DELETE FROM air_nomad_training' in sql
        assert 'WHERE' in sql
        assert 'nomad_id = :nomad_id' in sql


class TestDataProcessing:
    """Test data processing and transformation."""

    def test_set_values_basic(self, airbender_table):
        """Test setting values from Air Nomad recruitment data."""
        aang_data = {
            'trainee_id': 'AANG001',
            'monk_name': 'Aang',
            'home_temple': 'Southern Air Temple',
            'mastery_rank': '4',  # Will be converted to int
            'bison_companion': 'Appa',
            'daily_meditation': '8.5'  # Will be converted to float
        }

        airbender_table.set_values(aang_data)

        assert airbender_table.values['nomad_id'] == 'AANG001'
        assert airbender_table.values['name'] == 'Aang'
        assert airbender_table.values['temple'] == 'Southern Air Temple'
        assert airbender_table.values['airbending_level'] == 4  # Converted to int
        assert airbender_table.values['meditation_score'] == 8.5  # Converted to float
        assert airbender_table.values['instructor'] == 'Monk Gyatso'  # Default value

    def test_set_values_with_transformations(self, fire_nation_table):
        """Test setting values with Fire Nation data transformations."""
        zuko_data = {
            'recruit_number': 'ZUKO001',
            'full_name': 'Prince Zuko',
            'military_rank': 'Banished Prince',
            'flame_intensity': '9',  # High firebending skill
            'birthplace': 'Fire Nation Capital',
            'joined_date': '095 AG'
        }

        fire_nation_table.set_values(zuko_data)

        assert fire_nation_table.values['soldier_id'] == 'ZUKO001'
        assert fire_nation_table.values['name'] == 'Prince Zuko'
        assert fire_nation_table.values['firebending_skill'] == 9  # Converted to int
        assert fire_nation_table.values['commanding_officer'] == 'Admiral Zhao'

    def test_set_values_missing_field(self, airbender_table):
        """Test handling missing fields in Air Nomad data."""
        incomplete_data = {
            'trainee_id': 'JINORA001',
            'monk_name': 'Jinora'
            # Missing home_temple and other fields
        }

        airbender_table.set_values(incomplete_data)

        assert airbender_table.values['nomad_id'] == 'JINORA001'
        assert airbender_table.values['name'] == 'Jinora'
        assert airbender_table.values['temple'] is None
        assert airbender_table.values['airbending_level'] == 0  # Default from transform

    def test_requirements_checking(self, airbender_table):
        """Test checking if Air Nomad requirements are met."""
        # Complete data
        complete_data = {
            'trainee_id': 'TENZIN001',
            'monk_name': 'Tenzin',
            'home_temple': 'Air Temple Island'
        }
        airbender_table.set_values(complete_data)
        assert airbender_table.reqs_met is True
        assert airbender_table.reqs_missing == []

        # Incomplete data
        incomplete_data = {
            'trainee_id': 'MEELO001',
            'monk_name': 'Meelo'
            # Missing temple
        }
        airbender_table.set_values(incomplete_data)
        assert airbender_table.reqs_met is False
        assert 'temple' in airbender_table.reqs_missing


class TestDatabaseOperations:
    """Test database operation execution."""

    def test_exec_insert_success(self, airbender_table, mock_cursor):
        """Test successful insert of Air Nomad training record."""
        airbender_table.set_values({
            'trainee_id': 'IKKI001',
            'monk_name': 'Ikki',
            'home_temple': 'Air Temple Island'
        })

        result = airbender_table.exec_insert(mock_cursor)

        assert result == 0  # Success
        mock_cursor.execute.assert_called_once()

        # Verify SQL and parameters
        call_args = mock_cursor.execute.call_args
        sql, params = call_args[0]
        assert 'INSERT INTO air_nomad_training' in sql
        assert params['nomad_id'] == 'IKKI001'
        assert params['name'] == 'Ikki'

    def test_exec_insert_database_error(self, fire_nation_table, mock_cursor):
        """Test insert failure due to Fire Nation database constraints."""
        mock_cursor.execute.side_effect = Exception("Fire Nation security violation")

        fire_nation_table.set_values({
            'recruit_number': 'AZULA001',
            'full_name': 'Princess Azula',
            'military_rank': 'Fire Nation Princess'
        })

        result = fire_nation_table.exec_insert(mock_cursor)
        assert result == 1  # Error

    def test_exec_update_success(self, airbender_table, mock_cursor):
        """Test successful update of Air Nomad progress."""
        airbender_table.set_values({
            'trainee_id': 'AANG001',
            'monk_name': 'Avatar Aang',
            'home_temple': 'All Temples'
        })

        result = airbender_table.exec_update(mock_cursor)

        assert result == 0  # Success
        mock_cursor.execute.assert_called_once()

    def test_exec_update_missing_requirements(self, airbender_table):
        """Test update failure when Air Nomad requirements not met."""
        airbender_table.set_values({
            'trainee_id': 'INCOMPLETE001'
            # Missing required fields
        })

        with pytest.raises(ValueError, match="required fields are null"):
            airbender_table.exec_update(MagicMock())

    def test_current_row_fetch(self, fire_nation_table, mock_cursor):
        """Test fetching current Fire Nation soldier record."""
        mock_cursor.fetchone.return_value = {
            'soldier_id': 'MAI001',
            'name': 'Mai',
            'rank': 'Royal Guard'
        }

        fire_nation_table.set_values({
            'recruit_number': 'MAI001',
            'full_name': 'Mai',
            'military_rank': 'Royal Guard'
        })

        result = fire_nation_table.current_row(mock_cursor)

        assert result['soldier_id'] == 'MAI001'
        assert result['name'] == 'Mai'
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchone.assert_called_once()


class TestUpdateExclusions:
    """Test column exclusion logic for updates."""

    def test_calc_update_excludes_missing_fields(self, airbender_table):
        """Test excluding Air Nomad fields missing from source data."""
        # Simulate file headers missing some columns
        file_headers = {'trainee_id', 'monk_name', 'home_temple'}  # Missing bison_companion

        airbender_table.calc_update_excludes(file_headers)

        # Should exclude fields that don't have corresponding file columns
        assert 'sky_bison' in airbender_table._update_excludes
        assert 'meditation_score' in airbender_table._update_excludes

        # Should not exclude fields that exist in file
        assert 'name' not in airbender_table._update_excludes
        assert 'temple' not in airbender_table._update_excludes

    def test_calc_update_excludes_no_update_flag(self, airbender_table):
        """Test excluding Air Nomad fields marked as no_update."""
        file_headers = set(airbender_table.columns.keys())

        airbender_table.calc_update_excludes(file_headers)

        # instructor field has no_update=True, should be excluded
        assert 'instructor' in airbender_table._update_excludes

    def test_calc_update_excludes_key_fields(self, fire_nation_table):
        """Test that Fire Nation key fields are excluded from updates."""
        file_headers = set(fire_nation_table.columns.keys())

        fire_nation_table.calc_update_excludes(file_headers)

        # Key fields should always be excluded from SET clause
        assert 'soldier_id' in fire_nation_table._update_excludes


class TestPositionalParameterStyles:
    """Test positional parameter styles (QMARK, NUMERIC, FORMAT)."""

    @patch('dbtk.etl.table.convert_named_to_positional')
    def test_format_parameter_conversion(self, mock_convert, earth_kingdom_table):
        """Test FORMAT (%s) parameter conversion for Earth Kingdom records."""
        mock_convert.return_value = (
            'INSERT INTO earth_kingdom_census (citizen_id, name, city, earthbending_skill, occupation, registration_date, kingdom_loyalty) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, %s)',
            ['citizen_id', 'name', 'city', 'earthbending_skill', 'occupation', 'kingdom_loyalty']
        )

        # Force recreation to trigger conversion
        earth_kingdom_table._sql_insert = earth_kingdom_table._create_insert()

        # Should have called convert function for positional style
        mock_convert.assert_called()
        assert earth_kingdom_table._insert_param_order is not None

        # Verify parameter order doesn't include db_fn columns
        assert 'registration_date' not in earth_kingdom_table._insert_param_order

    def test_positional_parameter_preparation(self, earth_kingdom_table):
        """Test parameter preparation for Earth Kingdom positional binding."""
        # Set up mock conversion results
        earth_kingdom_table._insert_param_order = ['citizen_id', 'name', 'city', 'earthbending_skill', 'occupation',
                                                   'kingdom_loyalty']

        # Test data
        toph_data = {
            'citizen_id': 'TOPH001',
            'name': 'Toph Beifong',
            'city': 'Gaoling',
            'earthbending_skill': 10.0,
            'occupation': 'World\'s Greatest Earthbender',
            'kingdom_loyalty': 'True Earth Kingdom Citizen'
        }

        # Test parameter preparation
        result = earth_kingdom_table._prepare_params(toph_data, earth_kingdom_table._insert_param_order)

        # Should return tuple in correct order
        assert isinstance(result, tuple)
        assert result == ('TOPH001', 'Toph Beifong', 'Gaoling', 10.0, 'World\'s Greatest Earthbender',
                          'True Earth Kingdom Citizen')

    def test_positional_exec_insert(self, earth_kingdom_table, mock_cursor):
        """Test insert execution with positional parameters for Earth Kingdom."""
        # Set up mock conversion
        earth_kingdom_table._insert_param_order = ['citizen_id', 'name', 'city', 'earthbending_skill', 'occupation',
                                                   'kingdom_loyalty']

        earth_kingdom_table.set_values({
            'id': 'KATARA001',
            'full_name': 'Katara',  # Will map to name column
            'home_city': 'Southern Water Tribe',  # Cross-nation refugee in Earth Kingdom
            'boulder_size': '0.0',  # No earthbending skill
            'job_title': 'Waterbending Master'
        })

        result = earth_kingdom_table.exec_insert(mock_cursor)

        assert result == 0  # Success
        mock_cursor.execute.assert_called_once()

        # Verify tuple parameters were passed
        call_args = mock_cursor.execute.call_args[0]
        sql, params = call_args
        assert isinstance(params, tuple)
        assert params[0] == 'KATARA001'  # citizen_id
        assert params[1] == 'Katara'  # name
        assert params[2] == 'Southern Water Tribe'  # city

    def test_positional_select_with_key_fields(self, earth_kingdom_table, mock_cursor):
        """Test SELECT statement with positional parameters for Earth Kingdom lookups."""
        # Mock the conversion for SELECT statement
        with patch('dbtk.etl.table.convert_named_to_positional') as mock_convert:
            mock_convert.return_value = (
                'SELECT t.* FROM earth_kingdom_census t WHERE t.citizen_id = %s',
                ['citizen_id']
            )

            # Force SELECT statement creation
            earth_kingdom_table._sql_select = earth_kingdom_table._create_select()
            earth_kingdom_table._param_order_select = ['citizen_id']

            # Set values and test current_row
            earth_kingdom_table.set_values({
                'id': 'BUMI001',
                'full_name': 'King Bumi',
                'home_city': 'Omashu'
            })

            earth_kingdom_table.current_row(mock_cursor)

            # Verify SELECT was called with tuple parameters
            mock_cursor.execute.assert_called_once()
            call_args = mock_cursor.execute.call_args[0]
            sql, params = call_args
            assert isinstance(params, tuple)
            assert params == ('BUMI001',)

    def test_positional_update_execution(self, earth_kingdom_table, mock_cursor):
        """Test UPDATE statement with positional parameters for Earth Kingdom records."""
        # Mock conversion for UPDATE statement
        with patch('dbtk.etl.table.convert_named_to_positional') as mock_convert:
            mock_convert.return_value = (
                'UPDATE earth_kingdom_census t SET t.name = %s, t.city = %s, t.earthbending_skill = %s, t.occupation = %s, t.kingdom_loyalty = %s WHERE t.citizen_id = %s',
                ['name', 'city', 'earthbending_skill', 'occupation', 'kingdom_loyalty', 'citizen_id']
            )

            # Set update parameter order
            earth_kingdom_table._update_param_order = ['name', 'city', 'earthbending_skill', 'occupation',
                                                       'kingdom_loyalty', 'citizen_id']

            earth_kingdom_table.set_values({
                'id': 'SUKI001',
                'full_name': 'Suki',
                'home_city': 'Kyoshi Island',
                'boulder_size': '2.5',  # Some earthbending training
                'job_title': 'Kyoshi Warrior Leader'
            })

            result = earth_kingdom_table.exec_update(mock_cursor)

            assert result == 0
            mock_cursor.execute.assert_called_once()

            # Verify tuple parameters in correct order
            call_args = mock_cursor.execute.call_args[0]
            sql, params = call_args
            assert isinstance(params, tuple)
            assert len(params) == 6
            assert params[-1] == 'SUKI001'  # citizen_id should be last (WHERE clause)


class TestParameterStyles:
    """Test different database parameter styles."""

    def test_named_parameter_style(self):
        """Test Oracle-style named parameters for Water Tribe records."""
        water_table = Table('water_tribe_members', {
            'member_id': {'field': 'id'},
            'name': {'field': 'full_name'},
            'tribe': {'field': 'home_tribe'}
        }, paramstyle=ParamStyle.NAMED)

        sql = water_table._sql_insert
        assert ':member_id' in sql
        assert ':name' in sql
        assert ':tribe' in sql

    def test_pyformat_parameter_style(self, fire_nation_table):
        """Test PostgreSQL-style pyformat parameters for Fire Nation records."""
        sql = fire_nation_table._sql_insert
        assert '%(soldier_id)s' in sql
        assert '%(name)s' in sql
        assert '%(rank)s' in sql


class TestAdvancedFeatures:
    """Test advanced ETL features."""

    def test_database_function_wrapping(self, airbender_table):
        """Test wrapping Air Nomad fields with database functions."""
        # Test simple placeholder
        result = airbender_table._wrap_db_function('test_field')
        assert result == ':test_field'

        # Test function with placeholder substitution
        result = airbender_table._wrap_db_function('test_field', 'UPPER(#)')
        assert result == 'UPPER(:test_field)'

        # Test static function
        result = airbender_table._wrap_db_function('test_field', 'CURRENT_TIMESTAMP')
        assert result == 'CURRENT_TIMESTAMP'

    def test_complex_etl_scenario(self, airbender_table, mock_cursor):
        """Test complete ETL scenario for Air Nomad recruitment."""
        # Setup mock for existing record check
        mock_cursor.fetchone.return_value = None  # No existing record

        # Process new Air Nomad recruit
        recruit_data = {
            'trainee_id': 'ROHAN001',
            'monk_name': 'Rohan',
            'home_temple': 'Air Temple Island',
            'mastery_rank': '2',
            'bison_companion': 'Young Bison',
            'daily_meditation': '6.0'
        }

        airbender_table.set_values(recruit_data)

        # Verify requirements are met
        assert airbender_table.reqs_met

        # Check if record exists (simulate new recruit)
        existing = airbender_table.current_row(mock_cursor)
        assert existing is None

        # Insert new record
        result = airbender_table.exec_insert(mock_cursor)
        assert result == 0  # Success

        # Verify the INSERT was called
        mock_cursor.execute.assert_called()
        call_args = mock_cursor.execute.call_args[0]
        sql, params = call_args

        assert 'INSERT INTO air_nomad_training' in sql
        assert params['nomad_id'] == 'ROHAN001'
        assert params['name'] == 'Rohan'
        assert params['airbending_level'] == 2