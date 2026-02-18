# dbtk/etl/managers.py

"""
Orchestration tools for multi-stage, resumable ETL processes.

IdentityManager provides lightweight, incremental identity resolution
for imports where a reliable source primary key exists in source data.
"""

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from ..cursors import PreparedStatement
from .transforms.database import TableLookup
from ..record import Record
from ..utils import ErrorDetail

logger = logging.getLogger(__name__)


class EntityStatus:
    """
    Status constants for the entity resolution lifecycle.

    Attributes
    ----------
    PENDING : str
        Entity has been registered but resolution has not yet been attempted.
    RESOLVED : str
        Entity was successfully matched; ``target_key`` is populated.
    STAGED : str
        Entity exists in a staging table but has not yet been matched to the
        target system (e.g. an ERP record not yet confirmed).
    ERROR : str
        Resolution was attempted and failed due to an unexpected error.
    SKIPPED : str
        Resolution was intentionally bypassed for this entity.
    NOT_FOUND : str
        Resolution was attempted but no matching record was found in the target.
    """
    PENDING = "pending"
    RESOLVED = "resolved"
    STAGED = "staged"
    ERROR = "error"
    SKIPPED = "skipped"
    NOT_FOUND = "not_found"

    VALUES = (
        PENDING,
        RESOLVED,
        STAGED,
        ERROR,
        SKIPPED,
        NOT_FOUND,
    )

    @classmethod
    def __iter__(cls):
        yield from cls.VALUES


class IdentityManager:
    """
    Lightweight, resumable identity-resolution cache for ETL imports.

    Maps source-system primary keys to target-system identifiers using a
    SQL resolver query.  Resolved entities are stored as :class:`dbtk.record.Record`
    instances keyed by ``source_key`` and enriched with status, messages,
    errors, and any configured ``alternate_keys``.

    State can be persisted to JSON between runs with :meth:`save_state` and
    restored with :meth:`load_state`, allowing long-running imports to be
    resumed without re-querying already-resolved entities.

    Parameters
    ----------
    source_key : str
        Field name for the source-system primary key (e.g. ``'student_id'``).
    target_key : str
        Field name for the target-system primary key that the resolver returns
        (e.g. ``'erp_person_id'``).
    resolver : PreparedStatement or TableLookup, optional
        Query used to look up a ``target_key`` from a ``source_key``.
        Can be set or replaced later via the ``resolver`` property.
    alternate_keys : list of str, optional
        Additional key fields to track per entity (e.g. ``['banner_id', 'erp_vendor_id']``).
        These are persisted alongside ``target_key`` in saved state.

    Attributes
    ----------
    entities : dict
        Mapping of source_key value → resolved :class:`dbtk.record.Record`.
        Each Record contains all resolver columns plus ``_status``,
        ``_errors``, ``_messages``, and any ``alternate_keys``.

    Example
    -------
    ::

        stmt = cursor.prepare_file('sql/resolve_student.sql')
        im = IdentityManager('student_id', 'erp_person_id', resolver=stmt,
                             alternate_keys=['banner_id'])

        for row in reader:
            entity = im.resolve(row)
            if entity['_status'] == EntityStatus.RESOLVED:
                table.set_values(row)
                if table.execute('insert'):          # returns 1 on DB error
                    im.add_error(row['student_id'], table.last_error)
            else:
                im.add_error(row['student_id'],
                             ErrorDetail('Not found', field='student_id'))

        im.save_state('state/students.json')
    """

    def __init__(
        self,
        source_key: str,
        target_key: str,
        resolver: Optional[Union[PreparedStatement, TableLookup]] = None,
        alternate_keys: Optional[List[str]] = None
    ):
        """
        Initialize IdentityManager.

        Parameters
        ----------
        source_key : str
            Field name of the source-system primary key.
        target_key : str
            Field name of the target-system primary key returned by the resolver.
        resolver : PreparedStatement or TableLookup, optional
            Resolution query.  Accepts either type; ``TableLookup`` is unwrapped
            to its underlying ``PreparedStatement``.
        alternate_keys : list of str, optional
            Additional key fields to persist and track per entity.
        """
        self.source_key = source_key
        self.target_key = target_key
        self.alternate_keys = alternate_keys if alternate_keys else []
        self.resolver = resolver  # goes through property setter
        self.entities: Dict[Any, Record] = {}
        self._record_factory: Optional[type[Record]] = None  # lazy-created

    @property
    def resolver(self) -> Optional[PreparedStatement]:
        """The active PreparedStatement used for resolution queries."""
        return self._resolver

    @resolver.setter
    def resolver(self, value: Optional[Union[PreparedStatement, TableLookup]]):
        """
        Set the resolver, accepting either a PreparedStatement or TableLookup.

        ``TableLookup`` instances are automatically unwrapped to their
        underlying ``PreparedStatement``.  Pass ``None`` to clear the resolver
        (useful when loading state for inspection without re-querying).
        """
        if value is None:
            self._resolver = None
        elif isinstance(value, TableLookup):
            self._resolver = value._stmt
        elif isinstance(value, PreparedStatement):
            self._resolver = value
        else:
            raise ValueError('Resolver must be either a PreparedStatement or TableLookup')

    def _setup_record_class(self, record: Optional[Record] = None):
        """
        Create and cache the EntityRecord subclass used for all entities.

        Derives field list from ``record`` (a freshly resolved row) or from
        the resolver cursor's current record factory when ``record`` is None.
        Appends any ``alternate_keys`` not already present, then adds
        ``_status``, ``_errors``, and ``_messages`` sentinel fields.

        Called automatically on first resolution; idempotent after that.
        """
        if self._record_factory:
            return self._record_factory
        if record is None:
            if not self.resolver.cursor._row_factory_invalid:
                temp_class = self.resolver.cursor.record_factory
            else:
                temp_class = type('tempEntityRecord', (Record,), {})
                temp_class.set_fields([self.source_key, self.target_key])
            record = temp_class()
        alt_keys = [fld for fld in self.alternate_keys if fld not in record]
        fields = record.keys() + alt_keys + ['_status', '_errors', '_messages']
        RecordClass = type('EntityRecord', (Record,), {})
        RecordClass.set_fields(fields)
        if self.target_key not in RecordClass._fields \
                and self.target_key not in RecordClass._fields_normalized:
            raise ValueError(f'{self.target_key} must be returned by the primary resolver')
        self._record_factory = RecordClass

    def resolve(self, value: Any) -> Optional[Record]:
        """
        Resolve a source key to a target entity, caching the result.

        Parameters
        ----------
        value : scalar, dict, or Record
            * **scalar** — treated as the raw ``source_key`` value.  The
              resolver is called with ``{source_key: value}`` and the
              returned entity is cached but the caller's record is *not*
              mutated.
            * **dict or Record** — ``source_key`` is extracted from the
              mapping.  On a successful resolution the ``target_key`` is
              written back into the caller's record.

        Returns
        -------
        Record or None
            The cached/resolved entity Record, or ``None`` if ``source_key``
            cannot be found in ``value``.

        Raises
        ------
        ValueError
            If the resolved ``target_key`` conflicts with a value already
            present in the caller's record.
        """
        if isinstance(value, (dict, Record)):
            source_id = value.get(self.source_key)
            if source_id is None:
                return None
            record = value
            update_target_key = True
        else:
            source_id = value
            record = None
            update_target_key = False

        # Check cache / existing entity
        if source_id in self.entities:
            entity = self.entities[source_id]
            if entity['_status'] == EntityStatus.RESOLVED:
                resolved_id = entity[self.target_key]
                if update_target_key and record is not None:
                    existing = record.get(self.target_key)
                    if existing is not None and existing != resolved_id:
                        raise ValueError(
                            f"Conflict on {self.target_key}: existing={existing!r}, resolved={resolved_id!r}"
                        )
                    record[self.target_key] = resolved_id
                return entity

        # Not cached → run primary resolver
        # Prefer passing any existing entity data so alternate_keys and partial
        # results from prior lookups are available to the query.
        if record:
            bind_vars = self.resolver.cursor.prepare_params(self.resolver.param_names, record)
        elif source_id in self.entities:
            bind_vars = self.resolver.cursor.prepare_params(self.resolver.param_names, self.entities[source_id])
        else:
            bind_vars = {self.source_key: source_id}
        self.resolver.execute(bind_vars)
        resolved_raw = self.resolver.fetchone()
        if self._record_factory is None:
            self._setup_record_class(resolved_raw)
        if resolved_raw is None:
            resolved_raw = {self.source_key: source_id}
        entity = self._record_factory(**resolved_raw)
        entity['_messages'] = []
        entity['_errors'] = []
        if entity.get(self.target_key) is None:
            entity['_status'] = EntityStatus.NOT_FOUND
        else:
            entity['_status'] = EntityStatus.RESOLVED
        self.entities[source_id] = entity

        # Mutate input record if provided
        if update_target_key and record is not None and entity['_status'] == EntityStatus.RESOLVED:
            existing = record.get(self.target_key)
            if existing is not None and existing != entity[self.target_key]:
                raise ValueError(
                    f"Conflict on {self.target_key}: existing={existing!r}, resolved={entity[self.target_key]!r}"
                )
            record[self.target_key] = entity[self.target_key]

        return entity

    def add_message(self, source_id: str, message: str):
        """
        Append an informational message to an entity's ``_messages`` list.

        Parameters
        ----------
        source_id : str
            Source-system key identifying the entity (must already be cached).
        message : str
            Message text to append.
        """
        entity = self.entities[source_id]
        if entity.get('_messages') is None:
            entity['_messages'] = []
        entity['_messages'].append(message)

    def add_error(self, source_id: str, error: ErrorDetail):
        """
        Append an :class:`dbtk.utils.ErrorDetail` to an entity's ``_errors`` list.

        Parameters
        ----------
        source_id : str
            Source-system key identifying the entity (must already be cached).
        error : ErrorDetail
            Structured error to attach to the entity.
        """
        entity = self.entities[source_id]
        if entity.get('_errors') is None:
            entity['_errors'] = []
        entity['_errors'].append(error)

    def set_id(self, source_id: str, id_type: str, value: str):
        """
        Store a target or alternate key value for a cached entity.

        Parameters
        ----------
        source_id : str
            Source-system key identifying the entity (must already be cached).
        id_type : str
            Either ``target_key`` or one of ``alternate_keys``.
        value : str
            The identifier value to store.

        Raises
        ------
        ValueError
            If ``id_type`` is not the ``target_key`` or a registered ``alternate_key``.
        """
        if id_type not in self.alternate_keys and id_type != self.target_key:
            raise ValueError(f'id_type must be either the target_key or one of the alternate_keys')
        entity = self.entities[source_id]
        entity[id_type] = value

    def get_id(self, source_id: str, id_type: str):
        """
        Retrieve a target or alternate key value for a cached entity.

        Parameters
        ----------
        source_id : str
            Source-system key identifying the entity (must already be cached).
        id_type : str
            Either ``target_key`` or one of ``alternate_keys``.

        Returns
        -------
        str or None
            The stored identifier value, or ``None`` if not yet set.

        Raises
        ------
        ValueError
            If ``id_type`` is not the ``target_key`` or a registered ``alternate_key``.
        """
        if id_type not in self.alternate_keys and id_type != self.target_key:
            raise ValueError(f'id_type must be either the target_key or one of the alternate_keys')
        return self.entities[source_id].get(id_type)

    def batch_resolve(self):
        """
        Re-run the resolver for all entities whose status is PENDING or NOT_FOUND.

        Useful after bulk-loading staging data when some entities could not be
        resolved on first pass.  Initializes the record factory from a dry-run
        resolver call if it has not yet been set up.
        """
        if not self._record_factory:
            self.resolver.execute({})
            self._setup_record_class(None)
        for source_id, entity in self.entities.items():
            if entity.get('_status') in (EntityStatus.NOT_FOUND, EntityStatus.PENDING):
                self.resolve(source_id)

    def calc_stats(self):
        """
        Count entities by status.

        Returns
        -------
        dict
            Mapping of each :class:`EntityStatus` value to the number of
            entities currently at that status.

        Example
        -------
        ::

            stats = im.calc_stats()
            print(stats)
            # {'pending': 0, 'resolved': 142, 'staged': 5, 'error': 3, ...}
        """
        counts = {s: 0 for s in EntityStatus.VALUES}
        for entity in self.entities.values():
            status = entity.get('_status')
            if status:
                counts[status] += 1
        return counts

    def save_state(self, path: Union[str, Path]):
        """
        Persist the current entity cache to a JSON file.

        The file captures ``source_key``, ``target_key``, ``alternate_keys``,
        ``field_order`` (for factory reconstruction), summary stats, and the
        full entity dict.  :class:`dbtk.utils.ErrorDetail` objects are
        serialized to ``{"message": ..., "field": ..., "code": ...}`` dicts.

        Parameters
        ----------
        path : str or Path
            Destination file path.  Parent directory must exist.
        """
        field_order = None
        if self._record_factory:
            field_order = self._record_factory._fields  # exact ordered list
        elif self.entities:
            # Rare fallback: first entity might have partial fields
            first_entity = next(iter(self.entities.values()))
            field_order = list(first_entity.keys())  # whatever we have
        else:
            field_order = [self.target_key, '_status', '_errors', '_messages']

        def _serialize(obj):
            if isinstance(obj, ErrorDetail):
                return {'message': obj.message, 'field': obj.field, 'code': obj.code}
            return str(obj)
        stats = self.calc_stats()
        data = {
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
            "source_key": self.source_key,
            "target_key": self.target_key,
            "alternate_keys": self.alternate_keys,
            "field_order": field_order,
            "stats": stats,
            "entities": {
                str(source_pk): entity.to_dict(normalized=False)
                for source_pk, entity in self.entities.items()
            }
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=_serialize)
        logger.info(f"IdentityManager saved state to {path}")

    @classmethod
    def load_state(cls, path: Union[str, Path],
                   resolver: Optional[Union[PreparedStatement, TableLookup]] = None) -> 'IdentityManager':
        """
        Restore an IdentityManager from a previously saved JSON file.

        Re-creates the entity Record factory from ``field_order`` stored in
        the file.  Deserializes ``_errors`` lists back to
        :class:`dbtk.utils.ErrorDetail` instances.

        Parameters
        ----------
        path : str or Path
            Path to the JSON file written by :meth:`save_state`.
        resolver : PreparedStatement or TableLookup, optional
            Resolver to attach to the restored instance.  If the saved file
            has no ``field_order``, the resolver is used as a fallback to
            initialize the record factory.

        Returns
        -------
        IdentityManager
            Fully restored instance with all entities re-hydrated.
        """
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        instance = cls(
            source_key=data["source_key"],
            target_key=data["target_key"],
            alternate_keys=data.get("alternate_keys", []),
            resolver=resolver,
        )

        # Re-create factory from saved field_order
        field_order = data.get("field_order")
        if field_order:
            RecordClass = type('EntityRecord', (Record,), {})
            RecordClass.set_fields(field_order)
            instance._record_factory = RecordClass
        elif resolver:
            logger.warning("No field_order in saved state — falling back to resolver")
            instance.resolver.execute({})
            instance._setup_record_class()
        else:
            logger.warning("No field_order in saved state and no resolver — entity factory unavailable")

        instance.entities = {}
        for source_pk, entity_data in data["entities"].items():
            if isinstance(entity_data.get('_errors'), list):
                entity_data['_errors'] = [
                    ErrorDetail(**e) if isinstance(e, dict) else e
                    for e in entity_data['_errors']
                ]
            if instance._record_factory:
                entity = instance._record_factory(**entity_data)
            else:
                entity = Record(**entity_data)
            instance.entities[source_pk] = entity
        logger.info(f"IdentityManager loaded state from {path}")
        return instance


class ValidationCollector:
    """
    Callable collector/enricher for fn pipelines.

    During row-wise processing:
      - Collects unique codes
      - Optionally enriches them with descriptions using TableLookup
      - Can return enriched values (titles) instead of raw codes

    Supports:
      - Preload mode: instant enrichment, perfect valid/new split
      - Lazy mode: enrich on first encounter
      - No lookup: pure collection

    Returns enriched value if return_desc=True (default), else raw.
    """

    def __init__(
        self,
        lookup: Optional[TableLookup] = None,
        desc_field: Optional[str] = None,
        return_desc: bool = True,
    ):
        self.lookup = lookup
        self.desc_field = desc_field
        self.return_desc = return_desc

        self.existing: Dict[Any, str] = {}  # code -> description
        self.added: set = set()             # new codes

        if lookup:
            self.key_name = (
                lookup._key_col_names[0]
                if isinstance(lookup._key_col_names, (list, tuple))
                else lookup._key_col_names
            )

            if lookup._cache_strategy == TableLookup.CACHE_PRELOAD and lookup._preloaded:
                self._preload_all()

    def _preload_all(self):
        """Extract all valid codes + descriptions from preloaded cache."""
        for cache_key, result in self.lookup._cache.items():
            code = cache_key[0] if isinstance(cache_key, tuple) else cache_key
            desc = self._extract_desc(result)
            self.existing[code] = desc

    def _extract_desc(self, result: Any) -> str:
        if isinstance(result, (str, int, float)):
            return str(result)

        if hasattr(result, "get"):
            if self.desc_field:
                return result.get(self.desc_field, "")
            for field in ("title", "description", "name", "label"):
                if val := result.get(field):
                    return val
            return str(result)

        if isinstance(result, (tuple, list)) and len(result) > 1:
            return result[1]

        return str(result)

    def __call__(self, value: Any) -> Any:
        if value is None:
            return value

        if isinstance(value, str):
            raw_codes = [c.strip() for c in value.split(",") if c.strip()]
        elif isinstance(value, (list, tuple, set)):
            raw_codes = [c for c in value if c]
        else:
            raw_codes = [value]

        enriched = []
        for code in raw_codes:
            if code in self.existing:
                desc = self.existing[code]
            elif code in self.added:
                desc = code
            else:
                # Only query DB if lookup exists and isn't preloaded
                # Preloaded means all valid values are in cache, so cache miss = new value
                if self.lookup and not self.lookup._preloaded:
                    result = self.lookup({self.key_name: code})
                    if result:
                        desc = self._extract_desc(result)
                        self.existing[code] = desc
                    else:
                        desc = code
                        self.added.add(code)
                else:
                    # No lookup or preloaded (cache miss = definitely new)
                    desc = code
                    self.added.add(code)

            enriched.append(desc if self.return_desc else code)

        # Return in original format
        if isinstance(value, str):
            return ",".join(enriched)
        return enriched if isinstance(value, (list, tuple)) else enriched[0]

    def __contains__(self, value: Any) -> bool:
        """
        Support 'in' operator for validation.

        Check if a value exists in either existing or added sets.
        Useful for validating/filtering records based on collected values.

        Parameters
        ----------
        value : Any
            The value to check

        Returns
        -------
        bool
            True if value exists in either existing or added sets

        Example
        -------
        ::

            # Collect titles
            title_collector = ValidationCollector()
            for record in titles_reader:
                title_collector(record['tconst'])

            # Filter principals based on collected titles
            with get_reader('title.principals.tsv.gz') as reader:
                reader.filter(lambda r: r.tconst in title_collector)
                for record in reader:
                    process(record)
        """
        return value in self.existing or value in self.added

    # Reporting
    def get_valid_mapping(self) -> Dict[Any, str]:
        return dict(self.existing)

    def get_new_codes(self) -> List[Any]:
        return sorted(self.added)

    def get_all_mapping(self) -> Dict[Any, str]:
        combined = dict(self.existing)
        combined.update({code: code for code in self.added})
        return combined

    def get_all(self) -> set:
        """
        Get all codes (existing + added) as a set.

        Useful for filtering with tools like polars that need a set/list
        of valid values rather than a callable.

        Returns
        -------
        set
            Union of existing codes and added codes

        Example
        -------
        ::

            # Collect valid titles
            title_collector = ValidationCollector()
            for record in titles:
                title_collector(record['tconst'])

            # Use with polars filtering
            all_titles = title_collector.get_all()
            df = pl.scan_csv('principals.tsv.gz').filter(
                pl.col('tconst').is_in(all_titles)
            )

            # Or with dbtk reader filtering
            reader.filter(lambda r: r.tconst in title_collector)  # Uses __contains__
        """
        return set(self.existing.keys()) | self.added
