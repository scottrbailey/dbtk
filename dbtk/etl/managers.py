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
        An error occurred while creating or updating the entity. Possibly impacting
        downstream processing.
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
    restored with :meth:`load_state`, allowing long-running and multi-stage imports
    to be resumed without re-querying already-resolved entities.

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
        Additional key fields to track per entity (e.g. ``['staging_id', 'erp_vendor_id']``).
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

        # to initialize from saved state
        em = IdentityManager.load_state('state/students.json', resolver=stmt)
        em.batch_resolve([EntityStatus.STAGED])
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
            Field name of the target-system primary key. Must be returned by the resolver.
            Set equal to source_key to skip ID resolution.
        resolver : PreparedStatement or TableLookup, optional
            Resolution query.  Accepts either type; ``TableLookup`` is unwrapped
            to its underlying ``PreparedStatement``.
        alternate_keys : list of str, optional
            Additional key fields to persist and track per entity.
        """
        self.source_key = source_key
        self.target_key = target_key
        self.alternate_keys = alternate_keys if alternate_keys else []
        self._lookup: Optional[TableLookup] = None  # kept when resolver is a TableLookup
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

        Pass ``None`` to clear the resolver (useful when loading state for
        inspection without re-querying).
        """
        if value is None:
            self._resolver = None
            self._lookup = None
        elif isinstance(value, TableLookup):
            self._resolver = value._stmt
            self._lookup = value
        elif isinstance(value, PreparedStatement):
            self._resolver = value
            self._lookup = None
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
            if self.resolver and not self.resolver.cursor._row_factory_invalid:
                temp_class = self.resolver.cursor.record_factory
            else:
                temp_class = type('tempEntityRecord', (Record,), {})
                keys = [self.source_key]
                if self.target_key != self.source_key:
                    keys.append(self.target_key)
                temp_class.set_fields(keys)
            record = temp_class()
        alt_keys = [fld for fld in self.alternate_keys if fld not in record]
        fields = list(record.keys()) + alt_keys + ['_status', '_errors', '_messages']
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
        if self.resolver is None:
            if self.target_key == self.source_key:
                status = EntityStatus.RESOLVED
            else:
                status = EntityStatus.STAGED
            if not self._record_factory:
                self._setup_record_class(record)
            if record:
                entity = self._record_factory(**record)
                entity['_status'] = status
                entity['_messages'] = []
                entity['_errors'] = []
            else:
                entity_dict = {self.source_key: source_id,
                               '_status': status,
                               '_messages': [], '_errors': []}
                entity = self._record_factory(**entity_dict)
            self.entities[source_id] = entity
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

        # If the resolver is backed by an exhaustive preloaded cache, a miss means the
        # record definitively does not exist — skip the DB round-trip.  Only applies
        # once _record_factory is warm (first hit establishes it via the normal path).
        if self._lookup and self._lookup.exhaustive and self._record_factory:
            cache_key = tuple(bind_vars.get(n) for n in self._lookup._key_col_names)
            if cache_key not in self._lookup._cache:
                entity = self._record_factory(**{self.source_key: source_id})
                entity['_messages'] = []
                entity['_errors'] = []
                entity['_status'] = EntityStatus.NOT_FOUND
                self.entities[source_id] = entity
                return entity

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

    def batch_resolve(self, additional_statuses: Optional[List[str]] = None):
        """
        Re-run the resolver for all entities whose status is PENDING or NOT_FOUND.

        Useful after bulk-loading staging data when some entities could not be
        resolved on first pass.  Initializes the record factory from a dry-run
        resolver call if it has not yet been set up.

        Parameters
        ----------
        additional_statuses : optional list of str
            Additional statuses to resolve in addition to EntityStatus.NOT_FOUND and
            EntityStatus.PENDING
        """

        if not self._record_factory:
            self.resolver.execute({})
            self._setup_record_class(None)

        statuses = {EntityStatus.PENDING, EntityStatus.NOT_FOUND}
        if additional_statuses:
            statuses.update(additional_statuses)
        for source_id, entity in self.entities.items():
            if entity.get('_status') in statuses:
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
      - Can return a specific field from the lookup result instead of the raw code

    Supports:
      - Preload mode: instant enrichment, perfect valid/new split
      - Lazy mode: enrich on first encounter
      - No lookup: pure collection

    Set ``return_col`` to the field name you want returned; ``None`` (default)
    returns the raw code.
    """

    def __init__(
        self,
        lookup: Optional[TableLookup] = None,
        return_col: Optional[str] = None,
    ):
        self.lookup = lookup
        self.return_col = return_col

        self.existing: Dict[Any, Any] = {}   # code -> raw lookup result
        self.added: Dict[Any, Any] = {}      # new codes: None until annotated, then dict
        self._recently_added: bool = False

        if lookup:
            self.key_name = (
                lookup._key_col_names[0]
                if isinstance(lookup._key_col_names, (list, tuple))
                else lookup._key_col_names
            )

            if lookup._cache_strategy == TableLookup.CACHE_PRELOAD and lookup._preloaded:
                self._preload_all()

    def _preload_all(self):
        """Populate existing from preloaded cache, storing raw results."""
        for cache_key, result in self.lookup._cache.items():
            code = cache_key[0] if isinstance(cache_key, tuple) else cache_key
            self.existing[code] = result

    def _extract_col(self, result: Any) -> Optional[str]:
        """Extract return_col from a lookup result, or str-ify a scalar."""
        if self.return_col is None:
            return None
        if isinstance(result, (str, int, float)):
            return str(result)
        if hasattr(result, "get"):
            val = result.get(self.return_col)
            return str(val) if val is not None else None
        if isinstance(result, (tuple, list)):
            # Fall back to second element when result is a plain sequence
            return str(result[1]) if len(result) > 1 else None
        return str(result)

    def __call__(self, value: Any) -> Any:
        if value is None:
            return value

        self._recently_added = False

        if isinstance(value, str):
            raw_codes = [c.strip() for c in value.split(",") if c.strip()]
        elif isinstance(value, (list, tuple, set)):
            raw_codes = [c for c in value if c]
        else:
            raw_codes = [value]

        enriched = []
        for code in raw_codes:
            if code in self.existing:
                col = self._extract_col(self.existing[code])
            elif code in self.added:
                data = self.added[code]
                col = self._extract_col(data) if data is not None else None
            else:
                # Only query DB if lookup exists and isn't preloaded
                # Preloaded means all valid values are in cache, so cache miss = new value
                if self.lookup and not self.lookup._preloaded:
                    result = self.lookup({self.key_name: code})
                    if result:
                        self.existing[code] = result
                        col = self._extract_col(result)
                    else:
                        self.added[code] = None
                        self._recently_added = True
                        col = None
                else:
                    # No lookup or preloaded (cache miss = definitely new)
                    self.added[code] = None
                    self._recently_added = True
                    col = None

            enriched.append(col if self.return_col else code)

        # Return in original format
        if isinstance(value, str):
            joined = ",".join(e for e in enriched if e is not None)
            return joined if joined else None
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
                reader.add_filter(lambda r: r.tconst in title_collector)
                for record in reader:
                    process(record)
        """
        return value in self.existing or value in self.added

    # Reporting
    def get_valid_mapping(self) -> Dict[Any, Optional[str]]:
        return {code: self._extract_col(result) for code, result in self.existing.items()}

    def get_all_mapping(self) -> Dict[Any, Optional[str]]:
        combined = {code: self._extract_col(result) for code, result in self.existing.items()}
        combined.update({
            code: self._extract_col(fields) if fields else None
            for code, fields in self.added.items()
        })
        return combined

    def collect_new(self, code: Any, **fields) -> None:
        """
        Attach extra fields to a newly-encountered code for later bulk insertion.

        No-ops immediately when the preceding ``__call__`` did not add a new code
        (``_recently_added`` is False), so it is safe to call unconditionally on
        every record. First annotation wins — subsequent calls for the same code
        are ignored.

        Parameters
        ----------
        code : Any
            The code value that was passed to the validator (used as a cross-check
            and as the key into ``added``).
        **fields
            Extra columns to store, e.g. ``stvcipc_desc=record.cip_discipline``.

        Example
        -------
        ::

            cip_validator = ValidationCollector(lookup=cip_lookup)
            for record in reader:
                stvmajr.set_values(record)      # triggers cip_validator(record.cip_code)
                cip_validator.collect_new(record.cip_code, stvcipc_desc=record.cip_discipline)

        """
        if not self._recently_added:
            return
        self._recently_added = False
        if self.added.get(code) is None:
            self.added[code] = fields

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
            reader.add_filter(lambda r: r.tconst in title_collector)  # Uses __contains__
        """
        return set(self.existing.keys()) | set(self.added.keys())
