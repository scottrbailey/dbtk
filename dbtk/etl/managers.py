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

logger = logging.getLogger(__name__)


class EntityStatus:
    """Status values for entity resolution lifecycle."""
    PENDING = "pending"
    RESOLVED = "resolved"
    STAGED = "staged"     # exists in staging tables, not yet matched to ERP
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


class ErrorDetail:
    """Structured error record attached to an entity."""

    __slots__ = ("message", "stage", "field", "code")

    def __init__(
        self,
        message: str,
        stage: Optional[str] = None,
        field: Optional[str] = None,
        code: Optional[str] = None,
    ):
        self.message = message
        self.stage = stage
        self.field = field
        self.code = code

    def __repr__(self) -> str:
        return (
            f"ErrorDetail(message={self.message!r}, stage={self.stage!r}, "
            f"field={self.field!r}, code={self.code!r})"
        )


class IdentityManager:
    def __init__(
        self,
        source_key: str,
        target_key: str,
        resolver: Optional[Union[PreparedStatement, TableLookup]] = None,
        alternate_keys: Optional[List[str]] = None
    ):
        self.source_key = source_key
        self.target_key = target_key
        self.alternate_keys = alternate_keys if alternate_keys else []
        self.resolver = resolver  # goes through property setter
        self.entities: Dict[Any, Record] = {}
        self._record_factory: Optional[type[Record]] = None  # lazy-created

    @property
    def resolver(self) -> Optional[PreparedStatement]:
        return self._resolver

    @resolver.setter
    def resolver(self, value: Optional[Union[PreparedStatement, TableLookup]]):
        if value is None:
            self._resolver = None
        elif isinstance(value, TableLookup):
            self._resolver = value._stmt
        elif isinstance(value, PreparedStatement):
            self._resolver = value
        else:
            raise ValueError('Resolver must be either a PreparedStatement or TableLookup')

    def _setup_record_class(self, record: Optional[Record] = None):
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
        entity = self.entities[source_id]
        if entity.get('_messages') is None:
            entity['_messages'] = []
        entity['_messages'].append(message)

    def add_error(self, source_id: str, error: ErrorDetail):
        entity = self.entities[source_id]
        if entity.get('_errors') is None:
            entity['_errors'] = []
        entity['_errors'].append(error)

    def set_id(self, source_id: str, id_type: str, value: str):
        if id_type not in self.alternate_keys and id_type != self.target_key:
            raise ValueError(f'id_type must be either the target_key or one of the alternate_keys')
        entity = self.entities[source_id]
        entity[id_type] = value

    def get_id(self, source_id: str, id_type: str):
        if id_type not in self.alternate_keys and id_type != self.target_key:
            raise ValueError(f'id_type must be either the target_key or one of the alternate_keys')
        return self.entities[source_id].get(id_type)

    def batch_resolve(self):
        if not self._record_factory:
            self.resolver.execute({})
            self._setup_record_class(None)
        for source_id, entity in self.entities.items():
            if entity.get('_status') in (EntityStatus.NOT_FOUND, EntityStatus.PENDING):
                self.resolve(source_id)

    def calc_stats(self):
        counts = {s: 0 for s in EntityStatus.VALUES}
        for entity in self.entities.values():
            status = entity.get('_status')
            if status:
                counts[status] += 1
        return counts

    def save_state(self, path: Union[str, Path]):
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
                return {'message': obj.message, 'stage': obj.stage,
                        'field': obj.field, 'code': obj.code}
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
