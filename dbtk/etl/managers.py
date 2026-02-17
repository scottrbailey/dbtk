# dbtk/etl/managers.py

"""
Orchestration tools for multi-stage, resumable ETL processes.

EntityManager provides lightweight, incremental entity orchestration
for imports where a reliable primary key exists in source data.
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
    ERROR = "error"
    SKIPPED = "skipped"
    NOT_FOUND = "not_found"

    VALUES = (
        PENDING,
        RESOLVED,
        ERROR,
        SKIPPED,
        NOT_FOUND,
    )

    @classmethod
    def __iter__(cls):
        yield from cls.VALUES
    
    
class MessageDetail:
    """Structured message record attached to an entity."""

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
            f"ErrorDetail(message={self.message!r}, stage={self.stage!r},  "
            f"field={self.field!r}, code={self.code!r})"
        )

class EntityManager:
    foo = 'bar'

class IdentityManager:
    def __init__(
        self,
        source_key: str,
        target_key: str,
        resolver: Union[PreparedStatement, TableLookup],
        alternate_keys: Optional[List[str]] = []
    ):
        self.source_key = source_key
        self.target_key = target_key
        self.alternate_keys = alternate_keys
        if isinstance(resolver, TableLookup):
            # Get PreparedStatement from TableLookup
            self.resolver = resolver._stmt
        elif isinstance(resolver, PreparedStatement):
            self.resolver = resolver
        else:
            raise ValueError('Resolver must be either a PreparedStatement or TableLookup')
        self.entities: Dict[Any, Record] = {}
        self._record_factory: Optional[type[Record]] = None  # lazy-created

    def _setup_record_class(self, record: Optional[Record] = None):
        if self._record_factory:
            return self._record_factory
        if record:
            alt_keys = set(self.alternate_keys) - set(record.keys())
            fields =  record.keys() + list(alt_keys) + ['_status', '_messages']
        elif self.resolver.cursor.description:
            fields = [c[0] for c in self.resolver.cursor.description]
        else:
            fields = [self.target_key, '_status', '_messages']
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
            if entity._status == EntityStatus.RESOLVED:
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
        if record:
            bind_vars = self.resolver.cursor.prepare_params(self.resolver.param_names, record)
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

    def add_message(self, source_id: str, message: Union[str, MessageDetail]):
        self.entities[source_id]['_messages'].append(message)

    def set_id(self, source_id: str, id_type: str, value:str):
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
        if self.source_key not in self._record_factory._fields and \
                self.source_key not in self._record_factory._fields_normalized:
            raise ValueError(f'Resolver must return {self.source_key} to allow batch_resolve.')
        sql = self.resolver.sql
        param_names = self.resolver.param_names
        bind_array = []
        cnt = 0
        for source_key, entity in self.entities.items():
            if entity.get('_status') in (EntityStatus.NOT_FOUND, EntityStatus.PENDING):
                params = self.resolver.cursor.prepare_params(param_names, entity)
                bind_array.append(params)
                cnt += 1
                if cnt % 1000 == 0:
                    self.resolver.cursor.executemany(sql, bind_array)
                    self._process_batch_results()
                    bind_array = []
        if bind_array:
            self.resolver.cursor.executemany(sql, bind_array)
            self._process_batch_results()

    def _process_batch_results(self):
        for rec in self.resolver.cursor.fetchall():
            source_id = rec.get(self.source_key)
            if not source_id:
                continue
            raw_entity = self._record_factory(**rec)
            entity = self.entities.get(source_id)
            entity.coalesce(raw_entity)
            if entity.get(self.target_key):
                entity['_status'] = EntityStatus.RESOLVED
            self.entities[source_id] = entity

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
            field_order = [self.target_key, '_status', '_messages']

        data = {
            "version": 1,
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
            "source_key": self.source_key,
            "target_key": self.target_key,
            "field_order": field_order,
            "entities": {
                str(source_pk): entity.to_dict(normalized=False)
                for source_pk, entity in self.entities.items()
            }
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

    def load_state(self, path: Union[str, Path]):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        self.source_key = data["source_key"]
        self.target_key = data["target_key"]

        # Re-create factory from saved field_order
        field_order = data.get("field_order")
        if field_order:
            RecordClass = type('EntityRecord', (Record,), {})
            RecordClass.set_fields(field_order)
            self._record_factory = RecordClass
        else:
            logger.warning("No field_order in saved state — using minimal fallback")
            self.resolver.execute({})
            self._setup_record_class()  # fallback to target_key + metadata

        self.entities = {}
        for source_pk, entity_data in data["entities"].items():
            entity = self._record_factory(**entity_data)
            # Ignore extra keys in JSON (safe)
            self.entities[source_pk] = entity

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
