# dbtk/etl/manager.py

"""
Orchestration tools for multi-stage, resumable ETL processes.

EntityManager provides lightweight, incremental entity orchestration
for imports where a reliable primary key exists in source data.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from ..cursors import PreparedStatement
from .transforms.database import TableLookup

logger = logging.getLogger(__name__)


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
            f"ErrorDetail(message={self.message!r}, stage={self.stage!r},  "
            f"field={self.field!r}, code={self.code!r})"
        )


class EntityManager:
    """
    Incremental entity manager for resumable, multi-stage imports.

    Optimized for workflows where every inbound record has a reliable
    primary key (e.g., CRM application ID), and secondary keys plus
    enrichment data are resolved on-demand.

    Parameters
    ----------
    primary_key : str
        Name of the reliable source key (e.g., "crm_id")
    secondary_keys : List[str]
        Keys to resolve and index (e.g., ["recruit_id", "sis_id"])

    Examples
    --------
    >>> manager = EntityManager(primary_key="crm_id", secondary_keys=["recruit_id", "sis_id"])
    >>> stmt = cursor.prepare_file("sql/resolve_person.sql")
    >>> manager.set_main_resolver(stmt)
    >>>
    >>> for row in reader:
    ...     entity = manager.process_row(row["ApplicationID"])
    ...     print(f"Processing {entity.get('full_name', entity['crm_id'])}")
    >>>
    >>> manager.save("import_state.json")
    """

    def __init__(
        self,
        primary_key: str,
        secondary_keys: List[str],
    ):
        self.primary_key = primary_key
        self.secondary_keys = secondary_keys or []
        self.key_types = [primary_key] + self.secondary_keys

        # primary_value -> entity dict
        self.entities: Dict[Any, Dict[str, Any]] = {}

        # secondary_value -> primary_value
        self._secondary_index: Dict[str, Dict[Any, Any]] = {
            k: {} for k in self.secondary_keys
        }

        # from_key -> resolver
        self._resolvers: Dict[str, Any] = {}

    # =================================================================
    # Hot path
    # =================================================================
    def process_row(
        self,
        primary_value: Any,
        *,
        auto_resolve: bool = True,
        status: str = "pending",
    ) -> Dict[str, Any]:
        """Core method: get/create entity and optionally resolve."""
        entity = self.entities.get(primary_value)

        if entity is None:
            entity = {
                self.primary_key: primary_value,
                "status": status,
                "errors": [],
            }
            self.entities[primary_value] = entity

        if auto_resolve:
            self.resolve(entity)

        return entity

    # =================================================================
    # Resolver management
    # =================================================================
    def set_main_resolver(
        self,
        resolver: Union[TableLookup, PreparedStatement],
        from_keys: Optional[List[str]] = None,
    ) -> None:
        """Set the main resolver (most common: from primary_key)."""
        if from_keys is None:
            from_keys = [self.primary_key]

        for key in from_keys:
            if key not in self.key_types:
                raise ValueError(f"Invalid from_key for resolver: {key}")
            self._resolvers[key] = resolver

    def add_fallback_resolver(
        self,
        from_key: str,
        resolver: Union[TableLookup, PreparedStatement],
    ) -> None:
        """Add resolver for rare secondary-to-secondary cases."""
        if from_key not in self.secondary_keys:
            raise ValueError(f"Fallback from_key must be secondary: {from_key}")
        self._resolvers[from_key] = resolver

    # =================================================================
    # Resolution
    # =================================================================
    def resolve(self, entity: Dict[str, Any]) -> bool:
        """Resolve secondary keys and enrich entity."""
        updated = False
        primary_val = entity.get(self.primary_key)

        for key in self.key_types:
            if entity.get(key) is None:
                continue
            resolver = self._resolvers.get(key)
            if resolver is None:
                continue

            if self._apply_resolver(entity, key, resolver, primary_val):
                updated = True

        if updated:
            entity["status"] = "resolved"

        return updated

    def _apply_resolver(
        self,
        entity: Dict[str, Any],
        using_key: str,
        resolver: Union[TableLookup, PreparedStatement],
        primary_val: Any,
    ) -> bool:
        bind_vars = {using_key: entity[using_key]}

        if isinstance(resolver, TableLookup):
            result = resolver(bind_vars)
        else:
            resolver.execute(bind_vars)
            result = resolver.fetchone()

        if not result:
            return False

        # Normalize to dict
        if hasattr(result, "to_dict"):
            row_dict = result.to_dict()
        elif hasattr(result, "_asdict"):
            row_dict = result._asdict()
        else:
            row_dict = dict(result)

        updated = False

        # Tracked secondary keys
        for sk in self.secondary_keys:
            if sk == using_key:
                continue
            new_val = row_dict.get(sk)
            if new_val is not None and entity.get(sk) != new_val:
                old_val = entity.get(sk)
                entity[sk] = new_val
                self._secondary_index[sk][new_val] = primary_val
                if old_val is not None:
                    self._secondary_index[sk].pop(old_val, None)
                updated = True

        # Enrichment columns
        for col, val in row_dict.items():
            if col not in self.key_types and entity.get(col) != val:
                entity[col] = val
                updated = True

        return updated

    # =================================================================
    # Lookups
    # =================================================================
    def get_by_primary(self, primary_value: Any) -> Optional[Dict[str, Any]]:
        return self.entities.get(primary_value)

    def get_by_secondary(self, secondary_key: str, value: Any) -> Optional[Dict[str, Any]]:
        if secondary_key not in self.secondary_keys:
            raise ValueError(f"Unknown secondary key: {secondary_key}")
        primary_val = self._secondary_index[secondary_key].get(value)
        return self.entities.get(primary_val) if primary_val is not None else None

    # =================================================================
    # Errors
    # =================================================================
    def add_error(
        self,
        entity: Dict[str, Any],
        message: str = "",
        stage: Optional[str] = None,
        field: Optional[str] = None,
        code: Optional[str] = None,
    ) -> None:
        entity.setdefault("errors", []).append(
            ErrorDetail(stage=stage, field=field, message=message, code=code)
        )

    # =================================================================
    # Iteration
    # =================================================================
    def iter_entities(self) -> Iterator[Dict[str, Any]]:
        yield from self.entities.values()

    def iter_with_errors(self, stage: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        for entity in self.entities.values():
            errors = [e for e in entity.get("errors", []) if stage is None or e.stage == stage]
            if errors:
                yield entity

    # =================================================================
    # Persistence
    # =================================================================
    def save(self, path: Union[str, Path]) -> None:
        serializable = {
            primary: {
                **entity,
                "errors": [e.__dict__ for e in entity.get("errors", [])],
            }
            for primary, entity in self.entities.items()
        }
        path_obj = Path(path)
        path_obj.write_text(json.dumps(serializable, indent=2, default=str))
        logger.info(f"EntityManager state saved to {path_obj} ({len(self.entities)} entities)")

    @classmethod
    def load(
        cls,
        path: Union[str, Path],
        primary_key: str,
        secondary_keys: List[str],
    ) -> "EntityManager":
        path_obj = Path(path)
        data = json.loads(path_obj.read_text())

        manager = cls(primary_key=primary_key, secondary_keys=secondary_keys)

        for primary_val, entity_data in data.items():
            entity = {
                **entity_data,
                "errors": [ErrorDetail(**e) for e in entity_data.get("errors", [])],
            }
            manager.entities[primary_val] = entity

            for sk in secondary_keys:
                if (val := entity.get(sk)) is not None:
                    manager._secondary_index[sk][val] = primary_val

        logger.info(f"EntityManager state loaded from {path_obj} ({len(manager.entities)} entities)")
        return manager