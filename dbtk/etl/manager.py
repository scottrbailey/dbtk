# dbtk/etl/manager.py

"""
Orchestration tools for multi-stage, resumable ETL processes.

EntityManager provides lightweight, incremental entity orchestration
for imports where a reliable primary identifier exists in source data.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from ..cursors import PreparedStatement
from .transforms.database import TableLookup

logger = logging.getLogger(__name__)


class EntityStatus:
    """Status values for entity resolution lifecycle."""
    PENDING = "pending"
    RESOLVED = "resolved"
    ERROR = "error"
    SKIPPED = "skipped"


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


class EntityManager:
    """
    Incremental entity manager for resumable, multi-stage imports.

    Optimized for workflows where every inbound record has a reliable
    primary identifier (e.g., CRM application ID), and secondary identifiers
    plus enrichment data are resolved on-demand.

    Parameters
    ----------
    primary_id : str
        Name of the reliable source identifier (e.g., "crm_id")
    secondary_ids : List[str]
        Identifiers to resolve and index (e.g., ["recruit_id", "sis_id"])

    Examples
    --------
    >>> manager = EntityManager(primary_id="crm_id", secondary_ids=["recruit_id", "sis_id"])
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
        primary_id: str,
        secondary_ids: List[str],
    ):
        self.primary_id = primary_id
        self.secondary_ids = secondary_ids or []
        self.id_types = [primary_id] + self.secondary_ids

        # primary_value -> entity dict
        self.entities: Dict[Any, Dict[str, Any]] = {}

        # secondary_value -> primary_value
        self._secondary_index: Dict[str, Dict[Any, Any]] = {
            sid: {} for sid in self.secondary_ids
        }

        # from_id -> resolver
        self._resolvers: Dict[str, Any] = {}

    # =================================================================
    # Hot path
    # =================================================================
    def process_row(
        self,
        primary_value: Any,
        *,
        auto_resolve: bool = True,
        status: str = EntityStatus.PENDING,
    ) -> Dict[str, Any]:
        """Core method: get/create entity and optionally resolve."""
        entity = self.entities.get(primary_value)

        if entity is None:
            entity = {
                self.primary_id: primary_value,
                "status": status,
                "errors": [],
                "_resolved_ids": set(),
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
        from_ids: Optional[List[str]] = None,
    ) -> None:
        """Set the main resolver (most common: from primary_id)."""
        if from_ids is None:
            from_ids = [self.primary_id]

        for id_name in from_ids:
            if id_name not in self.id_types:
                raise ValueError(f"Invalid from_id for resolver: {id_name}")
            self._resolvers[id_name] = resolver

    def add_fallback_resolver(
        self,
        from_id: str,
        resolver: Union[TableLookup, PreparedStatement],
    ) -> None:
        """Add resolver for rare secondary-to-secondary cases."""
        if from_id not in self.secondary_ids:
            raise ValueError(f"Fallback from_id must be secondary: {from_id}")
        self._resolvers[from_id] = resolver

    # =================================================================
    # Resolution
    # =================================================================
    def resolve(self, entity: Dict[str, Any]) -> bool:
        """Resolve secondary identifiers and enrich entity."""
        updated = False
        primary_val = entity.get(self.primary_id)
        resolved_ids = entity.setdefault("_resolved_ids", set())

        for id_name in self.id_types:
            # Early exit if all ids already resolved
            if all(entity.get(i) is not None for i in self.id_types):
                break

            # Skip if already resolved
            if id_name in resolved_ids:
                continue
            if entity.get(id_name) is None:
                continue
            resolver = self._resolvers.get(id_name)
            if resolver is None:
                continue

            if self._apply_resolver(entity, id_name, resolver, primary_val):
                updated = True
                resolved_ids.add(id_name)

        if updated:
            entity["status"] = EntityStatus.RESOLVED

        return updated

    def _apply_resolver(
        self,
        entity: Dict[str, Any],
        using_id: str,
        resolver: Union[TableLookup, PreparedStatement],
        primary_val: Any,
    ) -> bool:
        try:
            bind_vars = {using_id: entity[using_id]}

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

            # Tracked secondary identifiers
            for sid in self.secondary_ids:
                if sid == using_id:
                    continue
                new_val = row_dict.get(sid)
                if new_val is not None and entity.get(sid) != new_val:
                    old_val = entity.get(sid)

                    # Detect conflicts: same secondary value pointing to different primary
                    existing_primary = self._secondary_index[sid].get(new_val)
                    if existing_primary and existing_primary != primary_val:
                        raise ValueError(
                            f"Secondary ID conflict: {sid}={new_val} already maps to "
                            f"{existing_primary}, cannot also map to {primary_val}"
                        )

                    entity[sid] = new_val
                    self._secondary_index[sid][new_val] = primary_val
                    if old_val is not None:
                        self._secondary_index[sid].pop(old_val, None)
                    updated = True

            # Enrichment columns
            for col, val in row_dict.items():
                if col not in self.id_types and entity.get(col) != val:
                    entity[col] = val
                    updated = True

            return updated

        except Exception as e:
            logger.error(f"Resolver failed for {using_id}={entity.get(using_id)}: {e}")
            raise

    # =================================================================
    # Lookups
    # =================================================================
    def get_by_primary(self, primary_value: Any) -> Optional[Dict[str, Any]]:
        return self.entities.get(primary_value)

    def get_by_secondary(self, secondary_id: str, value: Any) -> Optional[Dict[str, Any]]:
        if secondary_id not in self.secondary_ids:
            raise ValueError(f"Unknown secondary_id: {secondary_id}")
        primary_val = self._secondary_index[secondary_id].get(value)
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
    # Metrics
    # =================================================================
    def summary(self) -> Dict[str, int]:
        """Return processing summary statistics."""
        return {
            "total": len(self.entities),
            "resolved": sum(1 for e in self.entities.values() if e["status"] == EntityStatus.RESOLVED),
            "pending": sum(1 for e in self.entities.values() if e["status"] == EntityStatus.PENDING),
            "error": sum(1 for e in self.entities.values() if e["status"] == EntityStatus.ERROR),
            "skipped": sum(1 for e in self.entities.values() if e["status"] == EntityStatus.SKIPPED),
            "with_errors": sum(1 for e in self.entities.values() if e.get("errors")),
        }

    # =================================================================
    # Persistence
    # =================================================================
    def save(self, path: Union[str, Path]) -> None:
        serializable = {
            primary: {
                **entity,
                "errors": [e.__dict__ for e in entity.get("errors", [])],
                "_resolved_ids": list(entity.get("_resolved_ids", set())),
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
        primary_id: str,
        secondary_ids: List[str],
    ) -> "EntityManager":
        path_obj = Path(path)
        data = json.loads(path_obj.read_text())

        manager = cls(primary_id=primary_id, secondary_ids=secondary_ids)

        for primary_val, entity_data in data.items():
            entity = {
                **entity_data,
                "errors": [ErrorDetail(**e) for e in entity_data.get("errors", [])],
                "_resolved_ids": set(entity_data.get("_resolved_ids", [])),
            }
            manager.entities[primary_val] = entity

            for sid in secondary_ids:
                if (val := entity.get(sid)) is not None:
                    manager._secondary_index[sid][val] = primary_val

        logger.info(f"EntityManager state loaded from {path_obj} ({len(manager.entities)} entities)")
        return manager
