"""
Genie Observability Hub — Filtering Utilities

Provides workspace and tag-based filtering for the harvester and dashboard.
Used by 02_harvester and 06_alerts_and_retention.

Usage in a notebook:
    from filters import HarvestFilter
    hf = HarvestFilter(spark, catalog, schema)
    space_ids = hf.filter_spaces(discovered_space_ids)
"""

import json
from typing import List, Optional


class HarvestFilter:
    """
    Applies workspace and tag filters to control which spaces get harvested.

    Filter precedence:
    1. workspace_filter (from harvest_config) — restrict to specific workspace IDs
    2. space_include_tags — only harvest spaces with at least one of these tags
    3. space_exclude_tags — skip spaces with any of these tags

    If no filters are set, all discovered spaces are harvested.
    """

    def __init__(self, spark, catalog: str, schema: str):
        self.spark = spark
        self.prefix = f"{catalog}.{schema}"
        self._config_cache = {}
        self._load_config()

    def _load_config(self):
        """Load all filter config into memory."""
        try:
            rows = self.spark.sql(f"""
                SELECT config_key, config_value
                FROM {self.prefix}.harvest_config
                WHERE config_key IN (
                    'workspace_filter',
                    'space_include_tags',
                    'space_exclude_tags',
                    'space_include_ids',
                    'space_exclude_ids'
                )
            """).collect()
            for row in rows:
                self._config_cache[row.config_key] = row.config_value
        except Exception:
            pass  # Config table might not exist yet

    def get_workspace_filter(self) -> Optional[List[str]]:
        """Return list of workspace IDs to include, or None for all."""
        raw = self._config_cache.get("workspace_filter")
        if raw:
            try:
                ids = json.loads(raw)
                return ids if ids else None
            except json.JSONDecodeError:
                return [w.strip() for w in raw.split(",") if w.strip()] or None
        return None

    def get_include_tags(self) -> Optional[List[str]]:
        """Tags that spaces MUST have to be included."""
        raw = self._config_cache.get("space_include_tags")
        if raw:
            try:
                return json.loads(raw) or None
            except json.JSONDecodeError:
                return [t.strip() for t in raw.split(",") if t.strip()] or None
        return None

    def get_exclude_tags(self) -> Optional[List[str]]:
        """Tags that cause spaces to be EXCLUDED."""
        raw = self._config_cache.get("space_exclude_tags")
        if raw:
            try:
                return json.loads(raw) or None
            except json.JSONDecodeError:
                return [t.strip() for t in raw.split(",") if t.strip()] or None
        return None

    def get_explicit_includes(self) -> Optional[List[str]]:
        """Explicit space IDs to always include."""
        raw = self._config_cache.get("space_include_ids")
        if raw:
            try:
                return json.loads(raw) or None
            except json.JSONDecodeError:
                return None
        return None

    def get_explicit_excludes(self) -> Optional[List[str]]:
        """Explicit space IDs to always exclude."""
        raw = self._config_cache.get("space_exclude_ids")
        if raw:
            try:
                return json.loads(raw) or None
            except json.JSONDecodeError:
                return None
        return None

    def filter_spaces(self, space_ids: List[str]) -> List[str]:
        """
        Apply all configured filters to a list of discovered space IDs.

        Returns the filtered list of space IDs that should be harvested.
        """
        result = set(space_ids)

        # Explicit excludes
        excludes = self.get_explicit_excludes()
        if excludes:
            result -= set(excludes)

        # Tag-based exclusion
        exclude_tags = self.get_exclude_tags()
        if exclude_tags:
            tagged_spaces = self._get_spaces_with_tags(exclude_tags)
            result -= tagged_spaces

        # Tag-based inclusion (if set, ONLY include spaces with these tags)
        include_tags = self.get_include_tags()
        if include_tags:
            tagged_spaces = self._get_spaces_with_tags(include_tags)
            # Also keep explicit includes
            explicit = set(self.get_explicit_includes() or [])
            result = result.intersection(tagged_spaces | explicit)

        # Explicit includes (always added back)
        includes = self.get_explicit_includes()
        if includes:
            result |= set(includes).intersection(set(space_ids))

        return list(result)

    def get_audit_workspace_clause(self) -> str:
        """
        Returns a SQL WHERE clause fragment for workspace filtering in audit queries.
        Returns empty string if no filter is set.
        """
        ws_filter = self.get_workspace_filter()
        if ws_filter:
            ids_str = ", ".join(f"'{w}'" for w in ws_filter)
            return f"AND workspace_id IN ({ids_str})"
        return ""

    def _get_spaces_with_tags(self, tags: List[str]) -> set:
        """Look up which spaces have any of the given tags."""
        tags_str = ", ".join(f"'{t}'" for t in tags)
        try:
            rows = self.spark.sql(f"""
                SELECT DISTINCT space_id
                FROM {self.prefix}.space_tags
                WHERE tag IN ({tags_str})
            """).collect()
            return {row.space_id for row in rows}
        except Exception:
            return set()

    def describe(self) -> str:
        """Human-readable description of active filters."""
        parts = []
        ws = self.get_workspace_filter()
        if ws:
            parts.append(f"Workspaces: {ws}")
        inc_tags = self.get_include_tags()
        if inc_tags:
            parts.append(f"Include tags: {inc_tags}")
        exc_tags = self.get_exclude_tags()
        if exc_tags:
            parts.append(f"Exclude tags: {exc_tags}")
        inc_ids = self.get_explicit_includes()
        if inc_ids:
            parts.append(f"Force include: {inc_ids}")
        exc_ids = self.get_explicit_excludes()
        if exc_ids:
            parts.append(f"Force exclude: {exc_ids}")
        return " | ".join(parts) if parts else "No filters (harvest all)"
