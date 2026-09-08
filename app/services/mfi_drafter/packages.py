"""One serialization and budget boundary for all reliable MFI model calls."""
from __future__ import annotations
import json
from .reliable_contracts import MAX_PACKAGE_CHARACTERS


class PackageTooLarge(ValueError):
    def __init__(self, count, limit=MAX_PACKAGE_CHARACTERS):
        super().__init__(f"Unsplittable MFI package: {count} serialized characters exceeds {limit}; revise the work subdivision or contract configuration before Resume")
        self.character_count = count
        self.target_characters = limit


def serialized(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def ensure_message_budget(messages, schema=None):
    from app.shared.llm_observability import serialize_messages
    count = len(serialized({"messages": serialize_messages(messages), "response_schema": schema}))
    if count > MAX_PACKAGE_CHARACTERS:
        raise PackageTooLarge(count)
    return count


def bounded_groups(rows, build_package, *, maximum_items, maximum_characters=145_000):
    """Stable consecutive partition. Required evidence is rebuilt, never truncated."""
    groups, current = [], []
    for row in rows:
        candidate = [*current, row]
        if current and (len(candidate) > maximum_items or len(serialized(build_package(candidate))) > maximum_characters):
            groups.append(current)
            current = []
        current.append(row)
        count = len(serialized(build_package(current)))
        if count > maximum_characters:
            raise PackageTooLarge(count, maximum_characters)
    if current:
        groups.append(current)
    return groups
