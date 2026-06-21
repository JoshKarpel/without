from __future__ import annotations

from dataclasses import dataclass

from pydantic_settings import BaseSettings


@dataclass(frozen=True, slots=True)
class EnvContext[SettingsT: BaseSettings]:
    """A static `without.Context` whose value is parsed from the environment.

    The environment is read once, at the boundary, into a validated settings
    value (parse, don't validate). The value is then immutable: `current` always
    returns the same thing, so this models config that does not change for the
    life of the process. A reloading variant belongs in a separate plugin that
    watches a source and updates a held value.
    """

    settings: SettingsT

    @classmethod
    def load(cls, settings_type: type[SettingsT]) -> EnvContext[SettingsT]:
        return cls(settings=settings_type())

    def current(self) -> SettingsT:
        return self.settings
