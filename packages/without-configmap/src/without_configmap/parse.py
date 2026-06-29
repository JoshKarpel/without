from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import yaml
from pydantic import BaseModel


def read_yaml_file[ModelT: BaseModel](model_type: type[ModelT], file_name: str) -> Callable[[Path], ModelT]:
    """Parse a single YAML file from the mount into a validated model.

    The ConfigMap is mounted with one file (e.g. `config.yaml`) holding a YAML
    mapping. This reads `mount / file_name`, loads it, and validates it into
    `model_type` at the boundary, so missing keys fall back to declared model
    defaults and the rest of the system sees an already-valid value.
    """

    def parse(mount: Path) -> ModelT:
        contents = yaml.safe_load((mount / file_name).read_text())
        return model_type.model_validate(contents)

    return parse
