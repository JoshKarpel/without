# without-configmap

A `without` behavior source backed by a Kubernetes ConfigMap mount: the first
context that actually *changes*, proving the context-updated-by-a-stream half of
the model end to end. Where [`without-env`](without-env.md) loads a static value
once, this one re-parses on every mount change. See the
[`without_configmap` API reference](../reference/without_configmap.md) for the
full surface.

`watch_config(mount, parse)` is a `Stream` that yields the parsed config now and a
freshly parsed value every time the mount changes. It watches the mount
*directory*, not the file, so it catches the atomic `..data` symlink swap that
[projected ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/#mounted-configmaps-are-updated-automatically)
use (a naive watch on the file alone misses those updates). `read_yaml_file(Model,
"config.yaml")` is the usual `parse`: it reads one YAML file from the mount and
validates it into a pydantic model.

Feed the stream through `without.sample` to read the latest value as a `Context`:

```python
from pathlib import Path

from pydantic import BaseModel
from without import sample
from without_configmap import read_yaml_file, watch_config


class MyConfig(BaseModel):
    model_config = {"frozen": True}
    default_mode: str
    max_bytes: int


source = watch_config(Path("/etc/config"), read_yaml_file(MyConfig, "config.yaml"))
async with sample(source) as config:
    config.current()       # always the latest reloaded value, never blocks
    await config.updated() # block until the next reload lands, then return it
```

`sample` reads the first value eagerly, so the context is never "not ready"; a
background task keeps it current for the life of the `with` block. `current`
reads the latest value without blocking; `updated` is the deterministic
counterpart that waits for the next reload to land (it inherits latest-wins, so
it signals "the config moved on" rather than replaying every intermediate value).
A reload whose YAML fails validation raises from the watch loop rather than
silently serving a stale or partial value. Snapshot `config.current()` once per
request or connection so a mid-flight reload does not change a value an in-progress
handler already read.
