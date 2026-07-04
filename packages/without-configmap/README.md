# without-configmap

A `without` behavior source backed by a Kubernetes ConfigMap mount: the first
context that actually *changes*, proving the context-updated-by-a-stream half of
the model end to end. Where [`without-env`](../without-env) loads a static value
once, this one re-parses on every mount change.

`watch_config(mount, parse)` is a `Stream` that yields the parsed config now and a
freshly parsed value every time the mount changes. It watches the mount
*directory*, not the file, so it catches the atomic `..data` symlink swap that
[projected ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/#mounted-configmaps-are-updated-automatically)
use. Feed the stream through `without.sample` to read the latest value as a
`Context`:

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

See the
[`without-configmap` guide](https://without.help/guides/without-configmap/)
(with the [API reference](https://without.help/reference/without_configmap/))
for the full surface.
