# without-configmap

A `without` behavior source backed by a Kubernetes ConfigMap mount.

`watch_config(mount, parse)` yields the parsed config now and a freshly parsed
value every time the mount changes, watching the mount *directory* so it catches
the atomic `..data` symlink swap that projected ConfigMaps use. The ConfigMap is
mounted with a single YAML file, validated into a pydantic model. Feed it through
`without.sample` to read the latest config as a `Context`:

```python
from without import sample
from without_configmap import read_yaml_file, watch_config

source = watch_config(Path("/etc/config"), read_yaml_file(MyConfig, "config.yaml"))
async with sample(source) as config:
    config.current()  # always the latest reloaded value
```

This is the first context that actually *changes*, proving the
context-updated-by-a-stream half of the model end to end.
