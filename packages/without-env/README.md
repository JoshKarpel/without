# without-env

A `without` `Context` backed by environment variables: the simplest possible
context, a static one loaded at startup and never changed.

`EnvContext.load(MySettings)` reads the environment once, at the boundary, into a
validated [`pydantic-settings`](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
model (parse, don't validate), then hands that immutable value to processors via
`current()`. Because it never changes, a reloading source (a watched file, a
ConfigMap mount) is a separate plugin; see
[`without-configmap`](../without-configmap) for the context-updated-by-a-stream
case.

```python
from pydantic_settings import BaseSettings
from without_env import EnvContext


class Settings(BaseSettings):
    default_mode: str = "upper"
    max_bytes: int = 1_048_576


config = EnvContext.load(Settings)   # reads os.environ once, validates
config.current().default_mode        # the parsed value, same every call
```

See the
[`without-env` guide](https://without.help/without-env/)
(with the [API reference](https://without.help/reference/without_env/))
for the full surface.
