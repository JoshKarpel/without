# without-env

A `without` `Context` backed by environment variables.

`EnvContext.load(MySettings)` parses the environment once into a typed
`pydantic-settings` model at the boundary, then hands the validated value to
processors via `current()`. This is the simplest possible context: a static one,
loaded at startup and never changed.
