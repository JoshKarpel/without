from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from collections.abc import Callable

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from without_env import EnvContext
from without_streams import Context
from without_streams import Processor
from without_streams import Stream

from integration.transform.core import TransformConfig
from integration.transform.core import transform

# A second shell over the same `transform.core`, to make the portability claim
# concrete: the ASGI app drives the core from a `without-configmap` `Context` over
# HTTP, and this drives the same core from a `without-env` `Context` over stdin.
# Neither the core nor its `TransformConfig` knows which shell runs it, so the only
# thing that changes between the two is the I/O at the edge and where config comes
# from.


class CliSettings(BaseSettings):
    """
    The CLI's config, parsed from the environment at the boundary.

    The env-backed analogue of the ASGI app's `Settings`, composed the same way:
    `transform` is the domain `TransformConfig` (the only thing the core sees,
    handed over as `settings.transform`), and `prompt` is this shell's own knob
    (what to print before each line), the CLI's counterpart to the HTTP shell's
    `max_bytes`. The core never sees `prompt`, the same boundary the ASGI split
    draws. Nested fields read from env through the `__` delimiter, e.g.
    `TEXT_TRANSFORM__DEFAULT_MODE` and `TEXT_PROMPT`.
    """

    model_config = SettingsConfigDict(env_prefix="TEXT_", env_nested_delimiter="__")

    transform: TransformConfig = TransformConfig()
    prompt: str = ">"


def transform_lines(config: TransformConfig) -> Processor[str, str]:
    """
    The CLI's pure processor: map each input line to its transform under `config`.

    A `Processor[str, str]` built from a config snapshot, the same shape as the
    ASGI handlers (`buffered` builds one from the per-request state). It calls the
    very `transform` the HTTP handler calls, with no requested-mode override since
    the CLI has no query string. Pure, so a test drives it with a fixed line
    stream and reads the outputs back with `without.collect`.
    """

    async def processor(inputs: Stream[str]) -> AsyncIterator[str]:
        async for line in inputs:
            yield transform(config, None, line)

    return processor


async def stdin_lines(prompt: str) -> AsyncIterator[str]:
    """
    Stdin as a `Stream` of lines, writing `prompt` before each read.

    A source that touches the world, the CLI's counterpart to the ASGI inbound
    stream: it solicits each line by writing the prompt, then reads. The blocking
    read runs in a thread so the loop stays free; EOF (Ctrl-D) ends the stream,
    the same closable-stream signal the connection streams use.
    """
    while True:
        sys.stdout.write(prompt)
        sys.stdout.flush()
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            sys.stdout.write("\n")
            return
        yield line.rstrip("\n")


async def serve(config: Context[CliSettings], lines: Stream[str], emit: Callable[[str], None]) -> None:
    """
    Run the CLI: transform each input line and emit the result.

    The imperative shell, with its I/O injected so it stays testable: `main`
    passes the prompting stdin source and `print`, a test passes a fixed
    `stream_from_iterable(...)` and a list's `append`. It snapshots the config once (the CLI's
    whole session is one connection, like the ASGI app's per-request snapshot)
    and runs the processor built from the domain half over the lines.
    """
    process = transform_lines(config.current().transform)
    async for output in process(lines):
        emit(output)


def main() -> None:
    config = EnvContext.load(CliSettings)
    asyncio.run(serve(config, stdin_lines(config.current().prompt), print))


if __name__ == "__main__":  # pragma: no cover - module entrypoint, exercised by running the CLI, not by tests
    main()
