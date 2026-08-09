from __future__ import annotations

from datetime import timedelta
from math import ceil

# Every duration this package sends crosses the wire as a whole number of Redis's own
# units, and every one of those numbers has a zero that means something other than "no
# time at all". `EXPIRE key 0` deletes the key. `BLOCK 0` blocks forever. A lease of zero
# milliseconds writes a claim that has already lapsed and a delivery that is immediately
# visible to everyone. So rounding *down* into the unit does not shorten a duration, it
# swaps it for a sentinel, and it does so exactly where a caller was most careful: a
# duration `check_duration` has just certified as positive is the one that turns into a
# zero here.
#
# Rounding up is what closes that. The smallest positive duration a store can honour is
# one of its units, so a sub-unit duration becomes one unit rather than none, and nothing
# a caller can pass produces a sentinel by accident.
#
# A genuine zero still survives it, which is the other half of why this is `ceil` rather
# than a floor of one: `reclaim`'s `idle` is a threshold rather than an interval, and zero
# there means "take over anything outstanding, however recently it was delivered".
#
# These are where a `timedelta` becomes a number this store can hold, so they are the
# parse and not a step before one: the stores keep what comes out of here (`lease_ms`,
# `ttl_seconds`, computed once at construction) rather than re-deriving it per call, and
# because the conversion is *total* there is no duration left over for anything to have to
# check. Rounding up is what makes it total. A rule that refused a sub-unit duration
# instead would put the shortest wait a caller can express back in the hands of a
# validator, and would have to be stated separately at every place one enters, which is
# how the sentinel got in.

SECOND = timedelta(seconds=1)
MILLISECOND = timedelta(milliseconds=1)


def milliseconds(duration: timedelta) -> int:
    """`duration` as whole milliseconds, never rounding a positive one down to zero."""
    return ceil(duration / MILLISECOND)


def seconds(duration: timedelta) -> int:
    """`duration` as whole seconds, never rounding a positive one down to zero."""
    return ceil(duration / SECOND)
