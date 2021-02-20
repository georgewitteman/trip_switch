from functools import wraps
from enum import Enum
from typing import Any, Callable
from time import time
from math import floor


class State(Enum):
    OPEN = 'open'
    CLOSED = 'closed'
    HALF_OPEN = 'half_open'


class LeakyBucket:
    def __init__(self, drops_per_time_unit: int, seconds_per_time_unit: int) -> None:
        self._drops_per_time_unit = drops_per_time_unit
        self._seconds_per_time_unit = seconds_per_time_unit
        self._max_drops = drops_per_time_unit
        self._drops = 0
        self._last_check = time()

    def has_capacity(self) -> bool:
        now = time()
        time_passed = now - self._last_check

        water_leaked = time_passed * (self._drops_per_time_unit / self._seconds_per_time_unit)
        drops_leaked = floor(water_leaked)
        self._drops = max(0, self._drops - drops_leaked)
        self._last_check = now - (water_leaked - drops_leaked) * (self._drops_per_time_unit / self._seconds_per_time_unit)

        if self._drops + 1 <= self._max_drops:
            return True

        return False

    def add_water(self) -> None:
        if not self.has_capacity():
            raise Exception('Bucket is full')

        self._drops = self._drops + 1
        print(self._drops)

    def reset(self) -> None:
        self._last_check = time()
        self._drops = 0


class TrippedSwitch(Exception):
    pass

class TripSwitch:
    def __init__(
        self,
        failures_per_unit: int = 10,
        seconds_per_unit: int = 60,
        reset_timeout_s: int = 60,
    ) -> None:
        self.reset_timeout_s = reset_timeout_s

        self._state = State.CLOSED
        self._state_changed = time()
        self._leaky_bucket = LeakyBucket(failures_per_unit - 1, seconds_per_unit)

    @property
    def state(self) -> State:
        return self._state

    def set_state(self, state: State) -> None:
        self._state = state
        self._state_changed = time()

    def time_in_state(self) -> time:
        return time() - self._state_changed

    def call(self, func, *args, **kwargs):
        if self.state == State.OPEN:
            if self.time_in_state() > self.reset_timeout_s:
                self.set_state(State.HALF_OPEN)
            else:
                raise TrippedSwitch()

        try:
            result = func(*args, **kwargs)
        except Exception:
            if self.state == State.CLOSED:
                if self._leaky_bucket.has_capacity():
                    self._leaky_bucket.add_water()
                else:
                    self.set_state(State.OPEN)
            elif self.state == State.HALF_OPEN:
                self.set_state(State.OPEN)

            raise
        else:
            if self.state == State.HALF_OPEN:
                self._leaky_bucket.reset()
                self.set_state(State.CLOSED)
            return result


if __name__ == '__main__':
    from datetime import datetime
    circuit_breaker = TripSwitch(failures_per_unit=2, seconds_per_unit=10, reset_timeout_s=5)
    while True:
        def test_func(should_raise: bool) -> None:
            if should_raise:
                raise Exception('test exception')

        try:
            if input(f'{datetime.now()} Should raise? ') == 'y':
                circuit_breaker.call(test_func, True)
            else:
                circuit_breaker.call(test_func, False)
        except TrippedSwitch:
            print('TRIPPED SWITCH!')
        except Exception as e:
            if str(e) != 'test exception':
                raise
