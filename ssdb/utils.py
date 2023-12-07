import yaml
from typing import Any
from math import modf
from datetime import datetime, timedelta, timezone


class Yaml:
    TAG = 'tag:yaml.org,2002:str'

    @classmethod
    def dump(cls, path: str, data: dict[str, Any]):
        yaml.add_representer(str, cls.represent_str)
        with open(path, 'w')as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False)

    @classmethod
    def represent_str(cls, dumper: yaml.Dumper, instance: str) -> yaml.ScalarNode:
        if '\n' in instance:
            return dumper.represent_scalar(cls.TAG, instance, style='|')
        else:
            return dumper.represent_scalar(cls.TAG, instance)


class SerialNumber:
    @staticmethod
    def get_basedt(tz=timezone.utc) -> datetime:
        return datetime(1899, 12, 30, 0, 0, tzinfo=tz)

    @classmethod
    def to_datetime(cls, num: float) -> int:
        days, fraction = modf(num)
        return cls.get_basedt() + timedelta(days=days, hours=24*fraction)

    @classmethod
    def from_datetime(cls, date: datetime) -> float:
        delta = date - cls.get_basedt(date.tzinfo)
        days = float(delta.days)
        fraction = float(delta.seconds / (60*60*24))
        return days+fraction

    @classmethod
    def from_timestamp(cls, timestamp: int, tz=timezone.utc) -> float:
        delta = datetime.fromtimestamp(timestamp, tz=tz) - cls.get_basedt(tz)
        days = float(delta.days)
        fraction = float(delta.seconds / (60*60*24))
        return days+fraction