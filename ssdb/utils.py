import yaml
from typing import Any


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