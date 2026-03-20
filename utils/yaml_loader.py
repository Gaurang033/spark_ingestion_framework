# utils/yaml_loader.py

import yaml

_CONFIG_CACHE = {}


def load_config(path):
    if path in _CONFIG_CACHE:
        return _CONFIG_CACHE[path]

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    _CONFIG_CACHE[path] = config
    return config
