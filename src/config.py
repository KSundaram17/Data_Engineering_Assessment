pip install pyyaml

import yaml

def load_config(path="config_local.yaml"):
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config