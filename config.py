import json
import os


CONFIG_PATH = "config.json"
DEFAULT = {
"backoff_base": 2,
"default_max_retries": 3,
"worker_count": 1
}




def load_config():
if not os.path.exists(CONFIG_PATH):
save_config(DEFAULT)
return DEFAULT
with open(CONFIG_PATH, "r") as f:
return json.load(f)




def save_config(cfg: dict):
with open(CONFIG_PATH, "w") as f:
json.dump(cfg, f, indent=2)
