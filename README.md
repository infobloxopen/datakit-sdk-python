# datakit-sdk

DataKit Python SDK — store discovery and dbt profiles generation for [DataKit](https://github.com/Infoblox-CTO/platform.data.kit).

## Install

```bash
# From GitHub release (latest)
pip install datakit-sdk@https://github.com/infobloxopen/datakit-sdk-python/releases/latest/download/datakit_sdk-0.1.0-py3-none-any.whl

# From source
pip install git+https://github.com/infobloxopen/datakit-sdk-python.git

# From local checkout
pip install -e .
```

## What it does

The `dk dbt` command in the DataKit CLI uses this package to:

1. Read `DK_STORE_DSN_*` / `DK_STORE_TYPE_*` environment variables (injected by `dk run` or the DataKit controller)
2. Generate a `profiles.yml` for dbt with the correct connection details
3. dbt then uses that profile to connect to the database

## Usage

### CLI: dk-profiles

```bash
# Generate profiles.yml from environment variables
export DK_STORE_DSN_WAREHOUSE="postgresql://user:pass@host:5432/db?schema=stage"
export DK_STORE_TYPE_WAREHOUSE="postgres"

dk-profiles generate                # writes profiles.yml to current dir
dk-profiles generate -o /app        # writes to /app/profiles.yml
dk-profiles generate --project my_project  # override project name
```

### Python API

```python
from datakit import stores

# Get a specific store
warehouse = stores.get("warehouse")
print(warehouse.dsn)   # "postgresql://user:pass@host:5432/db"
print(warehouse.type)  # "postgres"

# Discover all stores
for name, store in stores.all().items():
    print(f"{name}: {store.type} -> {store.dsn}")
```

```python
from datakit.profiles import generate_profiles

# Generate dbt profiles YAML string
yaml_str = generate_profiles("my_project")
```

## Environment Variables

The DataKit platform injects two env vars per store:

| Variable | Example | Description |
|----------|---------|-------------|
| `DK_STORE_DSN_<NAME>` | `DK_STORE_DSN_WAREHOUSE=postgresql://...` | Connection string |
| `DK_STORE_TYPE_<NAME>` | `DK_STORE_TYPE_WAREHOUSE=postgres` | Connector type |

`<NAME>` is the store name uppercased with hyphens replaced by underscores.

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

Apache-2.0
