from pathlib import Path
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# Visualização simples de dataframes
def inspect_df(df: pd.DataFrame):
    print('\nshape:\n')
    print(df.shape)

    print('\nColumns type:\n')
    print(df.dtypes)

    print('\nCheck if has Null\n')
    print(df.isna().sum())


# Carregar configs YAML
def load_yaml_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_path_config() -> dict:
    config_path = PROJECT_ROOT / "config" / "paths.yaml"
    return load_yaml_config(config_path)


def get_orders_pipeline_config() -> dict:
    config_path = PROJECT_ROOT / "config" / "pipeline_orders.yaml"
    return load_yaml_config(config_path)


if __name__ == '__main__':
    ...
