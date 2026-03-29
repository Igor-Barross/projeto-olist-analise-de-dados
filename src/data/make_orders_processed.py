from pathlib import Path
from src.utils.functions import (
    get_path_config,
    get_orders_pipeline_config,
)
import pandas as pd
import yaml
# flake8: noqa: F841

# RAIZ DO PROJETO
PROJECT_ROOT = Path(__file__).parents[2]


def add_order_date_flags(df: pd.DataFrame) -> pd.DataFrame:
    # 1º incoerência: Entrega ao cliente antes da entrega à transportadora
    cond_delivered_before_carrier = (
        df['order_delivered_customer_date'] <
        df['order_delivered_carrier_date']
    )

    df['is_delivered_before_carrier'] = cond_delivered_before_carrier

    # 2º incoerência: Entrega ao cliente antes da compra
    cond_delivered_before_purchase = (
        df['order_approved_at'] <
        df['order_delivered_costumer_date']
    )

    df['is_delivered_before_purchase'] = cond_delivered_before_purchase

    # 3º incoerência: Pedidos cancelados, mas com data de entrega preenchida
    filter_order_canceled = (df["order_status"] == "canceled")
    filter_date_delivered = df["order_delivered_customer_date"].notna()

    df['is_delivered_before_carrier']
    # 1º incoerência: Entrega ao cliente antes da entrega à transportadora
    cond_delivered_before_carrier = (
        df['order_delivered_customer_date'] <
        df['order_delivered_carrier_date']
    )

    df['is_delivered_before_carrier']
    # 1º incoerência: Entrega ao cliente antes da entrega à transportadora
    cond_delivered_before_carrier = (
        df['order_delivered_customer_date'] <
        df['order_delivered_carrier_date']
    )

    df['is_delivered_before_carrier']

    return df


# Função principal do pipeline orders processed
def build_orders_processeced():
    paths_cfg = get_path_config()
    pipe_cfg = get_orders_pipeline_config()

    # caminhos base (paths).yaml
    raw_dir = PROJECT_ROOT / paths_cfg["paths"]["data_raw"]
    interim_dir = PROJECT_ROOT / paths_cfg["paths"]["data_interim"]
    processed_dir = PROJECT_ROOT / paths_cfg["paths"]["data_processed"]

    interim_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # parâmetros do pipeline (pipeline_orders.yaml)
    raw_filename = pipe_cfg["input"]["raw_filename"]
    raw_sep = pipe_cfg["input"].get("raw_sep", ",")
    raw_encoding = pipe_cfg["input"].get("raw_encoding", "utf-8")

    interim_enabled = pipe_cfg["interim"].get("enabled", True)
    interim_filename = pipe_cfg["interim"]["filename"]

    columns_to_keep: list = pipe_cfg["columns"]["keep"]

    # ler dado bruto
    raw_path = raw_dir / raw_filename

    df: pd.DataFrame = pd.read_csv(
        raw_path, sep=raw_sep, encoding=raw_encoding)
