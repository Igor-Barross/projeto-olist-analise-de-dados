import pandas as pd
from pathlib import Path
from src.utils.functions import (
    inspect_df,
    get_path_config,
    get_orders_pipeline_config
)
# RAIZ DO PROJETO
PROJECT_ROOT = Path(__file__).resolve().parents[2]


# Função para converter tipos de colunas do dataset
def convert_order_dates(df: pd.DataFrame, cols) -> pd.DataFrame:
    dt_cols = cols

    datetime_format = "%Y-%m-%d %H:%M:%S"

    for col in dt_cols:
        df[col] = pd.to_datetime(
            df[col],
            format=datetime_format,
            errors="coerce",
        )

    df["order_id"] = df["order_id"].astype("string")
    df["customer_id"] = df["customer_id"].astype("string")

    return df


# Função para analisar valores dentro de uma coluna
def inspect_col(df: pd.DataFrame, col: str):
    print("\nUnique\n")
    print(df[f"{col}"].unique())

    print("Value Counts\n")
    print(df[f"{col}"].value_counts())


# Função para padronizar a coluna oder_status
def clean_order_status(df: pd.DataFrame) -> pd.DataFrame:
    df["order_status"] = df["order_status"].str.lower()
    df["order_status"] = df["order_status"].str.strip()
    return df


# Função para chegar duplicata na coluna order_id
def remove_order_id_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    print("\nCheck duplicate\n")
    before = df.shape
    df = df.drop_duplicates(
        subset=["order_id"],
        keep='first'
    )
    after = df.shape
    print(f"Before: {before}, After: {after}")

    return df

# Função para salvar


# Função principal do pipeline de orders
def build_orders_base():
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

    # quantitdade de linhas antes da limpeza
    quantity_rows_before_clean = df.shape[0]

    # analise simples para visualização dos tipos das colunas
    inspect_df(df)

    # convertendo tipos das colunas
    df = convert_order_dates(df, pipe_cfg["columns"]["datetime"])

    # analisando coluna 'order_status'
    inspect_col(df, "order_status")

    # padronizando a coluna order_status
    df = clean_order_status(df)

    # analisando novamente a coluna 'order_status'
    inspect_col(df, "order_status")

    # verificando se existe duplicatas na coluna 'order_id'
    df = remove_order_id_duplicates(df)

    # selecionando apenas as colunas escolhidas na config
    df = df[columns_to_keep]

    # quantidade de linhas depois da limpeza
    quantity_rows_after_clean = df.shape[0]

    # Salvando o dataset limpo
    if interim_enabled:
        interim_path = interim_dir / interim_filename
        df.to_parquet(interim_path, index=False)
        print(f"Arquivo salvo com sucesso! caminho: \n {interim_path}")

        print(f'O dataset tinha antes {quantity_rows_before_clean} linhas')
        print(f'Agora tem {quantity_rows_after_clean} linhas')


if __name__ == '__main__':
    build_orders_base()
