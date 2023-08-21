"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.12
"""
# Libs
import datetime
from typing import Any, Dict

import pandas as pd
import yfinance as yf
from binance_historical_data import BinanceDataDumper
from finta import TA


# Nodes
def hist_data_download(params: Dict[str, Any]) -> bool:

    if params["is_update_data"]:

        for data_frequency in params["data_frequencies"]:

            data_dumper = BinanceDataDumper(
                path_dir_where_to_dump=params["path_dir"],
                data_type="klines",
                data_frequency=data_frequency,
            )

            data_dumper.dump_data(
                tickers=params["tickers"],
                date_start=datetime.date(**params["date_start"]),
                date_end=datetime.date(**params["date_end"]),
                is_to_update_existing=False,
            )

    return params["is_update_data"]


def ext_data_download(params: Dict[str, Any]) -> pd.DataFrame:

    data = (
        yf.download(  # or pdr.get_data_yahoo(...
            # tickers list or string as well
            tickers=params["tickers"],
            # use "period" instead of start/end
            # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            # (optional, default is '1mo')
            period="max",
            # fetch data by interval (including intraday if period < 60 days)
            # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            # (optional, default is '1d')
            interval="1d",
            # Whether to ignore timezone when aligning ticker data from
            # different timezones. Default is True. False may be useful for
            # minute/hourly data.
            ignore_tz=False,
            # group by ticker (to access via data['SPY'])
            # (optional, default is 'column')
            group_by="ticker",
            # adjust all OHLC automatically
            # (optional, default is False)
            auto_adjust=True,
            # download pre/post regular market hours data
            # (optional, default is False)
            prepost=True,
            # use threads for mass downloading? (True/False/Integer)
            # (optional, default is True)
            threads=True,
            # proxy URL scheme use use when downloading?
            # (optional, default is None)
            proxy=None,
        )
        .dropna()
        .reset_index()
    )

    data.columns = ["_".join(col) for col in data.columns.values]

    data = pd.concat([data["Date_"], data.filter(like="Close")], axis=1).rename(
        columns={
            "Date_": "timestamp_open",
            "GC=F_Close": "GOLD",
            "CL=F_Close": "OIL",
            "^GSPC_Close": "SP500",
            "^DJI_Close": "DOWJONES",
            "^IXIC_Close": "NASDAQ",
        }
    )

    data["timestamp_open"] = data["timestamp_open"].astype("int64") / 1e6

    return data


def data_preparation(
    flag: bool,
    cols_to_dop: list,
    btc_1h_raw: Any,
    eth_1h_raw: Any,
    bnb_1h_raw: Any,
) -> pd.DataFrame:

    # Contenating and Cleaning
    btc_1h_clean = pd.DataFrame()
    for _, partition_load_func in btc_1h_raw.items():
        partition_data = partition_load_func()
        btc_1h_clean = pd.concat(
            [btc_1h_clean, partition_data],
            ignore_index=True,
        )
    btc_1h_clean = (
        btc_1h_clean.drop(columns=cols_to_dop)
        .rename(columns={"quote_volume": "volume", "count": "trades_qty"})
        .sort_values(by=["timestamp_open"])
        .reset_index(drop=True)
    )

    eth_1h_clean = pd.DataFrame()
    for _, partition_load_func in eth_1h_raw.items():
        partition_data = partition_load_func()
        eth_1h_clean = pd.concat(
            [eth_1h_clean, partition_data],
            ignore_index=True,
        )
    eth_1h_clean = (
        eth_1h_clean.drop(columns=cols_to_dop)
        .rename(columns={"quote_volume": "volume", "count": "trades_qty"})
        .sort_values(by=["timestamp_open"])
        .reset_index(drop=True)
    )

    bnb_1h_clean = pd.DataFrame()
    for _, partition_load_func in bnb_1h_raw.items():
        partition_data = partition_load_func()
        bnb_1h_clean = pd.concat(
            [bnb_1h_clean, partition_data],
            ignore_index=True,
        )
    bnb_1h_clean = (
        bnb_1h_clean.drop(columns=cols_to_dop)
        .rename(columns={"quote_volume": "volume", "count": "trades_qty"})
        .sort_values(by=["timestamp_open"])
        .reset_index(drop=True)
    )

    # Trimming
    min_time = max(
        [
            btc_1h_clean["timestamp_open"].min(),
            eth_1h_clean["timestamp_open"].min(),
            bnb_1h_clean["timestamp_open"].min(),
        ]
    )
    max_time = min(
        [
            btc_1h_clean["timestamp_open"].max(),
            eth_1h_clean["timestamp_open"].max(),
            bnb_1h_clean["timestamp_open"].max(),
        ]
    )

    btc_1h_clean = btc_1h_clean[
        (btc_1h_clean["timestamp_open"] >= min_time)
        & (btc_1h_clean["timestamp_open"] <= max_time)
    ].reset_index(drop=True)
    eth_1h_clean = eth_1h_clean[
        (eth_1h_clean["timestamp_open"] >= min_time)
        & (eth_1h_clean["timestamp_open"] <= max_time)
    ].reset_index(drop=True)
    bnb_1h_clean = bnb_1h_clean[
        (bnb_1h_clean["timestamp_open"] >= min_time)
        & (bnb_1h_clean["timestamp_open"] <= max_time)
    ].reset_index(drop=True)

    return btc_1h_clean, eth_1h_clean, bnb_1h_clean


def fti_engineering(
    btc_1h_clean: pd.DataFrame,
    eth_1h_clean: pd.DataFrame,
    bnb_1h_clean: pd.DataFrame,
) -> pd.DataFrame:
    data = {
        "btc": btc_1h_clean,
        "eth": eth_1h_clean,
        "bnb": bnb_1h_clean,
    }

    # Pct changes, and technical indicators
    for token in data.keys():
        df = data[token]
        df = df.drop(columns=["timestamp_close"])

        df["close_pct"] = df["close"].pct_change()
        df["trade_vol_avg"] = df["volume"] / df["trades_qty"]
        df["sma_cross"] = TA.SMA(df, 50) - TA.SMA(df, 14)
        df["macd_signal"] = TA.MACD(df)["SIGNAL"]
        df["rsi"] = TA.RSI(df)
        df["obv_pct"] = TA.OBV(df).pct_change()
        df["bbands_size"] = TA.BBANDS(df)["BB_UPPER"] - TA.BBANDS(df)["BB_LOWER"]
        df["sar_diff"] = df["close"] - TA.SAR(df)

        data[token] = df.add_prefix(f"{token[:3]}_").rename(
            columns={f"{token[:3]}_timestamp_open": "timestamp_open"}
        )

    # Concat time, tick, and vol datsets
    data = pd.concat(
        [
            data["btc"],
            data["eth"].drop(columns="timestamp_open"),
            data["bnb"].drop(columns="timestamp_open"),
        ],
        axis=1,
    )

    return data


def ext_data_merge(
    token_1h_data: pd.DataFrame,
    ext_data: pd.DataFrame,
) -> pd.DataFrame:

    ext_data.columns = ext_data.columns.str.lower()

    df_fill = token_1h_data.merge(
        ext_data, how="outer", on="timestamp_open"
    ).sort_values("timestamp_open")
    df_fill.loc[:, ext_data.columns[1:]] = df_fill.loc[:, ext_data.columns[1:]].ffill()

    data = df_fill.dropna().copy()

    return data
