"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    data_preparation,
    ext_data_download,
    ext_data_merge,
    fti_engineering,
    hist_data_download,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=hist_data_download,
                inputs="params:data_engineering.hist_data_download",
                outputs="download_flag",
                name="hist_data_download",
            ),
            node(
                func=ext_data_download,
                inputs="params:data_engineering.ext_data_download",
                outputs="ext_data",
                name="ext_data_download",
            ),
            node(
                func=data_preparation,
                inputs=[
                    "download_flag",
                    "params:data_engineering.cols_to_drop",
                    "btc_1h_raw",
                    "eth_1h_raw",
                    "bnb_1h_raw",
                ],
                outputs=["btc_1h_clean", "eth_1h_clean", "bnb_1h_clean"],
                name="data_preparation",
            ),
            node(
                func=fti_engineering,
                inputs=["btc_1h_clean", "eth_1h_clean", "bnb_1h_clean"],
                outputs="token_1h_data",
                name="fti_engineering",
            ),
            node(
                func=ext_data_merge,
                inputs=["token_1h_data", "ext_data"],
                outputs="token_1h_data_with_ext",
                name="ext_data_merge",
            ),
        ]
    )
