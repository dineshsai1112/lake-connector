"""
Distributed feature encoder built on Spark ML transformers.
"""

from typing import Iterable, List, Optional

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql import DataFrame


class DistributedFeatureEncoder:
    """
    Encode categorical features in a distributed manner.

    This encoder creates a Spark ML Pipeline with:
      1. StringIndexer for each categorical column
      2. OneHotEncoder for each indexed column
    """

    def __init__(
        self,
        categorical_columns: Iterable[str],
        drop_last: bool = True,
        handle_invalid: str = "keep",
    ) -> None:
        self.categorical_columns: List[str] = list(categorical_columns)
        self.drop_last = drop_last
        self.handle_invalid = handle_invalid
        self._pipeline: Optional[Pipeline] = None
        self._model: Optional[PipelineModel] = None

        if not self.categorical_columns:
            raise ValueError("categorical_columns must not be empty.")

    def build_pipeline(self) -> Pipeline:
        """Build and cache the Spark ML pipeline."""
        index_output_cols = [f"{col}_idx" for col in self.categorical_columns]
        encoded_output_cols = [f"{col}_ohe" for col in self.categorical_columns]

        indexers = [
            StringIndexer(
                inputCol=input_col,
                outputCol=output_col,
                handleInvalid=self.handle_invalid,
            )
            for input_col, output_col in zip(self.categorical_columns, index_output_cols)
        ]

        encoder = OneHotEncoder(
            inputCols=index_output_cols,
            outputCols=encoded_output_cols,
            dropLast=self.drop_last,
            handleInvalid=self.handle_invalid,
        )

        self._pipeline = Pipeline(stages=[*indexers, encoder])
        return self._pipeline

    def fit(self, df: DataFrame) -> PipelineModel:
        """
        Fit the encoding pipeline on a Spark DataFrame.
        """
        pipeline = self._pipeline or self.build_pipeline()
        self._model = pipeline.fit(df)
        return self._model

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply the fitted encoder to a Spark DataFrame.

        Raises:
            RuntimeError: If fit() has not been called yet.
        """
        if self._model is None:
            raise RuntimeError("Encoder is not fitted. Call fit() before transform().")
        return self._model.transform(df)

    def fit_transform(self, df: DataFrame) -> DataFrame:
        """Fit and transform in one step."""
        self.fit(df)
        return self.transform(df)
