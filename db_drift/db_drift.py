"""Main module."""
from __future__ import print_function, division
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


class DBDrift(object):

    def __init__(self):
        self.excl_cols = []
        self.sel_cols = []
        self.non_sel_cols = []

    def _get_metadata(self, df: DataFrame,
                      timestamp_col=None,
                      interval="day"):
        if not isinstance(df, DataFrame):
            raise TypeError
        if not (timestamp_col is None):
            if not isinstance(timestamp_col, str):
                raise TypeError(f"table_name arg must be str, not {type(table_name).__name__}")
            if not (timestamp_col in df.schema.fieldNames()):
                raise UserWarning
        if not isinstance(interval, str):
            raise TypeError(f"table_name arg must be str, not {type(table_name).__name__}")

        if timestamp_col is None:
            funcs = []
            for c in self.sel_cols:
                funcs.append(F.mean(c).alias(f"{c}_mean"))
                funcs.append(F.min(c).alias(f"{c}_min"))
                funcs.append(F.max(c).alias(f"{c}_max"))
                funcs.append(F.stddev(c).alias(f"{c}_stddev"))
            res_df = (df
                      .select(*funcs)
                      .limit(1)
                      .withColumn(f"_data_drift_{interval}",
                                  F.date_trunc(interval, F.current_timestamp()))
                      )
        else:
            f_cols = [f"_data_drift_{interval}"]
            funcs = []
            for c in self.sel_cols:
                funcs.append(F.mean(c).alias(f"{c}_mean"))
                funcs.append(F.min(c).alias(f"{c}_min"))
                funcs.append(F.max(c).alias(f"{c}_max"))
                funcs.append(F.stddev(c).alias(f"{c}_stddev"))
                f_cols.extend([f"{c}_mean", f"{c}_min", f"{c}_max", f"{c}_stddev"])

            res_df = df.withColumn(f"_data_drift_{interval}",
                                   F.date_trunc(interval, F.col(timestamp_col)))
            res_df = (res_df
                      .groupBy(f"_data_drift_{interval}")
                      .agg(*funcs)
                      .select(*f_cols)
                      .distinct())
        return res_df

    @staticmethod
    def _write_to_metatable(updates_df: DataFrame,
                            table_name: str,
                            pkey_col: str):
        if not isinstance(updates_df, DataFrame):
            raise TypeError(f"updates_df arg must be str, not {type(updates_df).__name__}")
        if not isinstance(table_name, str):
            raise TypeError(f"table_name arg must be str, not {type(table_name).__name__}")
        if not isinstance(pkey_col, str):
            raise TypeError(f"pkey_col arg must be str, not {type(pkey_col).__name__}")
        if not (pkey_col in updates_df.schema.fieldNames()):
            raise UserWarning(f"Column {pkey_col} not found in updates_df schema: {updates_df.schema.fieldNames()}")
        if '.' in table_name:
            database, table = table_name.split(".")
            spark.sql(f"create database if not exists {database}")
        if not (spark._jsparkSession.catalog().tableExists(database, table)):
            (updates_df
             .write
             .format("delta")
             .mode("overwrite")
             .saveAsTable(f"{database}.{table}")
             )
        else:
            updates_df.createOrReplaceTempView("temp_drift_updates")
            merge_sql = f"""
            MERGE INTO {database}.{table} t USING temp_drift_updates u
            ON t.{pkey_col} = u.{pkey_col}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""

            spark.sql(merge_sql)

    def run(self, df,
            metadata_table: str,
            timestamp_col: str = None,
            excl_cols: list = [],
            interval="day"):
        if not isinstance(df, DataFrame):
            raise TypeError(f"df arg must be PySpark DataFrame, not {type(df).__name__}")
        if not isinstance(metadata_table, str):
            raise TypeError(f"metadata_table arg must be str, not {type(metadata_table).__name__}")
        if not (timestamp_col is None):
            if not isinstance(timestamp_col, str):
                raise TypeError(f"timestamp_col arg must be str, not {type(timestamp_col).__name__}")
            if not (timestamp_col in df.schema.fieldNames()):
                raise UserWarning(f"Column {timestamp_col} not found in df schema: {df.schema.fieldNames()}")
        if not isinstance(excl_cols, list):
            raise TypeError(f"table_name arg must be list, not {type(excl_cols).__name__}")
        for c in excl_cols:
            if not isinstance(c, str):
                raise TypeError(f"Columns in excl_cols arg must be str, not {type(c).__name__}")
            if not (c in df.schema.fieldNames()):
                raise UserWarning(f"Column {c} not found in df schema: {df.schema.fieldNames()}")
        interval_options = ["year", "month", "day", "minute", "second"]
        if not (interval in interval_options):
            raise UserWarning

        self.interval = interval
        self.excl_cols = excl_cols

        self.sel_cols = [c for c in df.schema.fieldNames() if
                         str(df.schema[c].dataType) in ["DoubleType()", "IntegerType()"] and c not in self.excl_cols]
        self.non_sel_cols = [c for c in df.schema.fieldNames() if not c in self.sel_cols]

        updates_df = self._get_metadata(df, timestamp_col=timestamp_col, interval=interval)

        pkey_col = f"_data_drift_{interval}"
        self._write_to_metatable(updates_df, table_name=metadata_table, pkey_col=pkey_col)
