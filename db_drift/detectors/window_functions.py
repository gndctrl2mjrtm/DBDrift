from pyspark.sql import functions as F
from pyspark.sql.window import Window
import numpy as np
from pyspark.sql.types import *


class WindowFilter(object):

    def __init__(self,
                 window_sizes: list):
        """

        :param window_sizes:
        """
        if not isinstance(window_sizes, list):
            raise TypeError
        for n in window_sizes:
            if not isinstance(n, int):
                raise TypeError
            if n < 2:
                raise UserWarning("Window size must be > 1")
        self.window_sizes = window_sizes
        self._median_udf = F.udf(lambda x: float(np.median(x)), FloatType())
        self.res = None

    def _days2seconds(self, n_days: int):
        """

        :param n_days:
        :return:
        """
        if not isinstance(n_days, int):
            raise TypeError
        return n_days * 86400

    def apply(self, df, target_col, date_col):
        """

        :param df:
        :return:
        """
        if not type(df).__name__ == "DataFrame":
            raise TypeError
        if not isinstance(target_col, str):
            raise TypeError
        if not (target_col in df.schema.fieldNames()):
            raise UserWarning
        if not isinstance(date_col, str):
            raise TypeError
        if not (date_col in df.schema.fieldNames()):
            raise UserWarning
        windows = {}
        for s in self.window_sizes:
            w_name = 'w_{}'
            windows[w_name] = (
                Window.orderBy(F.unix_timestamp(date_col, 'yyyy-MM-dd').cast('long')).rangeBetween(
                    -self._days2seconds(s), 0))
            df = (df
                  # Rolling average of records
                  .withColumn(f"{target_col}_{s}_rolling_average", F.avg(target_col).over(windows[w_name]))

                  # Collect a list of items in the window to calculate median values or other UDFs
                  .withColumn(f"{target_col}_{s}_window_list", F.collect_list(target_col).over(windows[w_name]))

                  #  Median value of the window, median is more robust than average for outlier detection
                  .withColumn(f"{target_col}_{s}_rolling_median", self._median_udf(f"{target_col}_{s}_window_list"))

                  # Difference between the records count and the rolling average
                  .withColumn(f"{target_col}_{s}_diff",
                              F.abs(F.col(target_col) - F.col(f"{target_col}_{s}_rolling_average")))

                  # Maximum value of {target_col}count over the window
                  .withColumn(f"{target_col}_{s}_max", F.max(F.col(target_col)).over(windows[w_name]))

                  # Minimum value of {target_col}count over the window
                  .withColumn(f"{target_col}_{s}_min", F.min(F.col(target_col)).over(windows[w_name]))

                  # Difference between minimum and maximum value over the window
                  .withColumn(f"{target_col}_{s}_minmax_diff",
                              F.col(f"{target_col}_{s}_max") - F.col(f"{target_col}_{s}_min"))

                  # Absolute difference between the average {target_col}count and the minimum value over the window
                  .withColumn(f"{target_col}_{s}_avgmin_diff",
                              F.abs(F.col(f"{target_col}_{s}_rolling_average") - F.col(f"{target_col}_{s}_min")))

                  # Absolute difference between the average {target_col}count and the maximum value over the window
                  .withColumn(f"{target_col}_{s}_avgmax_diff",
                              F.abs(F.col(f"{target_col}_{s}_rolling_average") - F.col(f"{target_col}_{s}_max")))

                  # Absolute differnece between {target_col}count and the minimum value over the window
                  .withColumn(f"{target_col}_{s}_min_diff", F.abs(F.col(target_col) - F.col(f"{target_col}_{s}_min")))

                  # Absolute difference between {target_col}count and the maximum value over the window
                  .withColumn(f"{target_col}_{s}_max_diff", F.abs(F.col(target_col) - F.col(f"{target_col}_{s}_max")))

                  # Drop the window list as it is no longer needed
                  .drop(f"{target_col}_{s}_window_list")
                  )
        return df
