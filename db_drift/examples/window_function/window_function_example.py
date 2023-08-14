
def sample_stats_filter(df, target_col):
    res_df = (df
              .withColumn("flagged",
                          F.when(
                              (F.abs(F.col(target_col) - F.col(f"{target_col}_3_rolling_average")) > F.col(
                                  target_col) * 0.30),
                              1)
                          .when(
                              (F.abs(F.col(target_col) - F.col(f"{target_col}_3_rolling_median")) > F.col(
                                  target_col) * 0.30),
                              1)
                          .when(
                              (F.col(f"{target_col}_2_minmax_diff") > F.col(target_col) * 0.10),
                              1)
                          .otherwise(0))
              .withColumn("flag_ts", F.current_timestamp())
              )
    return res_df


def example_main():
    df = spark.sql("select * from ....")
    time_col = '_data_drift_day'

    wf = WindowFilter()
    copy_df = df.withColumnRenamed(time_col, f"join_{time_col}")

    cols = [c for c in df.schema.fieldNames() if c != time_col]

    for c in cols:
        res_df = wf.apply(df, c, "_data_drift_day")
        flagged_df = sample_stats_filter(res_df, c)
        sel_df = flagged_df.select(c, time_col, "flagged").withColumnRenamed("flagged", f"{c}_flagged")
        copy_df = copy_df.join(sel_df, copy_df[f"join_{time_col}"] == sel_df[time_col])

    copy_df = copy_df.withColumn("flagged", F.greatest(*[f"{c}_flagged" for c in cols]))
    display(copy_df)
