import dask.dataframe as dd
import argparse
import logging
import datetime
import time
import pandas as pd
from dask.diagnostics import ProgressBar

def load_data_to_dask(file, delimiter, blocksize="16MB"):
    logging.info(f"Loading file: {file}")
    df = dd.read_csv(file, sep=delimiter, assume_missing=True, blocksize=blocksize, quotechar="'")
    df = df.map_partitions(lambda partition: partition.astype(str))
    df = df.fillna("NULL_VALUE")
    df.columns = df.columns.str.strip().str.replace(" ", "_")
    return df

def compare_files_with_keys_and_column_differences(base_df, actual_df, key_columns, exclude_columns):
    if exclude_columns:
        base_df = base_df.drop(columns=exclude_columns, errors='ignore')
        actual_df = actual_df.drop(columns=exclude_columns, errors='ignore')

    common_columns = list(set(base_df.columns).intersection(set(actual_df.columns)))
    base_df = base_df[common_columns]
    actual_df = actual_df[common_columns]

    logging.info("Performing key-based comparison...")
    with ProgressBar():
        base_except_actual = base_df.merge(actual_df, how="left", indicator=True).query('_merge == "left_only"').drop(columns=["_merge"])
    with ProgressBar():
        actual_except_base = actual_df.merge(base_df, how="left", indicator=True).query('_merge == "left_only"').drop(columns=["_merge"])

    logging.info("Performing column-wise comparison...")
    with ProgressBar():
        merged_df = base_df.merge(actual_df, on=key_columns, suffixes=('.base', '.actual'))

    column_differences = []
    column_diff_counts = {}

    for column in common_columns:
        if column not in key_columns:
            base_column = f"{column}.base"
            actual_column = f"{column}.actual"
            mismatched_rows = merged_df[merged_df[base_column] != merged_df[actual_column]][key_columns + [base_column, actual_column]]
            mismatched_rows_computed = mismatched_rows.compute()

            if not mismatched_rows_computed.empty:
                column_diff_counts[column] = len(mismatched_rows_computed)
                for _, row in mismatched_rows_computed.iterrows():
                    key_values = {key: row[key] for key in key_columns}
                    column_differences.append({
                        **key_values,
                        "columnName": column,
                        "baseValue": row[base_column],
                        "actualValue": row[actual_column]
                    })

    column_diff_keys = pd.DataFrame(column_differences)[key_columns].drop_duplicates()

    base_except_actual = base_except_actual.compute()
    actual_except_base = actual_except_base.compute()

    base_except_actual = base_except_actual[base_except_actual[key_columns].apply(tuple, axis=1).isin(column_diff_keys.apply(tuple, axis=1))]
    actual_except_base = actual_except_base[actual_except_base[key_columns].apply(tuple, axis=1).isin(column_diff_keys.apply(tuple, axis=1))]

    column_differences_df = pd.DataFrame(column_differences)

    if not column_differences_df.empty:
        pivot_table = column_differences_df.groupby(['columnName', 'baseValue', 'actualValue']).size().reset_index(name='count')
        pivot_table = pivot_table.sort_values(by=['count', 'columnName', 'baseValue', 'actualValue'], ascending=[False, True, True, True])
        pivot_table_df = dd.from_pandas(pivot_table, npartitions=1)

        column_differences_df["row"] = column_differences_df.groupby(['columnName', 'baseValue', 'actualValue']).cumcount() + 1
        top_5_differences = column_differences_df[column_differences_df["row"] <= 5].drop(columns=['row'])
        top_5_differences_df = dd.from_pandas(top_5_differences, npartitions=1)
    else:
        pivot_table_df = dd.from_pandas(pd.DataFrame(), npartitions=1)
        top_5_differences_df = dd.from_pandas(pd.DataFrame(), npartitions=1)

    column_diff_counts_df = dd.from_pandas(
        pd.DataFrame(list(column_diff_counts.items()), columns=["columnName", "DifferenceCount"]),
        npartitions=1
    )

    return base_except_actual, actual_except_base, column_differences, column_diff_counts_df, pivot_table_df, top_5_differences_df

def write_output_to_csv(output_dir, base_except_actual, actual_except_base, column_differences, column_diff_counts_df, pivot_table_df, top_5_differences_df, key_columns):
    logging.info(f"Writing output to directory: {output_dir}")

    def compute_if_dask(df):
        return df.compute() if hasattr(df, "compute") else df

    base_except_actual = compute_if_dask(base_except_actual)
    actual_except_base = compute_if_dask(actual_except_base)
    column_diff_counts_df = compute_if_dask(column_diff_counts_df)
    pivot_table_df = compute_if_dask(pivot_table_df)
    top_5_differences_df = compute_if_dask(top_5_differences_df)

    base_except_actual.to_csv(f"{output_dir}/base_except_actual.csv", index=False)
    actual_except_base.to_csv(f"{output_dir}/actual_except_base.csv", index=False)
    column_diff_counts_df.to_csv(f"{output_dir}/column_diff_counts.csv", index=False)
    pivot_table_df.to_csv(f"{output_dir}/pivot_table.csv", index=False)
    top_5_differences_df.to_csv(f"{output_dir}/top_5_differences.csv", index=False)

def file_compare_with_key(base_file, actual_file, key_columns, exclude_columns, base_delimiter, actual_delimiter, output_dir):
    base_df = load_data_to_dask(base_file, base_delimiter)
    actual_df = load_data_to_dask(actual_file, actual_delimiter)

    logging.info(f"Total records in base file: {base_df.shape[0].compute()}")
    logging.info(f"Total records in actual file: {actual_df.shape[0].compute()}")

    missing_base_columns = [col for col in key_columns if col not in base_df.columns]
    missing_actual_columns = [col for col in key_columns if col not in actual_df.columns]

    if missing_base_columns:
        raise KeyError(f"Missing key columns in base file: {missing_base_columns}")
    if missing_actual_columns:
        raise KeyError(f"Missing key columns in actual file: {missing_actual_columns}")

    base_df = base_df.drop_duplicates(subset=key_columns)
    actual_df = actual_df.drop_duplicates(subset=key_columns)

    results = compare_files_with_keys_and_column_differences(base_df, actual_df, key_columns, exclude_columns)
    write_output_to_csv(output_dir, *results, key_columns)

    logging.info(f"Total differing rows: {len(results[2])}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--baseFileLocation", required=True, help="Base file location with directory")
    parser.add_argument("-a", "--actualFileLocation", required=True, help="Actual file location with directory")
    parser.add_argument("-o", "--outputDir", required=True, help="Output directory for result files")
    parser.add_argument("-k", "--keyColumns", required=True, help="Key columns to compare, separated with pipe (|)")
    parser.add_argument("-e", "--excludeColumns", default="", help="Columns to exclude, separated with pipe (|)")
    parser.add_argument("-ba", "--baseDelimiter", default=",", help="Base file delimiter")
    parser.add_argument("-ad", "--actualDelimiter", default=",", help="Actual file delimiter")

    args = parser.parse_args()

    logging.basicConfig(format="%(message)s", level=logging.INFO)
    start_time = time.time()
    logging.info("**** File Comparison started *****")

    key_columns = args.keyColumns.split('|')
    exclude_columns = args.excludeColumns.split('|') if args.excludeColumns else []

    try:
        file_compare_with_key(
            args.baseFileLocation,
            args.actualFileLocation,
            key_columns,
            exclude_columns,
            args.baseDelimiter,
            args.actualDelimiter,
            args.outputDir
        )
    except Exception as e:
        logging.error(f"Error during comparison: {e}")
        raise

    elapsed_time = time.time() - start_time
    time_taken = str(datetime.timedelta(seconds=elapsed_time))
    logging.info(f"File Comparison completed in {time_taken}")
