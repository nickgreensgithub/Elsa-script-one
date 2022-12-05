import pandas as pd
import argparse
import os


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path')
    parser.add_argument('-f', '--result_file_name', default="joined", required=False, help="The name of the resulting file, will have the same extension as the input files")
    parser.add_argument('-s', '--column_separator', default=None, required=False)
    args=parser.parse_args()
    return args


def unescaped_str(arg_str: str) -> str:
    if arg_str.startswith('\\'):
        return arg_str.removeprefix('\\')
    return arg_str


def read_file(path: str, col_separator='\t') -> pd.DataFrame:
    df = pd.read_csv(path, sep=col_separator, header=None, index_col=False, engine='python')
    return df


def write_file(df: pd.DataFrame, path: str, col_separator='\t') -> None:
    df.to_csv(path, sep=col_separator, index=False, header=False)


def append_string_if_no_value(row: pd.Series, appending_value: str, column_number: int = 8) -> pd.Series:
    if not row['has_value']:
        row.iloc[column_number] = row.iloc[column_number] + appending_value
    return row


def mark_rows_with_values(df: pd.DataFrame, column_number: int = 8) -> pd.DataFrame:
    test = df.apply(axis='columns', func=lambda x: has_appending_value(x.iloc[column_number]))
    
    df['has_value'] = test
    return df


def append_string_to_column(df: pd.DataFrame, column_number: int, string_to_append: str = ':PS') -> pd.DataFrame:
    df = df.apply(axis='columns', func=lambda x: append_string_if_no_value(x, string_to_append, column_number))
    return df


def get_position_value(row: pd.Series) ->pd.Series:
    return row.iloc[9].split(':')[-1]


def has_appending_value(string: str) -> bool:
    if string.count(':') == 7:
        return True
    return False


def add_adjacent_value_columns(df: pd.DataFrame) -> pd.DataFrame:
    previous_column = []
    next_column = []
    previous_value = (0, 0)
    next_value = (0, 0)

    length = len(df.index)

    for index, row in df.iterrows():
        if has_appending_value(row.iloc[9]):
            previous_value = (int(row.iloc[1]), int(get_position_value(row)))
        previous_column.append(previous_value)
        reverse_index = length - (index + 1)
        reverse_row = df.iloc[reverse_index]
        if has_appending_value(reverse_row.iloc[9]):
            next_value = (int(reverse_row.iloc[1]), int(get_position_value(reverse_row)))
        next_column.append(next_value)

    df['previous_value'] = pd.Series(previous_column)
    df['next_value'] = pd.Series(next_column[::-1])
    return df


def append_adjacent_value_values(row: pd.Series) -> pd.Series:
    if not row['has_value']:
        row.iloc[9] = row.iloc[9] + ':' + str(min([row['previous_value'], row['next_value']], key=lambda x: abs(x[0] - row.iloc[1]))[1])
    return row


def finally_append_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.apply(axis='columns', func=lambda x: append_adjacent_value_values(x))
    return df


def main() -> None:
    args = read_args()
    input_file = args.file_path
    result_file_name = args.result_file_name
    column_separator = args.column_separator

    if not os.path.exists(input_file):
        print("File path does not exist")
        exit(1)

    df = read_file(input_file, column_separator)
    df = mark_rows_with_values(df, 8)
    df = append_string_to_column(df, 8, ':PS')
    df = add_adjacent_value_columns(df)
    df = finally_append_values(df)
    df.drop(columns=['has_value', 'previous_value', 'next_value'], inplace=True)
    write_file(df, result_file_name, '\t')


if __name__ == "__main__":
    main()
