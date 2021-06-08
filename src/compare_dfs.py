import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

def clean_dfs(df1, df2=pd.DataFrame):
    """Function will convert datatypes and replace any white spaces, tabs, line feeds and carriage returns to make 
       both DataFrames comparable and prevent malfunctioning in later functions.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame) [default pd.DataFrame]: second DataFrame

    Returns:
        df1 (Pandas DataFrame): cleaned first DataFrame
        df2 (Pandas DataFrame): cleaned second DataFrame
    """

    try:
        df1.columns = df1.columns.str.strip()
        df1.columns = df1.columns.str.replace(' ', '_')
        df2.columns = df2.columns.str.strip()
        df2.columns = df2.columns.str.replace(' ', '_')

        df1.columns = df1.columns.str.replace(r"[^a-zA-Z\d\_]+", "")
        df2.columns = df2.columns.str.replace(r"[^a-zA-Z\d\_]+", "")

        df1 = df1.convert_dtypes()
        df2 = df2.convert_dtypes()
        
        df1.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)   
        df2.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
    except Exception as e:
        logging.error(f'Error in clean_dfs(): {e}')
    finally:
        logging.info(f'Success: {len(df1)} and {len(df2)} cleaned')
        return df1, df2

def compare_columns(df1, df2):
    """Function will compare the column headers of both DataFrames and find differences.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame): second DataFrame

    Returns:
        column_diff_df1 (Array): df1 column difference in comparison to df2
        column_diff_df2 (Array): df2 column difference in comparison to df1
    """

    try:
        df1_cols = df1.columns
        df2_cols = df2.columns
        column_diff_df1 = df1_cols.difference(df2_cols)
        column_diff_df2 = df2_cols.difference(df1_cols)
    except Exception as e:
        logging.error(f'Error in compare_columns(): {e}')
    finally:
        if len(column_diff_df1) > 3 or len(column_diff_df2) > 3:
            logging.info(f'Success: differences found:\n {column_diff_df1}\n {column_diff_df2}')
        else:
            logging.info(f'Success: no differences in columns found')

        return column_diff_df1, column_diff_df2

def compare_count(df1, df2):
    """Function will compare the number of rows.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame): second DataFrame

    Returns:
        count_df1 (int): number of rows of df1
        count_df2 (int): number of rows of df2
        count_diff (int): difference in rows between df1 and df2
    """
    
    try:
        count_df1 = len(df1)
        count_df2 = len(df2)
        count_diff = count_df1 - count_df2

        if count_diff < 0:
            count_diff = count_diff * -1
        else:
            count_diff = count_diff
    except Exception as e:
        logging.error(f'Error in compare_count(): {e}')
    finally:
        return count_df1, count_df2, count_diff

def compare_values(df1, df2, keys):
    """Function will compare the values of both DataFrames and return results in a new DataFrame.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame): second DataFrame
        keys (Array): primary keys to join DataFrames

    Returns:
        result_df (Pandas DataFrame): differences in values in both DataFrames
    """

    try:
        df1, df2 = clean_dfs(df1, df2)

        result_df = pd.DataFrame()

        for column in df2.columns:
            if pd.api.types.is_datetime64tz_dtype(df2[column]):
                df2[column] = df2[column].apply(lambda x: x.tz_localize(None))
            elif pd.api.types.is_datetime64_ns_dtype(df2[column]):
                df2[column] = pd.to_datetime(df2[column], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f').dt.strftime('%Y-%m-%d %H:%M:%S')

        for column in df1.columns:
            if pd.api.types.is_datetime64tz_dtype(df1[column]):
                df1[column] = df1[column].apply(lambda x: x.tz_localize(None))

        names = {'at_df1': 'df1', 'at_df2': 'df2', 'keys': keys}

        df1[keys] = df1[keys].astype(str)
        df2[keys] = df2[keys].astype(str)

        df_merged = df1.merge(df2, indicator=True, how='outer', on=keys, sort=True, suffixes=('_df1', '_df2'))

        df_merged = df_merged.convert_dtypes()
        results = dict()
        compare_list = list(set(df2.columns) - set(keys))

        for case in ['left_only', 'right_only']:
            df_one_sided = df_merged[df_merged['_merge'] == case][keys]
            if len(df_one_sided.index) > 0:
                results[case] = len(df_one_sided.index)
                for key, value in names.items():
                    df_one_sided[key] = str(value)
                if case == 'right_only':
                    df_one_sided['error_description'] = 'only df2'
                else:
                    df_one_sided['error_description'] = 'only df1'
                df_one_sided['inserted_at'] = datetime.now()
                df_one_sided['error_keys'] = df_one_sided[keys].astype(str).agg(','.join, axis=1)
                df_one_sided = df_one_sided.reindex(columns=list(['at_df1','at_df2','keys', 'error_keys',
                                                                'error_description', 'inserted_at']))

                try:
                    result_df = result_df.append(df_one_sided)
                except Exception as e:
                    logging.error(f'Error in compare_values(): {e}')

        df_both = df_merged[df_merged['_merge'] == 'both']
        results['both'] = 0
        for column in compare_list:
            df = df_both[keys + [column + '_df1', column + '_df2']]
            df = df[df[column + '_df1'] != df[column + '_df2']]
            if len(df.index) > 0:
                results['both'] += len(df.index)
                for key, value in names.items():
                    df[key] = str(value)
                df['error_description'] = 'value difference'
                df.reset_index(inplace=True)
                if pd.api.types.is_string_dtype(df[column + '_df1']) and not df[column + '_df1'] is None:
                    df_trimmed = df[
                        df[column + '_df1'].apply(lambda x: str(x).strip()) == df[column + '_df2'].apply(
                            lambda x: str(x).strip())]
                    df.at[df_trimmed.index, 'error_description'] = 'value difference untrimmed only'
                df['inserted_at'] = datetime.now()
                df['error_keys'] = df[keys].astype(str).agg(','.join, axis=1)
                df['error_field'] = column
                df[[column + '_df1', column + '_df2']] = df[[column + '_df1', column + '_df2']].astype(str)
                df = df.reindex(columns=list(['at_df1','at_df2','keys', 'error_keys', column + '_df1', column + '_df2',
                                            'error_description', 'error_field', 'inserted_at']))

                try:
                    df.rename(columns={column + '_df1': 'value_df1', column + '_df2': 'value_df2'}, inplace=True)
                    result_df = result_df.append(df)
                except Exception as e:
                    logging.error(f'Error in compare_values(): {e}')

    except Exception as e:
        logging.error(f'Error in compare_values(): {e}')
    finally:
        return result_df
