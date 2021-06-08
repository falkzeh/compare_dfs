# compare_dfs

Tool to compare two Pandas DataFrames.

## Functions

### compare_count()
Function will compare the number of rows.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame): second DataFrame

    Returns:
        count_df1 (int): number of rows of df1
        count_df2 (int): number of rows of df2
        count_diff (int): difference in rows between df1 and df2

---
### compare_columns()
Function will compare the column headers of both DataFrames and find differences.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame): second DataFrame

    Returns:
        column_diff_df1 (Array): df1 column difference in comparison to df2
        column_diff_df2 (Array): df2 column difference in comparison to df1

---
### compare_values()
Function will compare the values of both DataFrames and return results in a new DataFrame.

    Args:
        df1 (Pandas DataFrame): first DataFrame
        df2 (Pandas DataFrame): second DataFrame
        keys (Array): primary keys to join DataFrames

    Returns:
        result_df (Pandas DataFrame): differences in values in both DataFrames

## Examples

### compare_count()

```python
# Create two example DataFrames with different row counts
data1 = {'Name':['Tom', 'nick', 'krish', 'jack'],
        'Age':[20, 21, 19, 18]}
df1 = pd.DataFrame(data1)

data2 = {'Name':['Tom', 'nick', 'krish'],
        'Age':[20, 21, 19]}
df2 = pd.DataFrame(data2)


# Run the function
count_df1, count_df2, count_diff = compare_count(df1, df2)


# Output
print(f'count_df1: {count_df1}, count_df2: {count_df2}, count_diff: {count_diff}')
```
```bash
count_df1: 4, count_df2: 3, count_diff: 1
```
---
### compare_columns()

```python
# Create two example DataFrames with different columns
data1 = {'Name':['Tom', 'nick', 'krish', 'jack'],
        'Age':[20, 21, 19, 18],
        'City':['NYC', 'BER', 'LON', 'LIS']}
df1 = pd.DataFrame(data1)

data2 = {'Name':['Tom', 'nick', 'krish', 'jack'],
        'Age':[20, 21, 19, 18],
        'Country':['US', 'DE', 'GB', 'PT']}
df2 = pd.DataFrame(data2)


# Run the function
column_diff_df1, column_diff_df2 = compare_columns(df1, df2)


# Output
print(f'column_diff_df1: {column_diff_df1},\n column_diff_df2: {column_diff_df2}')
```
```bash
column_diff_df1: Index(['City'], dtype='object'),
 column_diff_df2: Index(['Country'], dtype='object')
```
---
### compare_values()

* Example 1

```python
# Create two example DataFrames with different values
data1 = {'Name':['Tom', 'nick', 'krish', 'jack'],
        'Age':[20, 21, 19, 18]}
df1 = pd.DataFrame(data1)

data2 = {'Name':['Tom', 'nick', 'krish', 'jack'],
        'Age':[80, 21, 20, 18]}
df2 = pd.DataFrame(data2)


# Run the function
results = compare_values(df1, df2, keys=['Name'])


# Output
print(results)
```
```bash
  at_df1 at_df2      keys error_keys value_df1 value_df2 error_description error_field                inserted_at
0    df1    df2  ['Name']        Tom        20        80  value difference         Age 2021-06-08 16:47:10.097277
1    df1    df2  ['Name']      krish        19        20  value difference         Age 2021-06-08 16:47:10.097277
```

* Example 2

```python
# Create two example DataFrames with different sets
data1 = {'Name':['Tom', 'nick', 'krish'],
        'Age':[20, 21, 19]}
df1 = pd.DataFrame(data1)

data2 = {'Name':['Tom', 'nick', 'krish', 'jack'],
        'Age':[20, 21, 19, 18]}
df2 = pd.DataFrame(data2)


# Run the function
results = compare_values(df1, df2, keys=['Name'])


# Output
print(results)
```
```bash
  at_df1 at_df2      keys error_keys error_description                inserted_at
1    df1    df2  ['Name']       jack          only df2 2021-06-08 16:51:16.643965
```