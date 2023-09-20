<h1 align="center">dbt-expectations</h1>
<p align="center">
<img alt="logo" width="10%" src="https://raw.githubusercontent.com/calogica/dbt-expectations/main/static/dbt-expectations-logo.svg" />
</p>

<hr/>

<p align="center">
<a href="https://circleci.com/gh/calogica/dbt-expectations/tree/main">
<img alt="CircleCI" src="https://img.shields.io/circleci/build/github/calogica/dbt-expectations/main?style=plastic"/>
</a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4?style=plastic"/>

</p>

## About

`dbt-expectations` is an extension package for [**dbt**](https://github.com/dbt-labs/dbt), inspired by the [Great Expectations package for Python](https://greatexpectations.io/). The intent is to allow dbt users to deploy GE-like tests in their data warehouse directly from dbt, vs having to add another integration with their data warehouse.

## Featured Sponsors ❤️

Development of `dbt-expectations` (and `dbt-date`) is funded by our amazing [sponsors](https://github.com/sponsors/calogica), including our **featured** sponsors:

<table width="80%">
<tr>

<td width="40%" valign="top" align="center">
<p><a href="https://datacoves.com/product" target="_blank">datacoves.com</a></p>
<p>
<a href="https://datacoves.com/product" target="_blank">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/datacoves/dbt-coves/blob/main/images/datacoves-dark.png">
  <img alt="Datacoves" src="https://github.com/datacoves/dbt-coves/blob/main/images/datacoves-light.png" width="150">
</picture>
</a>
</p>
</td>

</tr>

</table>

## Install

`dbt-expectations` currently supports `dbt 1.2.x` or higher.

Check [dbt package hub](https://hub.getdbt.com/calogica/dbt_expectations/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in `packages.yml`

```yaml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.8.0", "<0.9.0"]
    # <see https://github.com/calogica/dbt-expectations/releases/latest> for the latest version tag
```

This package supports:

* Postgres
* Snowflake
* BigQuery
* DuckDB

For latest release, see [https://github.com/calogica/dbt-expectations/releases](https://github.com/calogica/dbt-expectations/releases)

### Dependencies

This package includes a reference to [`dbt-date`](https://github.com/calogica/dbt-date), so there's no need to also import `dbt-date` in your local project.

### Variables

The following variables need to be defined in your `dbt_project.yml` file:

```yaml
vars:
  'dbt_date:time_zone': 'America/Los_Angeles'
```

You may specify [any valid timezone string](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) in place of `America/Los_Angeles`.
For example, use `America/New_York` for East Coast Time.

## Available Tests

### Table shape

- [expect_column_to_exist](#expect_column_to_exist)
- [expect_row_values_to_have_recent_data](#expect_row_values_to_have_recent_data)
- [expect_grouped_row_values_to_have_recent_data](#expect_grouped_row_values_to_have_recent_data)
- [expect_table_aggregation_to_equal_other_table](#expect_table_aggregation_to_equal_other_table)
- [expect_table_column_count_to_be_between](#expect_table_column_count_to_be_between)
- [expect_table_column_count_to_equal_other_table](#expect_table_column_count_to_equal_other_table)
- [expect_table_column_count_to_equal](#expect_table_column_count_to_equal)
- [expect_table_columns_to_not_contain_set](#expect_table_columns_to_not_contain_set)
- [expect_table_columns_to_contain_set](#expect_table_columns_to_contain_set)
- [expect_table_columns_to_match_ordered_list](#expect_table_columns_to_match_ordered_list)
- [expect_table_columns_to_match_set](#expect_table_columns_to_match_set)
- [expect_table_row_count_to_be_between](#expect_table_row_count_to_be_between)
- [expect_table_row_count_to_equal_other_table](#expect_table_row_count_to_equal_other_table)
- [expect_table_row_count_to_equal_other_table_times_factor](#expect_table_row_count_to_equal_other_table_times_factor)
- [expect_table_row_count_to_equal](#expect_table_row_count_to_equal)

### Missing values, unique values, and types

- [expect_column_values_to_be_null](#expect_column_values_to_be_null)
- [expect_column_values_to_not_be_null](#expect_column_values_to_not_be_null)
- [expect_column_values_to_be_unique](#expect_column_values_to_be_unique)
- [expect_column_values_to_be_of_type](#expect_column_values_to_be_of_type)
- [expect_column_values_to_be_in_type_list](#expect_column_values_to_be_in_type_list)
- [expect_column_values_to_have_consistent_casing](#expect_column_values_to_have_consistent_casing)

### Sets and ranges

- [expect_column_values_to_be_in_set](#expect_column_values_to_be_in_set)
- [expect_column_values_to_not_be_in_set](#expect_column_values_to_not_be_in_set)
- [expect_column_values_to_be_between](#expect_column_values_to_be_between)
- [expect_column_values_to_be_decreasing](#expect_column_values_to_be_decreasing)
- [expect_column_values_to_be_increasing](#expect_column_values_to_be_increasing)

### String matching

- [expect_column_value_lengths_to_be_between](#expect_column_value_lengths_to_be_between)
- [expect_column_value_lengths_to_equal](#expect_column_value_lengths_to_equal)
- [expect_column_values_to_match_like_pattern](#expect_column_values_to_match_like_pattern)
- [expect_column_values_to_match_like_pattern_list](#expect_column_values_to_match_like_pattern_list)
- [expect_column_values_to_match_regex](#expect_column_values_to_match_regex)
- [expect_column_values_to_match_regex_list](#expect_column_values_to_match_regex_list)
- [expect_column_values_to_not_match_like_pattern](#expect_column_values_to_not_match_like_pattern)
- [expect_column_values_to_not_match_like_pattern_list](#expect_column_values_to_not_match_like_pattern_list)
- [expect_column_values_to_not_match_regex](#expect_column_values_to_not_match_regex)
- [expect_column_values_to_not_match_regex_list](#expect_column_values_to_not_match_regex_list)

### Aggregate functions

- [expect_column_distinct_count_to_be_greater_than](#expect_column_distinct_count_to_be_greater_than)
- [expect_column_distinct_count_to_be_less_than](#expect_column_distinct_count_to_be_less_than)
- [expect_column_distinct_count_to_equal_other_table](#expect_column_distinct_count_to_equal_other_table)
- [expect_column_distinct_count_to_equal](#expect_column_distinct_count_to_equal)
- [expect_column_distinct_values_to_be_in_set](#expect_column_distinct_values_to_be_in_set)
- [expect_column_distinct_values_to_contain_set](#expect_column_distinct_values_to_contain_set)
- [expect_column_distinct_values_to_equal_set](#expect_column_distinct_values_to_equal_set)
- [expect_column_max_to_be_between](#expect_column_max_to_be_between)
- [expect_column_mean_to_be_between](#expect_column_mean_to_be_between)
- [expect_column_median_to_be_between](#expect_column_median_to_be_between)
- [expect_column_min_to_be_between](#expect_column_min_to_be_between)
- [expect_column_most_common_value_to_be_in_set](#expect_column_most_common_value_to_be_in_set)
- [expect_column_proportion_of_unique_values_to_be_between](#expect_column_proportion_of_unique_values_to_be_between)
- [expect_column_quantile_values_to_be_between](#expect_column_quantile_values_to_be_between)
- [expect_column_stdev_to_be_between](#expect_column_stdev_to_be_between)
- [expect_column_sum_to_be_between](#expect_column_sum_to_be_between)
- [expect_column_unique_value_count_to_be_between](#expect_column_unique_value_count_to_be_between)

### Multi-column

- [expect_column_pair_values_A_to_be_greater_than_B](#expect_column_pair_values_a_to_be_greater_than_b)
- [expect_column_pair_values_to_be_equal](#expect_column_pair_values_to_be_equal)
- [expect_column_pair_values_to_be_in_set](#expect_column_pair_values_to_be_in_set)
- [expect_compound_columns_to_be_unique](#expect_compound_columns_to_be_unique)
- [expect_multicolumn_sum_to_equal](#expect_multicolumn_sum_to_equal)
- [expect_select_column_values_to_be_unique_within_record](#expect_select_column_values_to_be_unique_within_record)

### Distributional functions

- [expect_column_values_to_be_within_n_moving_stdevs](#expect_column_values_to_be_within_n_moving_stdevs)
- [expect_column_values_to_be_within_n_stdevs](#expect_column_values_to_be_within_n_stdevs)
- [expect_row_values_to_have_data_for_every_n_datepart](#expect_row_values_to_have_data_for_every_n_datepart)

## Documentation

### [expect_column_to_exist](macros/schema_tests/table_shape/expect_column_to_exist.sql)

Expect the specified column to exist.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_to_exist
```

### [expect_row_values_to_have_recent_data](macros/schema_tests/table_shape/expect_row_values_to_have_recent_data.sql)

Expect the model to have rows that are at least as recent as the defined interval prior to the current timestamp. Optionally gives the possibility to apply filters on the results.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_row_values_to_have_recent_data:
      datepart: day
      interval: 1
      row_condition: 'id is not null' #optional
```

### [expect_grouped_row_values_to_have_recent_data](macros/schema_tests/table_shape/expect_grouped_row_values_to_have_recent_data.sql)

Expect the model to have **grouped** rows that are at least as recent as the defined interval prior to the current timestamp.
Use this to test whether there is recent data for each grouped row defined by `group_by` (which is a list of columns) and a `timestamp_column`. Optionally gives the possibility to apply filters on the results.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name : my_model
    tests :
      - dbt_expectations.expect_grouped_row_values_to_have_recent_data:
          group_by: [group_id]
          timestamp_column: date_day
          datepart: day
          interval: 1
          row_condition: "id is not null" #optional
      # or also:
      - dbt_expectations.expect_grouped_row_values_to_have_recent_data:
          group_by: [group_id, other_group_id]
          timestamp_column: date_day
          datepart: day
          interval: 1
          row_condition: "id is not null" #optional
```

### [expect_table_aggregation_to_equal_other_table](macros/schema_tests/table_shape/expect_table_aggregation_to_equal_other_table.sql)

Except an (optionally grouped) expression to match the same (or optionally other) expression in a different table.

*Applies to:* Model, Seed, Source

Simple:

```yaml
tests:
  - dbt_expectations.expect_table_aggregation_to_equal_other_table:
      expression: sum(col_numeric_a)
      compare_model: ref("other_model")
      group_by: [idx]
```

More complex:

```yaml
tests:
  - dbt_expectations.expect_table_aggregation_to_equal_other_table:
      expression: count(*)
      compare_model: ref("other_model")
      compare_expression: count(distinct id)
      group_by: [date_column]
      compare_group_by: [some_other_date_column]
```

or:

```yaml
tests:
  - dbt_expectations.expect_table_aggregation_to_equal_other_table:
      expression: max(column_a)
      compare_model: ref("other_model")
      compare_expression: max(column_b)
      group_by: [date_column]
      compare_group_by: [some_other_date_column]
      row_condition: some_flag=true
      compare_row_condition: some_flag=false
```

**Note**: You can also express a **tolerance** factor, either as an absolute tolerable difference, `tolerance`, or as a tolerable % difference `tolerance_percent` expressed as a decimal (i.e 0.05 for 5%).

### [expect_table_column_count_to_be_between](macros/schema_tests/table_shape/expect_table_column_count_to_be_between.sql)

Expect the number of columns in a model to be between two values.

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_table_column_count_to_be_between:
      min_value: 1 # (Optional)
      max_value: 4 # (Optional)
```

### [expect_table_column_count_to_equal_other_table](macros/schema_tests/table_shape/expect_table_column_count_to_equal_other_table.sql)

Expect the number of columns in a model to match another model.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_column_count_to_equal_other_table:
          compare_model: ref("other_model")
```

### [expect_table_columns_to_not_contain_set](macros/schema_tests/table_shape/expect_table_columns_to_not_contain_set.sql)

Expect the columns in a model not to contain a given list.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_columns_to_not_contain_set:
          column_list: ["col_a", "col_b"]
          transform: upper # (Optional)
```

### [expect_table_columns_to_contain_set](macros/schema_tests/table_shape/expect_table_columns_to_contain_set.sql)

Expect the columns in a model to contain a given list.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_columns_to_contain_set:
          column_list: ["col_a", "col_b"]
          transform: upper # (Optional)
```

### [expect_table_column_count_to_equal](macros/schema_tests/table_shape/expect_table_column_count_to_equal.sql)

Expect the number of columns in a model to be equal to `expected_number_of_columns`.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_column_count_to_equal:
          value: 7
```

### [expect_table_columns_to_match_ordered_list](macros/schema_tests/table_shape/expect_table_columns_to_match_ordered_list.sql)

Expect the columns to exactly match a specified list.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_columns_to_match_ordered_list:
          column_list: ["col_a", "col_b"]
          transform: upper # (Optional)
```

### [expect_table_columns_to_match_set](macros/schema_tests/table_shape/expect_table_columns_to_match_set.sql)

Expect the columns in a model to match a given list.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_columns_to_match_set:
          column_list: ["col_a", "col_b"]
          transform: upper # (Optional)
```

### [expect_table_row_count_to_be_between](macros/schema_tests/table_shape/expect_table_row_count_to_be_between.sql)

Expect the number of rows in a model to be between two values.
*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1 # (Optional)
          max_value: 4 # (Optional)
          group_by: [group_id, other_group_id, ...] # (Optional)
          row_condition: "id is not null" # (Optional)
          strictly: false # (Optional. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_table_row_count_to_equal_other_table](macros/schema_tests/table_shape/expect_table_row_count_to_equal_other_table.sql)

Expect the number of rows in a model match another model.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_row_count_to_equal_other_table:
          compare_model: ref("other_model")
          group_by: [col1, col2] # (Optional)
          compare_group_by: [col1, col2] # (Optional)
          factor: 1 # (Optional)
          row_condition: "id is not null" # (Optional)
          compare_row_condition: "id is not null" # (Optional)
```

### [expect_table_row_count_to_equal_other_table_times_factor](macros/schema_tests/table_shape/expect_table_row_count_to_equal_other_table_times_factor.sql)

Expect the number of rows in a model to match another model times a preconfigured factor.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_row_count_to_equal_other_table_times_factor:
          compare_model: ref("other_model")
          factor: 13
          group_by: [col1, col2] # (Optional)
          compare_group_by: [col1, col2] # (Optional)
          row_condition: "id is not null" # (Optional)
          compare_row_condition: "id is not null" # (Optional)
```

### [expect_table_row_count_to_equal](macros/schema_tests/table_shape/expect_table_row_count_to_equal.sql)

Expect the number of rows in a model to be equal to `expected_number_of_rows`.

*Applies to:* Model, Seed, Source

```yaml
models: # or seeds:
  - name: my_model
    tests:
      - dbt_expectations.expect_table_row_count_to_equal:
          value: 4
          group_by: [group_id, other_group_id, ...] # (Optional)
          row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_be_unique](macros/schema_tests/column_values_basic/expect_column_values_to_be_unique.sql)

Expect each column value to be unique.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_unique:
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_not_be_null](macros/schema_tests/column_values_basic/expect_column_values_to_not_be_null.sql)

Expect column values to not be null.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_not_be_null:
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_be_null](macros/schema_tests/column_values_basic/expect_column_values_to_be_null.sql)

Expect column values to be null.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_null:
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_be_of_type](macros/schema_tests/column_values_basic/expect_column_values_to_be_of_type.sql)

Expect a column to be of a specified data type.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_of_type:
      column_type: date
```

### [expect_column_values_to_be_in_type_list](macros/schema_tests/column_values_basic/expect_column_values_to_be_in_type_list.sql)

Expect a column to be one of a specified type list.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_in_type_list:
      column_type_list: [date, datetime]
```

### [expect_column_values_to_have_consistent_casing](macros/schema_tests/column_values_basic/expect_column_values_to_have_consistent_casing.sql)

Expect a column to have consistent casing. By setting `display_inconsistent_columns` to true, the number of inconsistent values in the column will be displayed in the terminal whereas the inconsistent values themselves will be returned if the SQL compiled test is run.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_have_consistent_casing:
      display_inconsistent_columns: false # (Optional)
```

### [expect_column_values_to_be_in_set](macros/schema_tests/column_values_basic/expect_column_values_to_be_in_set.sql)

Expect each column value to be in a given set.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_in_set:
      value_set: ['a','b','c']
      quote_values: true # (Optional. Default is 'true'.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_be_between](macros/schema_tests/column_values_basic/expect_column_values_to_be_between.sql)

Expect each column value to be between two values.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_between:
      min_value: 0  # (Optional)
      max_value: 10 # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_values_to_not_be_in_set](macros/schema_tests/column_values_basic/expect_column_values_to_not_be_in_set.sql)

Expect each column value not to be in a given set.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_not_be_in_set:
      value_set: ['e','f','g']
      quote_values: true # (Optional. Default is 'true'.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_be_increasing](macros/schema_tests/column_values_basic/expect_column_values_to_be_increasing.sql)

Expect column values to be increasing.

If `strictly: True`, then this expectation is only satisfied if each consecutive value is strictly increasing – equal values are treated as failures.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_increasing:
      sort_column: date_day
      row_condition: "id is not null" # (Optional)
      strictly: true # (Optional for comparison operator. Default is 'true', and it uses '>'. If set to 'false' it uses '>='.)
      group_by: [group_id, other_group_id, ...] # (Optional)
```

### [expect_column_values_to_be_decreasing](macros/schema_tests/column_values_basic/expect_column_values_to_be_decreasing.sql)

Expect column values to be decreasing.

If `strictly=True`, then this expectation is only satisfied if each consecutive value is strictly decreasing – equal values are treated as failures.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_decreasing:
      sort_column: col_numeric_a
      row_condition: "id is not null" # (Optional)
      strictly: true # (Optional for comparison operator. Default is 'true' and it uses '<'. If set to 'false', it uses '<='.)
      group_by: [group_id, other_group_id, ...] # (Optional)
```

### [expect_column_value_lengths_to_be_between](macros/schema_tests/string_matching/expect_column_value_lengths_to_be_between.sql)

Expect column entries to be strings with length between a min_value value and a max_value value (inclusive).

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_value_lengths_to_be_between:
      min_value: 1 # (Optional)
      max_value: 4 # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_value_lengths_to_equal](macros/schema_tests/string_matching/expect_column_value_lengths_to_equal.sql)

Expect column entries to be strings with length equal to the provided value.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_value_lengths_to_equal:
      value: 10
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_match_regex](macros/schema_tests/string_matching/expect_column_values_to_match_regex.sql)

Expect column entries to be strings that match a given regular expression. Valid matches can be found anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat", "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".

Optional (keyword) arguments:

- `is_raw` indicates the `regex` pattern is a "raw" string and should be escaped. The default is `False`.
- `flags` is a string of one or more characters that are passed to the regex engine as flags (or parameters). Allowed flags are adapter-specific. A common flag is `i`, for case-insensitive matching. The default is no flags.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_match_regex:
      regex: "[at]+"
      row_condition: "id is not null" # (Optional)
      is_raw: True # (Optional)
      flags: i # (Optional)
```

### [expect_column_values_to_not_match_regex](macros/schema_tests/string_matching/expect_column_values_to_not_match_regex.sql)

Expect column entries to be strings that do NOT match a given regular expression. The regex must not match any portion of the provided string. For example, "[at]+" would identify the following strings as expected: "fish”, "dog”, and the following as unexpected: "cat”, "hat”.

Optional (keyword) arguments:

- `is_raw` indicates the `regex` pattern is a "raw" string and should be escaped. The default is `False`.
- `flags` is a string of one or more characters that are passed to the regex engine as flags (or parameters). Allowed flags are adapter-specific. A common flag is `i`, for case-insensitive matching. The default is no flags.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_not_match_regex:
      regex: "[at]+"
      row_condition: "id is not null" # (Optional)
      is_raw: True # (Optional)
      flags: i # (Optional)
```

### [expect_column_values_to_match_regex_list](macros/schema_tests/string_matching/expect_column_values_to_match_regex_list.sql)

Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions. Matches can be anywhere in the string.

Optional (keyword) arguments:

- `is_raw` indicates the `regex` pattern is a "raw" string and should be escaped. The default is `False`.
- `flags` is a string of one or more characters that are passed to the regex engine as flags (or parameters). Allowed flags are adapter-specific. A common flag is `i`, for case-insensitive matching. The default is no flags.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_match_regex_list:
      regex_list: ["@[^.]*", "&[^.]*"]
      match_on: any # (Optional. Default is 'any', which applies an 'OR' for each regex. If 'all', it applies an 'AND' for each regex.)
      row_condition: "id is not null" # (Optional)
      is_raw: True # (Optional)
      flags: i # (Optional)
```

### [expect_column_values_to_not_match_regex_list](macros/schema_tests/string_matching/expect_column_values_to_not_match_regex_list.sql)

Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can be anywhere in the string.

Optional (keyword) arguments:

- `is_raw` indicates the `regex` pattern is a "raw" string and should be escaped. The default is `False`.
- `flags` is a string of one or more characters that are passed to the regex engine as flags (or parameters). Allowed flags are adapter-specific. A common flag is `i`, for case-insensitive matching. The default is no flags.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_not_match_regex_list:
      regex_list: ["@[^.]*", "&[^.]*"]
      match_on: any # (Optional. Default is 'any', which applies an 'OR' for each regex. If 'all', it applies an 'AND' for each regex.)
      row_condition: "id is not null" # (Optional)
      is_raw: True # (Optional)
      flags: i # (Optional)
```

### [expect_column_values_to_match_like_pattern](macros/schema_tests/string_matching/expect_column_values_to_match_like_pattern.sql)

Expect column entries to be strings that match a given SQL `like` pattern.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_match_like_pattern:
      like_pattern: "%@%"
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_not_match_like_pattern](macros/schema_tests/string_matching/expect_column_values_to_not_match_like_pattern.sql)

Expect column entries to be strings that do not match a given SQL `like` pattern.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_not_match_like_pattern:
      like_pattern: "%&%"
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_match_like_pattern_list](macros/schema_tests/string_matching/expect_column_values_to_match_like_pattern_list.sql)

Expect the column entries to be strings that match any of a list of SQL `like` patterns.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_match_like_pattern_list:
      like_pattern_list: ["%@%", "%&%"]
      match_on: any # (Optional. Default is 'any', which applies an 'OR' for each pattern. If 'all', it applies an 'AND' for each regex.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_values_to_not_match_like_pattern_list](macros/schema_tests/string_matching/expect_column_values_to_not_match_like_pattern_list.sql)

Expect the column entries to be strings that do not match any of a list of SQL `like` patterns.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_not_match_like_pattern_list:
      like_pattern_list: ["%@%", "%&%"]
      match_on: any # (Optional. Default is 'any', which applies an 'OR' for each pattern. If 'all', it applies an 'AND' for each regex.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_count_to_equal](macros/schema_tests/aggregate_functions/expect_column_distinct_count_to_equal.sql)

Expect the number of distinct column values to be equal to a given value.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_distinct_count_to_equal:
      value: 10
      quote_values: true # (Optional. Default is 'true'.)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_count_to_be_greater_than](macros/schema_tests/aggregate_functions/expect_column_distinct_count_to_be_greater_than.sql)

Expect the number of distinct column values to be greater than a given value.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_distinct_count_to_be_greater_than:
      value: 10
      quote_values: true # (Optional. Default is 'true'.)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_count_to_be_less_than](macros/schema_tests/aggregate_functions/expect_column_less_count_to_be_less_than.sql)

Expect the number of distinct column values to be less than a given value.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_distinct_count_to_be_less_than:
      value: 10
      quote_values: true # (Optional. Default is 'true'.)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_values_to_be_in_set](macros/schema_tests/aggregate_functions/expect_column_distinct_values_to_be_in_set.sql)

Expect the set of distinct column values to be contained by a given set.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_distinct_values_to_be_in_set:
      value_set: ['a','b','c','d']
      quote_values: true # (Optional. Default is 'true'.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_values_to_contain_set](macros/schema_tests/aggregate_functions/expect_column_distinct_values_to_contain_set.sql)

Expect the set of distinct column values to contain a given set.

In contrast to `expect_column_values_to_be_in_set` this ensures not that all column values are members of the given set but that values from the set must be present in the column.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_distinct_values_to_contain_set:
      value_set: ['a','b']
      quote_values: true # (Optional. Default is 'true'.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_values_to_equal_set](macros/schema_tests/aggregate_functions/expect_column_distinct_values_to_equal_set.sql)

Expect the set of distinct column values to equal a given set.

In contrast to `expect_column_distinct_values_to_contain_set` this ensures not only that a certain set of values are present in the column but that these and only these values are present.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_distinct_values_to_equal_set:
      value_set: ['a','b','c']
      quote_values: true # (Optional. Default is 'true'.)
      row_condition: "id is not null" # (Optional)
```

### [expect_column_distinct_count_to_equal_other_table](macros/schema_tests/aggregate_functions/expect_column_distinct_count_to_equal_other_table.sql)

Expect the number of distinct column values to be equal to number of distinct values in another model.

*Applies to:* Model, Column, Seed, Source

This can be applied to a model:

```yaml
models: # or seeds:
  - name: my_model_1
    tests:
      - dbt_expectations.expect_column_distinct_count_to_equal_other_table:
          column_name: col_1
          compare_model: ref("my_model_2")
          compare_column_name: col_2
          row_condition: "id is not null" # (Optional)
          compare_row_condition: "id is not null" # (Optional)
```

or at the column level:

```yaml
models: # or seeds:
  - name: my_model_1
    columns:
      - name: col_1
        tests:
          - dbt_expectations.expect_column_distinct_count_to_equal_other_table:
              compare_model: ref("my_model_2")
              compare_column_name: col_2
              row_condition: "id is not null" # (Optional)
              compare_row_condition: "id is not null" # (Optional)
```

If `compare_model` or `compare_column_name` are no specified, `model` and `column_name` are substituted. So, one could compare distinct counts of two different columns in the same model, or identically named columns in separate models etc.

### [expect_column_mean_to_be_between](macros/schema_tests/aggregate_functions/expect_column_mean_to_be_between.sql)

Expect the column mean to be between a min_value value and a max_value value (inclusive).

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_mean_to_be_between:
      min_value: 0 # (Optional)
      max_value: 2 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_median_to_be_between](macros/schema_tests/aggregate_functions/expect_column_median_to_be_between.sql)

Expect the column median to be between a min_value value and a max_value value (inclusive).

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_median_to_be_between:
      min_value: 0
      max_value: 2
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_quantile_values_to_be_between](macros/schema_tests/aggregate_functions/expect_column_quantile_values_to_be_between.sql)

Expect specific provided column quantiles to be between provided min_value and max_value values.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_quantile_values_to_be_between:
      quantile: .95
      min_value: 0 # (Optional)
      max_value: 2 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_stdev_to_be_between](macros/schema_tests/aggregate_functions/expect_column_stdev_to_be_between.sql)

Expect the column standard deviation to be between a min_value value and a max_value value. Uses sample standard deviation (normalized by N-1).

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_stdev_to_be_between:
      min_value: 0 # (Optional)
      max_value: 2 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_unique_value_count_to_be_between](macros/schema_tests/aggregate_functions/expect_column_unique_value_count_to_be_between.sql)

Expect the number of unique values to be between a min_value value and a max_value value.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_unique_value_count_to_be_between:
      min_value: 3 # (Optional)
      max_value: 3 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_proportion_of_unique_values_to_be_between](macros/schema_tests/aggregate_functions/expect_column_proportion_of_unique_values_to_be_between.sql)

Expect the proportion of unique values to be between a min_value value and a max_value value.

For example, in a column containing [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], there are 4 unique values and 10 total values for a proportion of 0.4.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_proportion_of_unique_values_to_be_between:
      min_value: 0  # (Optional)
      max_value: .4 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_most_common_value_to_be_in_set](macros/schema_tests/aggregate_functions/expect_column_most_common_value_to_be_in_set.sql)

Expect the most common value to be within the designated value set

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_most_common_value_to_be_in_set:
      value_set: [0.5]
      top_n: 1
      quote_values: true # (Optional. Default is 'true'.)
      data_type: "decimal" # (Optional. Default is 'decimal')
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_max_to_be_between](macros/schema_tests/aggregate_functions/expect_column_max_to_be_between.sql)

Expect the column max to be between a min and max value

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_max_to_be_between:
      min_value: 1 # (Optional)
      max_value: 1 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_min_to_be_between](macros/schema_tests/aggregate_functions/expect_column_min_to_be_between.sql)

Expect the column min to be between a min and max value

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_min_to_be_between:
      min_value: 0 # (Optional)
      max_value: 1 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_sum_to_be_between](macros/schema_tests/aggregate_functions/expect_column_sum_to_be_between.sql)

Expect the column to sum to be between a min and max value

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_sum_to_be_between:
      min_value: 1 # (Optional)
      max_value: 2 # (Optional)
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
      strictly: false # (Optional. Default is 'false'. Adds an 'or equal to' to the comparison operator for min/max)
```

### [expect_column_pair_values_A_to_be_greater_than_B](macros/schema_tests/multi-column/expect_column_pair_values_A_to_be_greater_than_B.sql)

Expect values in column A to be greater than column B.

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B:
      column_A: col_numeric_a
      column_B: col_numeric_a
      or_equal: True
      row_condition: "id is not null" # (Optional)
```

### [expect_column_pair_values_to_be_equal](macros/schema_tests/multi-column/expect_column_pair_values_to_be_equal.sql)

Expect the values in column A to be the same as column B.

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_column_pair_values_to_be_equal:
      column_A: col_numeric_a
      column_B: col_numeric_a
      row_condition: "id is not null" # (Optional)
```

### [expect_column_pair_values_to_be_in_set](macros/schema_tests/multi-column/expect_column_pair_values_to_be_in_set.sql)

Expect paired values from columns A and B to belong to a set of valid pairs.

Note: value pairs are expressed as lists within lists

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_column_pair_values_to_be_in_set:
      column_A: col_numeric_a
      column_B: col_numeric_b
      value_pairs_set: [[0, 1], [1, 0], [0.5, 0.5], [0.5, 0.5]]
      row_condition: "id is not null" # (Optional)
```

### [expect_select_column_values_to_be_unique_within_record](macros/schema_tests/multi-column/expect_select_column_values_to_be_unique_within_record.sql)

Expect the values for each record to be unique across the columns listed. Note that records can be duplicated.

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_select_column_values_to_be_unique_within_record:
      column_list: ["col_string_a", "col_string_b"]
      ignore_row_if: "any_value_is_missing" # (Optional. Default is 'all_values_are_missing')
      quote_columns: false # (Optional)
      row_condition: "id is not null" # (Optional)
```

Note:

- `all_values_are_missing` (default) means that rows are excluded where *all* of the test columns are `null`
- `any_value_is_missing` means that rows are excluded where *either* of the test columns are `null`

### [expect_multicolumn_sum_to_equal](macros/schema_tests/multi-column/expect_multicolumn_sum_to_equal.sql)

Expects that sum of all rows for a set of columns is equal to a specific value

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_multicolumn_sum_to_equal:
      column_list: ["col_numeric_a", "col_numeric_b"]
      sum_total: 4
      group_by: [group_id, other_group_id, ...] # (Optional)
      row_condition: "id is not null" # (Optional)
```

### [expect_compound_columns_to_be_unique](macros/schema_tests/multi-column/expect_compound_columns_to_be_unique.sql)

Expect that the columns are unique together, e.g. a multi-column primary key.

*Applies to:* Model, Seed, Source

```yaml
tests:
  - dbt_expectations.expect_compound_columns_to_be_unique:
      column_list: ["date_col", "col_string_b"]
      ignore_row_if: "any_value_is_missing" # (Optional. Default is 'all_values_are_missing')
      quote_columns: false # (Optional)
      row_condition: "id is not null" # (Optional)
```

Note:

- `all_values_are_missing` (default) means that rows are excluded where *all* of the test columns are `null`
- `any_value_is_missing` means that rows are excluded where *either* of the test columns are `null`

### [expect_column_values_to_be_within_n_moving_stdevs](macros/schema_tests/distributional/expect_column_values_to_be_within_n_moving_stdevs.sql)

A simple anomaly test based on the assumption that differences between periods in a given time series follow a log-normal distribution.
Thus, we would expect the logged differences (vs N periods ago) in metric values to be within Z sigma away from a moving average.
By applying a list of columns in the `group_by` parameter, you can also test for deviations within a group.

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:
      date_column_name: date
      period: day # (Optional. Default is 'day')
      lookback_periods: 1 # (Optional. Default is 1)
      trend_periods: 7 # (Optional. Default is 7)
      test_periods: 14 # (Optional. Default is 14)
      sigma_threshold: 3 # (Optional. Default is 3)
      take_logs: true # (Optional. Default is 'true')
      sigma_threshold_upper: x # (Optional. Replace 'x' with a value. Default is 'None')
      sigma_threshold_lower: y # (Optional. Replace 'y' with a value. Default is 'None')
      take_diffs: true # (Optional. Default is 'true')
      group_by: [group_id] # (Optional. Default is 'None')
```

### [expect_column_values_to_be_within_n_stdevs](macros/schema_tests/distributional/expect_column_values_to_be_within_n_stdevs.sql)

Expects (optionally grouped & summed) metric values to be within Z sigma away from the column average

*Applies to:* Column

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_within_n_stdevs:
      group_by: group_id # (Optional. Default is 'None')
      sigma_threshold: 3 # (Optional. Default is 3)
```

### [expect_row_values_to_have_data_for_every_n_datepart](macros/schema_tests/distributional/expect_row_values_to_have_data_for_every_n_datepart.sql)

Expects model to have values for every grouped `date_part`.

For example, this tests whether a model has data for every `day` (grouped on `date_col`) between either:

- The `min`/`max` value of the specified `date_col` (default).
- A specified `test_start_date` and/or `test_end_date`.

*Applies to:* Model, Seed, Source

```yaml
tests:
    - dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart:
        date_col: date_day
        date_part: day # (Optional. Default is 'day')
        row_condition: "id is not null" # (Optional)
        test_start_date: 'yyyy-mm-dd' # (Optional. Replace 'yyyy-mm-dd' with a date. Default is 'None')
        test_end_date: 'yyyy-mm-dd' # (Optional. Replace 'yyyy-mm-dd' with a date. Default is 'None')
        exclusion_condition: statement # (Optional. See details below. Default is 'None')
```

**Notes**:

- `test_end_date` is exclusive, e.g. a test with `test_end_date` value of `'2020-01-05'` will pass if your model has data through `'2021-01-04'`.

- If `test_start_date` or `test_end_date` are not specified, the test automatically determines the `min`/`max` of the specified `date_col` from your data, respectively.
On some platforms, and/or if your table is not partitione on that date column, this may lead to performance issues. In these cases, we recommend setting an explicit date literal. You may also set a "dynamic" date literal via the built-in `modules.datetime` functions:

```yaml
    date_part: day
    test_start_date: '2021-05-01'
    test_end_date: '{{ modules.datetime.date.today() }}'
```

or, for example:

```yaml
    date_part: day
    test_start_date: '2021-05-01'
    test_end_date: '{{ modules.datetime.date.today() - modules.datetime.timedelta(1) }}'
```

Unfortunately, you currently **cannot** use a dynamic SQL date, such as `current_date` or macro from a dbt package such as dbt-date, as the underlying `date_spine` macro expects a date literal.

The `interval` argument will optionally group `date_part` by a given integer to test data presence at a lower granularity, e.g. adding `interval: 7` to the example above will test whether a model has data for each 7-`day` period instead of for each `day`.

Known or expected missing dates can be excluded from the test by setting the `exclusion_criteria` with a valid SQL statement; e.g., adding `exclusion_condition: not(date_day = '2021-10-19')` will ensure that test passes if and only if `date_day = '2021-10-19'` is the only date with missing data. Alternatively, `exclusion_condition: not(date_part(month, date_day) = 12 and date_part(day, date_day) = 25)` will permit data to be missing on the 25th of December (Christmas day) every year.

## ~ Developers Only ~

### Integration Tests

This project contains integration tests for all test macros in a separate `integration_tests` dbt project contained in this repo.

To run the tests:

1. You will need a profile called `integration_tests` in `~/.dbt/profiles.yml` pointing to a writable database. We only support postgres, BigQuery and Snowflake.
2. Then, from within the `integration_tests` folder, run `dbt build` to run the test models in `integration_tests/models/schema_tests/` and run the tests specified in `integration_tests/models/schema_tests/schema.yml`

<img src="https://raw.githubusercontent.com/calogica/dbt-expectations/main/expectations.gif"/>
