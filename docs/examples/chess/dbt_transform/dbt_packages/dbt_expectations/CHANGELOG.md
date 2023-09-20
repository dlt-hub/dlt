# dbt-expectations v0.9.0

## New Features
* Add count to expect_compound_columns_to_be_unique by @VDFaller in https://github.com/calogica/dbt-expectations/pull/261
* Add duckdb support by @clausherther in https://github.com/calogica/dbt-expectations/pull/271

## Docs
* Update README.md by @mbkupfer in https://github.com/calogica/dbt-expectations/pull/268

# dbt-expectations v0.8.5

## New Features

* Add support for regex flags for BigQuery by @lookslikeitsnot in https://github.com/calogica/dbt-expectations/pull/253

# dbt-expectations v0.8.4

## Fixes
* escape quotes in `expect_column_values_to_be_in_type_list` by @RobbertDM in https://github.com/calogica/dbt-expectations/pull/251
* Fix `postgres__regexp_instr` not validating regex #249 by @lookslikeitsnot in https://github.com/calogica/dbt-expectations/pull/250

## Under The Hood
* added negative tests for regex email test to make sure those tests are failing when they should

# dbt-expectations v0.8.3

## New Features
* Add optional flags argument to regex tests by @tconbeer in https://github.com/calogica/dbt-expectations/pull/247

## Docs
* Update docs site by @clausherther in https://github.com/calogica/dbt-expectations/pull/235

## Under The Hood
* Add explicit reference to dbt.length() by @clausherther in https://github.com/calogica/dbt-expectations/pull/234
* Disabling SQLFluff by @clausherther in https://github.com/calogica/dbt-expectations/pull/242

# dbt-expectations v0.8.2

## Fixes
* Fix dangling datediff reference by @clausherther in https://github.com/calogica/dbt-expectations/pull/233

# dbt-expectations v0.8.1

## Fixes
* Make type macro calls fully qualified by @clausherther in https://github.com/calogica/dbt-expectations/pull/232
* Clean up quote values defaults by @clausherther in https://github.com/calogica/dbt-expectations/pull/231

## Documentation
* Fix indentation of examples by @clausherther in https://github.com/calogica/dbt-expectations/pull/219
* Update README by @clausherther in https://github.com/calogica/dbt-expectations/pull/220
* Update logo paths by @clausherther in https://github.com/calogica/dbt-expectations/pull/221

## Additions
* Add expect_table_aggregation_to_equal_other_table by @clausherther in https://github.com/calogica/dbt-expectations/pull/224
* Adds group by to expect_table_row_count_to_equal_other_table by @clausherther in https://github.com/calogica/dbt-expectations/pull/225

# dbt-expectations v0.8.0

## Breaking Changes
NOTE: including this package in your project will no longer auto-include `dbt-utils`!
* Remove references to dbt_utils by @clausherther in https://github.com/calogica/dbt-expectations/pull/217

# dbt-expectations v0.7.0

## Breaking Changes
* Fix boolean logic in `expect_compound_columns_to_be_unique` `ignore_row_if`  by @clausherther in https://github.com/calogica/dbt-expectations/pull/202
  * Fixed issue https://github.com/calogica/dbt-expectations/issues/200 raised by @mcannamela
* Refactor ignore_row_if logic by @clausherther in https://github.com/calogica/dbt-expectations/pull/204

## Under The Hood
* Add SQL Fluff support by @clausherther in https://github.com/calogica/dbt-expectations/pull/198

# dbt-expectations v0.6.1

## New Features
* Add expect_column_distinct_count_to_be_less_than.sql by @dylanrigal in https://github.com/calogica/dbt-expectations/pull/193
* Add group_by option to expect_column_values_to_be_within_n_moving_stdevs by @clausherther in https://github.com/calogica/dbt-expectations/pull/182

## Fixes
* Add option to escape raw strings in RegEx functions by @clausherther in https://github.com/calogica/dbt-expectations/pull/191

## New Contributors
* @dylanrigal made their first contribution in https://github.com/calogica/dbt-expectations/pull/193

# dbt-expectations v0.6.0

## Breaking Changes
* Requires dbt 1.2.x (via https://github.com/calogica/dbt-expectations/pull/189)
* Migrate to dbt-utils 0.9.0 and dbt-date 0.6.0 by @clausherther in https://github.com/calogica/dbt-expectations/pull/189

# dbt-expectations v0.5.8

## Fixes
* Fixed timestamp comparisons in expect_grouped_row_values_to_have_recent_data by @clausherther in https://github.com/calogica/dbt-expectations/pull/179

# dbt-expectations v0.5.7

## Documentation
* Add example for dynamic date params in expect_row_values_to_have_data_for_every_n_datepart by @clausherther in https://github.com/calogica/dbt-expectations/pull/174

## Fixes
* Fix take_diff syntax in moving stddev test by @karanhegde in https://github.com/calogica/dbt-expectations/pull/178

# dbt-expectations v0.5.6

**Patch Release**

## Fixes
* Fix `expect_compound_columns_to_be_unique` to properly handle `ignore_row_if` by @clausherther in https://github.com/calogica/dbt-expectations/pull/171


# dbt-expectations v0.5.5

## New Features
* Add automated integration testing with CircleCI in https://github.com/calogica/dbt-expectations/pull/161
* Show `group_by` columns in validation errors for column increasing test by @dluftspring in https://github.com/calogica/dbt-expectations/pull/158
* Add `exclusion_condition` to `expect_row_values_to_have_data_for_every_n_datepart` by @gofford in https://github.com/calogica/dbt-expectations/pull/141

## Fixes
* Set flakey integration tests to `warn` in https://github.com/calogica/dbt-expectations/pull/162


# dbt-expectations v0.5.4

## New Features
* Adds test for column presence by @rcaddell in https://github.com/calogica/dbt-expectations/pull/149

## Fixes
* Fix emails.sql by @clausherther in https://github.com/calogica/dbt-expectations/pull/153
* Fix expect_row_values_to_have_recent_data issues on bigquery by @clausherther in https://github.com/calogica/dbt-expectations/pull/147

## New Contributors
* @rcaddell made their first contribution in https://github.com/calogica/dbt-expectations/pull/149

# dbt-expectations v0.5.3

## New Features
* Add `group_by` parameter to `expect_column_values_to_be_increasing` and `expect_column_values_to_be_decreasing` ([#146](https://github.com/calogica/dbt-expectations/pull/146) @Lucasthenoob)


# dbt-expectations v0.5.2

## Fixes
* Fix `expect_row_values_to_have_recent_data` to use current timestamp by @MrJoosh in https://github.com/calogica/dbt-expectations/pull/145 (also fixes https://github.com/calogica/dbt-expectations/issues/104)

## New Features
* Add new `expect_column_values_to_have_consistent_casing` test by @agusfigueroa-htg in https://github.com/calogica/dbt-expectations/pull/138

## Doc Updates 💌
* Update README.md by @kdw2126 in https://github.com/calogica/dbt-expectations/pull/134
* Fix documentation to note DBT 1.0.0 compatibility by @kdw2126 in https://github.com/calogica/dbt-expectations/pull/136

## New Contributors
* @kdw2126 made their first contribution in https://github.com/calogica/dbt-expectations/pull/134
* @agusfigueroa-htg made their first contribution in https://github.com/calogica/dbt-expectations/pull/138
* @MrJoosh made their first contribution in https://github.com/calogica/dbt-expectations/pull/145


# dbt-expectations v0.5.1

## Fixes
* Add better support for Redshift by typing implicit `varchar` fields explicitly to strings. ([#131](https://github.com/calogica/dbt-expectations/pull/131) [#132](https://github.com/calogica/dbt-expectations/pull/132))


# dbt-expectations v0.5.0
* adds full support for dbt 1.x without backwards compatability
* supports `dbt-date 0.5.0`, which supports `dbt-utils 0.8.0`

# dbt-expectations v0.4.7
* Patch: adds support for dbt 1.x

# dbt-expectations v0.4.6

## What's Changed
* Append missing optional parameters documentation to README.md by @makotonium in https://github.com/calogica/dbt-expectations/pull/124
* Fix missing group_by default value in string_matching macros by @samantha-guerriero-cko in https://github.com/calogica/dbt-expectations/pull/126

## New Contributors
* @makotonium made their first contribution in https://github.com/calogica/dbt-expectations/pull/124
* @samantha-guerriero-cko made their first contribution in https://github.com/calogica/dbt-expectations/pull/126
# dbt-expectations v0.4.5

## Fixes
* Fix missing group by default value in string_matching macros  ([#126](https://github.com/calogica/dbt-expectations/pull/126) by [@samantha-guerriero-cko](https://github.com/samantha-guerriero-cko))

## Doc Updates
* Append missing optional parameters documentation to README.md ([#124](https://github.com/calogica/dbt-expectations/pull/124) by [@makotonium](https://github.com/makotonium))

# dbt-expectations v0.4.5

## Features
* Add an optional argument to allow for intervals of `date_part` in `expect_row_values_to_have_data_for_every_n_datepart`. ([#110](https://github.com/calogica/dbt-expectations/pull/110) by [@lewisarmistead](https://github.com/lewisarmistead))

## Fixes

* Fixed a regression introduced in 0.4.3 that made `expect_table_columns_to_match_ordered_list` incomatible with Redshift ([#123](https://github.com/calogica/dbt-expectations/pull/123) by [@mirosval](https://github.com/mirosval))

# dbt-expectations v0.4.4

## Fixes

* Replaced hardcoded value in `expect_column_to_exist` with mapping call to provided transform filter and join to reduce list back to single value. ([#118](https://github.com/calogica/dbt-expectations/pull/118) [@UselessAlias](https://github.com/UselessAlias))


# dbt-expectations v0.4.3

## Fixes
* Fixes incompatibility on Snowflake with use of `row_number()` without `order by` in `expect_table_columns_to_match_ordered_list`([#112](https://github.com/calogica/dbt-expectations/pull/112))

## Features

## Under the hood
* Supports dbt 0.21.x

# dbt-expectations v0.4.2

## Features
 * Added `row_condition` to `expect_grouped_row_values_to_have_recent_data` and `expect_row_values_to_have_recent_data` to allow for partition filtering before applying the recency test ([#106](https://github.com/calogica/dbt-expectations/pull/106) w/ [@edbizarro](https://github.com/edbizarro))

## Under the hood
* Converted Jinja set logic to SQL joins to make it easier to follow and iterate in the future ([#108](https://github.com/calogica/dbt-expectations/pull/108))

# dbt-expectations v0.4.1

## Fixes
* `expect_table_columns_to_match_list` remove `''` to leave columns as numbers ([#98](https://github.com/calogica/dbt-expectations/issues/98))

* `expect_table_columns_to_match_ordered_list` now explicitly casts the column list to a string type ([#99](https://github.com/calogica/dbt-expectations/issues/99))

* Fixes regex matching tests for Redshift by adding a Redshift specific adapter macro in `regexp_instr` ([#99](https://github.com/calogica/dbt-expectations/pull/102) @mirosval)

# dbt-expectations v0.4.0

## Breaking Changes

* Requires `dbt >= 0.20`

* Requires `dbt-date >= 0.4.0`

* Updates test macros to tests to support `dbt >= 0.20`

* Updates calls to adapter.dispatch to support `dbt >= 0.20` (see [Changes to dispatch in dbt v0.20 #78](https://github.com/calogica/dbt-expectations/issues/78))

# dbt-expectations v0.3.7

* Fix join in `expect_column_values_to_be_in_set` ([#91](https://github.com/calogica/dbt-expectations/pull/91) @ahmedrad)
* Add support for Redshift `random` function in `rand` macro ([#92](https://github.com/calogica/dbt-expectations/pull/92) @ahmedrad)

# dbt-expectations v0.3.6

* Remove unnecessary macro to fix issue with 0.19.2 ([#88](https://github.com/calogica/dbt-expectations/pull/88))

# dbt-expectations v0.3.5

## Features
* Added a new macro, `expect_row_values_to_have_data_for_every_n_datepart`, which tests whether a model has values for every grouped `date_part`.


    For example, this tests whether a model has data for every `day` (grouped on `date_col`) from either a specified `start_date` and `end_date`, or for the `min`/`max` value of the specified `date_col`.


    ```yaml
    tests:
        - dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart:
            date_col: date_day
            date_part: day
    ```

## Fixes

* Updated description of type check tests ([#84](https://github.com/calogica/dbt-expectations/pull/84) @noel)

* Fixed `join` syntax because Twitter induced guilt: https://twitter.com/emilyhawkins__/status/1400967270537564160

* Bump version of dbt-date to `< 0.4.0` ([#85](https://github.com/calogica/dbt-expectations/issues/85))


# dbt-expectations v0.3.4

## Features

* Added support for optional `min_value` and `max_value` parameters to all`*_between_*` tests. ([#70](https://github.com/calogica/dbt-expectations/pull/70))

* Added support for `strictly` parameter to `between` tests. If set to `True`, `striclty` changes the operators `>=` and `<=` to`>` and `<`.

    For example, while

    ```yaml
    dbt_expectations.expect_column_stdev_to_be_between:
        min_value: 0
    ```

    evaluates to `>= 0`,

    ```yaml
    dbt_expectations.expect_column_stdev_to_be_between:
        min_value: 0
        strictly: True
    ```

    evaluates to `> 0`.
    ([#72](https://github.com/calogica/dbt-expectations/issues/72), [#74](https://github.com/calogica/dbt-expectations/pull/74))

## Fixes

* Corrected a typo in the README ([#67](https://github.com/calogica/dbt-expectations/pull/67))

## Under the hood

* Refactored `get_select` function to generate SQL grouping more explicitly ([#63](https://github.com/calogica/dbt-expectations/pull/63)))

* Added dispatch call to `expect_table_row_count_to_equal` to make it easier to shim macros for the tsql-utils package ([#64](https://github.com/calogica/dbt-expectations/pull/64) Thanks [@alieus](https://github.com/alieus)!)
