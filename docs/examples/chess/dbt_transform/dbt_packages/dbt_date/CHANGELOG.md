# dbt-date v0.8.0
## New Features
* Add support for dbt-duckdb to the dbt_date package by @jwills in https://github.com/calogica/dbt-date/pull/105

## Docs
* Update README.md by @JordanZimmitti in https://github.com/calogica/dbt-date/pull/99
* Update README.md by @tgmof in https://github.com/calogica/dbt-date/pull/104

**Full Changelog**: https://github.com/calogica/dbt-date/compare/0.7.2...0.8.0

# dbt-date v0.7.2

## Fixes
* Update refs to dbt-core macros by @clausherther in https://github.com/calogica/dbt-date/pull/97
* Add dbt prefix to current_timestamp macro by @GtheSheep in https://github.com/calogica/dbt-date/pull/98

# dbt-date v0.7.1

* Fix calls to last_month and next_month by @clausherther in https://github.com/calogica/dbt-date/pull/95

# dbt-date v0.7.0

## Breaking Changes
* Removed dependency on dbt-utils by @clausherther in https://github.com/calogica/dbt-date/pull/91

# dbt-date v0.6.3
* Switch to dbt-core's implementation of current_timestamp() by @clausherther in https://github.com/calogica/dbt-date/pull/88

# dbt-date v0.6.2
* Simplify convert from `source_tz` by @clausherther in https://github.com/calogica/dbt-date/pull/86
* Fix issue of convert_timezone converting timestamps twice for timestamp_tz date types (issue #85, #77, #76  )

# dbt-date v0.6.1
* New: added `round_timestamp` macro by @jpmmcneill in https://github.com/calogica/dbt-date/pull/84

# dbt-date v0.6.0
* Move to dbt-utils 0.9.0
* Remove references to deprecated dbt-utils cross-db macros by @clausherther in https://github.com/calogica/dbt-date/pull/79

# dbt-date v0.5.7
* Add github actions workflow by @clausherther in https://github.com/calogica/dbt-date/pull/69
* Fix Redshift timezone conversion macro by @wellykachtel in https://github.com/calogica/dbt-date/pull/71

# dbt-date v0.5.6
* Fix missing bracket by @clausherther in https://github.com/calogica/dbt-date/pull/66

# dbt-date v0.5.5
* Fix README table of contents' links by @coisnepe [#61](https://github.com/calogica/dbt-date/pull/61)
* Fix timezone conversion macro on redshift by @msnidal [#63](https://github.com/calogica/dbt-date/pull/63)

# dbt-date v0.5.4
* Updated Documentation [#60](https://github.com/calogica/dbt-date/pull/60)

# dbt-date v0.5.3
* Allow negative shift year for fiscal periods ([#57](https://github.com/calogica/dbt-date/issues/57) [@boludo00](https://github.com/boludo00))

# dbt-date v0.5.2
* Fix [#55](https://github.com/calogica/dbt-date/issues/55) by removing dead macro.

# dbt-date v0.5.1
* Fix `week_start` and `week_end` on Snowflake ([#53](https://github.com/calogica/dbt-date/issues/53), [#54](https://github.com/calogica/dbt-date/pull/54))

# dbt-date v0.5.0
* Deprecates support for dbt < 1.0.0

# dbt-date v0.4.2
## Under the hood
* Patch: adds support for dbt 1.x

# dbt-date v0.4.1

## Under the hood
* Support for dbt 0.21.x

# dbt-date v0.4.0

## Breaking Changes

* Updates calls to adapter.dispatch to support `dbt >= 0.20` (see [Changes to dispatch in dbt v0.20 #34](https://github.com/calogica/dbt-date/issues/34))

* Requires `dbt >= 0.20`

## Under the hood

* Adds tests for timestamp and timezone macros (previously untested, new dbt version highlighted that)

# dbt-date v0.3.1

*Patch release*

## Fixes

* Fixed a bug in `snowflake__from_unixtimestamp` that prevented the core functionaility from being called ([#38](https://github.com/calogica/dbt-date/pull/38) by @swanderz)

## Under the hood

* Simplified `join` syntax ([#36](https://github.com/calogica/dbt-date/pull/36))

# dbt-date v0.3.0

## Breaking Changes

* Switched `day_of_week` column in `get_date_dimension` from ISO to *not* ISO to align with the rest of the package. [#33](https://github.com/calogica/dbt-date/pull/33) (@davesgonechina)

## Features

* Added `day_of_week_iso` column to `get_date_dimension` [#33](https://github.com/calogica/dbt-date/pull/33) (@davesgonechina)

* Added `prior_year_iso_week_of_year` column to `get_date_dimension`

## Fixes

* Refactored Snowflake's `day_name` to not be ISO dependent [#33](https://github.com/calogica/dbt-date/pull/33) (@davesgonechina)

* Fixed data types for `day_of_*` attributes in Redshift ([#28](https://github.com/calogica/dbt-date/pull/28) by @sparcs)

* Fixed / added support for date parts other than `day` in `get_base_dates` ([#30](https://github.com/calogica/dbt-date/pull/30))

## Under the hood

* Making it easier to shim macros for other platforms ([#27](https://github.com/calogica/dbt-date/pull/27) by @swanderz)
