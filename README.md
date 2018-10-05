TSHISTORY ALIAS
================

# Purpose

This [tshistory][tshistory] component provides fixed mechanisms to build
computed series.

Using `csv` definition files, one can define:

* filters for `outliers` elimination (fixed min/max values)

* composition of virtual (or alias) series by linear combination (also
  named `arithmetic` aliases)

* composition of aliases series by stacking series onto each others
  (also named `priority` aliases)

Let's explain a bit more the last two items.

An `arithmetic` alias is defined by a list of series to be added to
form a new virtual series.

For instance one could define the solar output of the UE as the sum
of all member countries solar output.

A `priority` alias is defined by a list of series, the first series
providing baseline values, and the nexts completing missing values of
the previous combination (up to the baseline).

For instance one could use the realised solar output as a baseline of
a `priority` which would be completed by a forecast series.

For both `arithmetic` and `priority` aliases, each individual series
can also be multiplied by a scalar (this helps with unit conversions).

Either `arithmetic` or `priority` aliases can be made of base series
(internally referred to as `primary` series) or other aliases,
recursively.

It is not possible to `.insert` data into an alias.

[tshistory]: https://bitbucket.org/pythonian/tshistory


# API

A few api calls are added to the `tshistory` base:

* `.add_bounds` to define an `outliers` filter (with optional min/max
  parameters)

* `.build_arithmetic` to define an `arithmetic` alias

* `.build_priority` to define a `priority` alias


# Command line

A few commands are provided to deal with the specifics of aliases. The
`tsh` command carries them. The output below shows only the specific
aliases subcommands:

```shell
$ tsh
Usage: tsh [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  audit-aliases             perform a visual audit of aliases
  register-arithmetic       register arithmetic timeseries aliases
  register-outliers         register outlier definitions
  register-priorities       register priorities timeseries aliases
  remove-alias              remove singe alias
  reset-aliases             remove aliases wholesale (all or per type...
  verify-aliases            verify aliases wholesale (all or per type...
```
