---
sidebar_label: known_env
title: common.known_env
---

Defines env variables that `dlt` uses independently of its configuration system

## DLT\_PROJECT\_DIR

The dlt project dir is the current working directory, '.' (current working dir) by default

## DLT\_DATA\_DIR

Gets default directory where pipelines' data (working directories) will be stored

## DLT\_CONFIG\_FOLDER

A folder (path relative to DLT_PROJECT_DIR) where config and secrets are stored

## DLT\_DEFAULT\_NAMING\_NAMESPACE

Python namespace default where naming modules reside, defaults to dlt.common.normalizers.naming

## DLT\_DEFAULT\_NAMING\_MODULE

A module name with the default naming convention, defaults to snake_case

## DLT\_DLT\_ID\_LENGTH\_BYTES

The length of the _dlt_id identifier, before base64 encoding

## DLT\_USE\_JSON

Type of json parser to use, defaults to orjson, may be simplejson

## DLT\_JSON\_TYPED\_PUA\_START

Start of the unicode block within the PUA used to encode types in typed json

## DLT\_PIP\_TOOL

Pip tool used to install deps in Venv

