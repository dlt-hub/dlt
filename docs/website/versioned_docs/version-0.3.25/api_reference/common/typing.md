---
sidebar_label: typing
title: common.typing
---

## StrAny

immutable, covariant entity

## StrStr

immutable, covariant entity

## StrStrStr

immutable, covariant entity

## TFun

any function

## TSecretValue

type: ignore

## TSecretStrValue

type: ignore

## TDataItem

A single data item as extracted from data source

## TDataItems

A single data item or a list as extracted from the data source

## TAnyDateTime

DateTime represented as pendulum/python object, ISO string or unix timestamp

## ConfigValue

value of type None indicating argument that may be injected by config provider

## SupportsVariant Objects

```python
@runtime_checkable
class SupportsVariant(Protocol, Generic[TVariantBase])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L100)

Defines variant type protocol that should be recognized by normalizers

Variant types behave like TVariantBase type (ie. Decimal) but also implement the protocol below that is used to extract the variant value from it.
See `Wei` type declaration which returns Decimal or str for values greater than supported by destination warehouse.

## SupportsHumanize Objects

```python
class SupportsHumanize(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L110)

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L111)

Represents object as dict with a schema loadable by dlt

### asstr

```python
def asstr(verbosity: int = 0) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L115)

Represents object as human readable string

## extract\_inner\_type

```python
def extract_inner_type(hint: Type[Any],
                       preserve_new_types: bool = False) -> Type[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L218)

Gets the inner type from Literal, Optional, Final and NewType

**Arguments**:

- `hint` _Type[Any]_ - Type to extract
- `preserve_new_types` _bool_ - Do not extract supertype of a NewType
  

**Returns**:

- `Type[Any]` - Inner type if hint was Literal, Optional or NewType, otherwise hint

## get\_generic\_type\_argument\_from\_instance

```python
def get_generic_type_argument_from_instance(
        instance: Any, sample_value: Optional[Any]) -> Type[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L250)

Infers type argument of a Generic class from an `instance` of that class using optional `sample_value` of the argument type

Inference depends on the presence of __orig_class__ attribute in instance, if not present - sample_Value will be used

**Arguments**:

- `instance` _Any_ - instance of Generic class
- `sample_value` _Optional[Any]_ - instance of type of generic class, optional
  

**Returns**:

- `Type[Any]` - type argument or Any if not known

## copy\_sig

```python
def copy_sig(
    wrapper: Callable[TInputArgs, Any]
) -> Callable[[Callable[..., TReturnVal]], Callable[TInputArgs, TReturnVal]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/typing.py#L276)

Copies docstring and signature from wrapper to func but keeps the func return value type

