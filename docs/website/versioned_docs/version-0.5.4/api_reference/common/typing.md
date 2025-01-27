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

## TLoaderFileFormat

known loader file formats

## ConfigValueSentinel Objects

```python
class ConfigValueSentinel(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L113)

Class to create singleton sentinel for config and secret injected value

## ConfigValue

Config value indicating argument that may be injected by config provider. Evaluates to None when type checking

## SecretValue

Secret value indicating argument that may be injected by config provider. Evaluates to None when type checking

## SupportsVariant Objects

```python
@runtime_checkable
class SupportsVariant(Protocol, Generic[TVariantBase])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L138)

Defines variant type protocol that should be recognized by normalizers

Variant types behave like TVariantBase type (ie. Decimal) but also implement the protocol below that is used to extract the variant value from it.
See `Wei` type declaration which returns Decimal or str for values greater than supported by destination warehouse.

## SupportsHumanize Objects

```python
class SupportsHumanize(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L148)

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L149)

Represents object as dict with a schema loadable by dlt

### asstr

```python
def asstr(verbosity: int = 0) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L153)

Represents object as human readable string

## get\_type\_name

```python
def get_type_name(t: Type[Any]) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L158)

Returns a human-friendly name of type `t`

## is\_callable\_type

```python
def is_callable_type(hint: Type[Any]) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L168)

Checks if `hint` is callable: a function or callable class. This function does not descend
into type arguments ie. if Union, Literal or NewType contain callables, those are ignored

## get\_literal\_args

```python
def get_literal_args(literal: Type[Any]) -> List[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L263)

Recursively get arguments from nested Literal types and return an unified list.

## extract\_inner\_type

```python
def extract_inner_type(hint: Type[Any],
                       preserve_new_types: bool = False,
                       preserve_literal: bool = False) -> Type[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L324)

Gets the inner type from Literal, Optional, Final and NewType

**Arguments**:

- `hint` _Type[Any]_ - Type to extract
- `preserve_new_types` _bool_ - Do not extract supertype of a NewType
  

**Returns**:

- `Type[Any]` - Inner type if hint was Literal, Optional or NewType, otherwise hint

## get\_all\_types\_of\_class\_in\_union

```python
def get_all_types_of_class_in_union(hint: Any,
                                    cls: TAny,
                                    with_superclass: bool = False
                                    ) -> List[TAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L350)

if `hint` is an Union that contains classes, return all classes that are a subclass or (optionally) superclass of cls

## is\_generic\_alias

```python
def is_generic_alias(tp: Any) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L361)

Tests if type is a generic alias ie. List[str]

## is\_subclass

```python
def is_subclass(subclass: Any, cls: Any) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L371)

Return whether 'cls' is a derived from another class or is the same class.

Will handle generic types by comparing their origins.

## get\_generic\_type\_argument\_from\_instance

```python
def get_generic_type_argument_from_instance(instance: Any,
                                            sample_value: Optional[Any] = None
                                            ) -> Type[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L386)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/typing.py#L418)

Copies docstring and signature from wrapper to func but keeps the func return value type

