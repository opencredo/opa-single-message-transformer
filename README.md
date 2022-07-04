# OPA Single Message Transformer

This is a Single Message Transformer for Apache Kafka.
It uses OPA (Open Policy Agent) to choose which records to filter out based on a specified OPA policy bundle.
It is intended to be use with either a source or sink Kafka Connect component.

It interacts with OPA via the following library by Sangkeon Lee:
https://github.com/sangkeon/java-opa-wasm

The class RemoteBundleFetcher is a modified version of his BundleUtil class.

## Usage Instructions

1. Build using Maven: `mvn package`

2. Set up your Kafka Connect sink or source.

3. Add the OPA Single Message Transformer with-dependencies jar to your sink or source's `plugin.path` folder configured in its config file.

4. Add the configuration for your OPA Single Message Transformer to your sink or source configuration file, e.g.:

```
transforms=opa
transforms.opa.type=OpaTransformer
transforms.opa.bundleFile=/Users/mfarrow/code/opa-single-message-transformer/example/bundle.tar.gz
transforms.opa.filteringEntrypoint=kafka/filter
transforms.opa.maskingEntrypoint=kafka/maskingEntryPoint
```

## Parameters

| Name                | Description                                                                                                   |
|---------------------|---------------------------------------------------------------------------------------------------------------|
| bundleFile          | The file containing the OPA bundle that the transformer should use.                                           |
| bundleUri           | The URI containing the OPA bundle that the transformer should use.                                            |
| filteringEntrypoint | The OPA entrypoint that specifies whether to filter out a record                                              |
| maskingEntrypoint   | The OPA endpoint that specifies either the value to mask a field as, or null if no masking is to be performed |

Either the bundleFile or the bundleUri parameter should be set, but not both.

The example bundle included is currently built manually from the .rego file:

`opa build -t wasm -e kafka/filter rego.rego`

## Masking configuration in rego

Here we are masking several fields.

```
maskingByField = {
    "pii" : "****",
    "phone": "000 0000 0000",
    "address.city": "anon city",
    "pets[*].species": "* * * *",
    "['bob'].street": "a secret street"
}
```


### Masking object fields
'city' is a field on the 'address' object.  It is referenced using dot notation.

### Masking fields on array elements
It is possible to mask fields on arrays.
All elements of the array will have that field masked.
In the example, all elements in the 'pets' array have their species field masked.

### Masking fields that are map values
In the example above, we mask the 'street' field that is on a struct that was the value in a map that associated with the key 'bob'.