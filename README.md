# OPA Single Message Transformer

This is a Single Message Transformer for Apache Kafka.
It uses OPA (Open Policy Agent) to choose which records to filter out based on a specified OPA policy bundle.
It is intended to be use with either a source or sink Kafka Connect component.

## Usage Instructions

1. Build using Maven: `mvn package`

2. Set up your Kafka Connect sink or source.

3. Add the OPA Single Message Transformer with-dependencies jar to your sink or source's `plugin.path`.

4. Add your OPA Single Message Transformer configuration to your sink or source configuration file, e.g.:

```
transforms=opa
transforms.opa.type=OpaTransformer
transforms.opa.opaBundlePath=/Users/mfarrow/code/opa-single-message-transformer/example/bundle.tar.gz
```

## Parameters

| Name          | Description                                                |
|---------------|------------------------------------------------------------|
| opaBundlePath | The path to the OPA bundle that the transfomer should use. |


The example bundle included is currently built manually from the rego file:

`opa build -t wasm -e kafka/filter rego.rego`