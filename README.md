# OPA Single Message Transformer

This is a Single Message Transformer for Apache Kafka.
It uses OPA (Open Policy Agent) to choose which records to filter out based on a specified OPA policy bundle.
It is intended to be use with either a source or sink Kafka Connect component.

When the bundle is configured using a filesystem path, it listens to the filesystem for changes.  There can be a few seconds of delay.
When the bundle is configured using a URI, it polls that URI at an interval specified in the `pollBundleUriFrequencySeconds` configuration property.

## Usage Instructions

1. Build using Maven `mvn package` to generate a JAR containing the component and all dependencies.

2. Follow the very approachable [Kafka Quickstart](https://kafka.apache.org/quickstart) tutorial's steps 1-3 to install Zookeeper and Kafka and to create a topic.  

3. Create a worker config file for a Kafka Connect Worker.

The [Kafka Connect docs](https://docs.confluent.io/home/connect/self-managed/userguide.html) are essential for a basic idea of setting this up.

Here are some values that worked for my simple case:
```
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=true
internal.value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=5000
consumer.metadata.max.age.ms=10000
plugin.path=[some directory where you'll put any sink, source and transform JARs]
```

4. Add the OPA Single Message Transformer with-dependencies jar to your sink or source's `plugin.path` folder configured in its config file.


5. Build an OPA webassembly bundle by writing a rego file and running ```opa build -t wasm -e kafka/filter -e kafka/maskingByField [rego-file] ``` 

or copy this one from the component's repository to your local drive:
https://github.com/opencredo/opa-single-message-transformer/blob/main/src/test/resources/testRego/bundle.tar.gz


6. Add the configuration for your OPA Single Message Transformer to your sink or source configuration file.  For example, here I am setting up a file source to read from the folder referred to under `fs.uris`:

```
name=local-file-source
connector.class=com.github.mmolimar.kafka.connect.fs.FsSourceConnector
fs.uris=file:///Users/mfarrow/kafka_2.13-3.2.0/test-data/
policy.class=com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy
file_reader.class=com.github.mmolimar.kafka.connect.fs.file.reader.JsonFileReader
tasks.max=1
topic=connect-test

transforms=opa
transforms.opa.type=OpaTransformer
transforms.opa.bundleFile=[path to your bundle that you just generated or copied]
transforms.opa.filteringEntrypoint=kafka/filter
transforms.opa.maskingEntrypoint=kafka/maskingEntryPoint
```

I also had to add the file source's jar (https://www.confluent.io/hub/mmolimar/kafka-connect-fs) to my worker's plugin-path directory.


7. Run the Kafka Connect pipeline, in my case:
```
bin/connect-standalone.sh config/worker.properties config/file-source.properties
```


8. Run kafka-console-consumer to listen to messages that the pipeline writes to your Kafka topic: 
```
bin/kafka-console-consumer.sh --topic connect-test --from-beginning --bootstrap-server localhost:9092
```

## Parameters

| Name                          | Description                                                                                                   |
|-------------------------------|---------------------------------------------------------------------------------------------------------------|
| bundleFile                    | The file containing the OPA bundle that the transformer should use.                                           |
| bundleUri                     | The URI containing the OPA bundle that the transformer should use.                                            |
| filteringEntrypoint           | The OPA entrypoint that specifies whether to filter out a record                                              |
| maskingEntrypoint             | The OPA endpoint that specifies either the value to mask a field as, or null if no masking is to be performed |
| pollBundleUriFrequencySeconds | How often to poll the bundle URI, in seconds. Defaults to 60. Setting to zero disables polling.               |

Either the `bundleFile` or the `bundleUri` parameter should be set, but not both.

We have included a variety of OPA bundle examples for different test purposes, and you can find in `/src/test/resources` folder.
For each example we have compiled the rego files into a WebAssembly bundle so that tests can use them. This is how you can compile the rego file:
```
opa build -t wasm -e kafka/filter -e kafka/maskingByField rego.rego
```

# Configuration Example 

Here is an example where we are masking several fields and filtering records.

```
package kafka

default filter = false

filter {
    input.personal == true
}

maskingByField = {
    "pii" : "****",
    "phone": "000 0000 0000",
    "address.city": "anon city",
    "pets[*].species": "* * * *",
    "['bob'].street": "a secret street"
}
```

### Filtering records
The 'filter' entrypoint has a rego string that tells the component to filter for records where the 'personal' field is set to true.
Of course, any valid rego expression could have been used here. 

### Masking object fields
'city' is a field on the 'address' object.  It is referenced using dot notation.

### Masking fields on array elements
It is possible to mask fields on arrays.
All elements of the array will have that field masked.
In the example, all elements in the 'pets' array have their species field masked.

### Masking fields that are map values
In the example above, we mask the 'street' field that is on a struct that was the value in a map that associated with the key 'bob'.

# Acknowledgements
This component interacts with OPA via the following library by Sangkeon Lee:
https://github.com/sangkeon/java-opa-wasm

The class RemoteBundleFetcher is a modified version of his BundleUtil class.
