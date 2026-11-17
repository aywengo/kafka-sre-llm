# Schema-registry wire format, the magic byte, and "Unknown magic byte!" errors

| Field | Value |
|---|---|
| Product | Kafka clients that use the Confluent-compatible schema-registry serializers (Avro, JSON Schema, Protobuf) |
| Doc version | Serializer source code of confluentinc/schema-registry at tag v8.4.1 (commit 2e7a638f235f5ef18acff5556652e6a17a5b2f0c), Apache-2.0 modules only. Cross-checks: Apicurio Registry 3.3.4, Karapace 6.2.4, Apache Kafka 4.3 docs (apache/kafka tag 4.3.2) |
| Sources | Short names used below: **SR** = https://github.com/confluentinc/schema-registry/tree/v8.4.1 ; **Apicurio** = https://github.com/Apicurio/apicurio-registry/tree/3.3.4 ; **Karapace** = https://github.com/Aiven-Open/karapace/tree/6.2.4 ; **Kafka docs** = https://github.com/apache/kafka/tree/4.3.2/docs . Full paths, commits and line ranges: [Sources](#sources). |
| License | Derived from: confluentinc/schema-registry modules `schema-serializer`, `avro-serializer`, `json-schema-serializer`, `protobuf-serializer`, © Confluent Inc., Apache License 2.0 (each module's `LICENSE` and each file's header; the repository root and `core/` are under the Confluent Community License and were not used). Apicurio Registry, Apache License 2.0. Karapace, © Aiven Ltd, Apache License 2.0. Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; identifiers, error strings and commands quoted verbatim. |
| Retrieved | 2026-11-16 |

> **Version note.** The layout and exceptions below are read from the serializer source code at v8.4.1, not from product documentation. No other serializer version was read, and no Confluent website text was used.

> **Not Kafka's record magic.** Kafka's record batch has its own field `magic: int8 (current magic value is 2)`, and brokers have the metric `kafka.server:type=BrokerTopicMetrics,name=InvalidMagicNumberRecordsPerSec`. The magic byte in this runbook is something else: the first byte of the serialized key or value (or of a schema-id record header), written by the client-side serializer.
> — *Source:* Kafka docs `implementation/message-format.md` L31-L61, `operations/monitoring.md` L330-L334; SR `SchemaId.java` L78-L113

## Wire format (reference)

| Where the schema id goes | First byte | Next bytes | Then | Written by |
|---|---|---|---|---|
| Prefix of the serialized key or value (default) | `MAGIC_BYTE_V0 = 0x0` | 4 bytes (`ID_SIZE = 4`): the integer schema id, written with `ByteBuffer.allocate(ID_SIZE).putInt(getId())` | Protobuf message indexes if set, then the serialized data | `PrefixSchemaIdSerializer` (calls `SchemaId.idToBytes()`) |
| Record header `__key_schema_id` (key) or `__value_schema_id` (value) | `MAGIC_BYTE_V1 = 0x1` | 16 bytes: the schema GUID, most significant 8 bytes first | Protobuf message indexes if set. The key or value is the serialized data with no prefix | `HeaderSchemaIdSerializer` (calls `SchemaId.guidToBytes()`) |

— *Source:* SR `SchemaId.java` L33-L37, L78-L113; `PrefixSchemaIdSerializer.java` L23-L39; `HeaderSchemaIdSerializer.java` L27-L50; `AbstractKafkaProtobufSerializer.java` L196-L208

- **Defaults.** `key.schema.id.serializer` and `value.schema.id.serializer`: `PrefixSchemaIdSerializer`. `key.schema.id.deserializer` and `value.schema.id.deserializer`: `DualSchemaIdDeserializer`. The Avro, JSON Schema and Protobuf config classes all extend `AbstractKafkaSchemaSerDeConfig`, which defines these keys.
  — *Source:* SR `AbstractKafkaSchemaSerDeConfig.java` L49, L359-L388, L502-L511; `KafkaAvroSerializerConfig.java` L24; `KafkaAvroDeserializerConfig.java` L26; `KafkaJsonSchemaSerializerConfig.java` L28; `KafkaJsonSchemaDeserializerConfig.java` L24; `KafkaProtobufSerializerConfig.java` L27; `KafkaProtobufDeserializerConfig.java` L24
- **Reading.** `SchemaId.fromBytes` reads one byte. `0x0`: the next 4 bytes are the id (`buffer.getInt()`). `0x1`: the next 16 bytes are the GUID. Anything else: `throw new IllegalArgumentException("Unknown magic byte!")`. Message indexes are read next only when the schema type is `PROTOBUF`.
  — *Source:* SR `SchemaId.java` L60-L76
- **Where each id deserializer looks.** `DualSchemaIdDeserializer`: the last record header named `__key_schema_id` or `__value_schema_id`; if it has a non-null value, the id comes from the header and the whole key or value is data. Otherwise the id comes from the prefix. `PrefixSchemaIdDeserializer`: the prefix only. `ConfigSchemaIdDeserializer`: nothing in the record; it uses the configured `use.schema.id` or `use.schema.guid`.
  — *Source:* SR `DualSchemaIdDeserializer.java` L27-L51; `PrefixSchemaIdDeserializer.java` L23-L33; `ConfigSchemaIdDeserializer.java` L28-L80
- **Protobuf message indexes.** The number of indexes, then each index, each written with `ByteUtils.writeVarint`. The default list (the single index 0) is the single byte `0x00`.
  — *Source:* SR `SchemaId.java` L181-L231
- **Byte order of the id.** The code calls `putInt` and `getInt` on a `ByteBuffer` and sets no byte order. No source read names the byte order in words; see [Not covered](#not-covered).
  — *Source:* SR `SchemaId.java` L63, L84
- **Version-neutral statement.** The layout *first byte 0, then a 4-byte schema id, then the data* is also what two independent implementations use: Karapace (`START_BYTE = 0x0`, `HEADER_FORMAT = ">bI"`, `HEADER_SIZE = 5`) and Apicurio Registry (`MAGIC_BYTE = 0x0`; `Default4ByteIdHandler` "Stores the ID as a 4-byte integer", a default its docs call "also compatible with the Confluent ID format"). The `0x1` GUID form and the header location are stated for the v8.4.1 code only.
  — *Source:* Karapace `src/karapace/core/serialization.py` L65-L67, L412-L415; Apicurio `BaseSerde.java` L25, `assembly-configuring-kafka-client-serdes.adoc` L42-L44, L581-L586
- **Subject names** are not on the wire. `key.subject.name.strategy` and `value.subject.name.strategy` default to `AssociatedNameStrategy`, which by default falls back to `TopicNameStrategy` (`<topic>-key`, `<topic>-value`) when the registry returns no associated subject. The subject is used after the id is read, to look up the schema.
  — *Source:* SR `AbstractKafkaSchemaSerDeConfig.java` L352-L357, L371-L376; `AssociatedNameStrategy.java` L40-L49; `TopicNameStrategy.java` L24-L42; `AbstractKafkaSchemaSerDe.java` L681-L690

## Symptoms / trigger

A client that uses a schema-registry deserializer fails on a record whose first byte (or schema-id header) is not `0x0` or `0x1`. Only the reading side throws. In the four modules read, no serializer checks the magic byte; serializers only write it.
— *Source:* SR `SchemaId.java` L60-L113

| Reading side (v8.4.1) | What is thrown |
|---|---|
| Avro deserializer (`KafkaAvroDeserializer`) with the default `DualSchemaIdDeserializer` | `SerializationException` (`org.apache.kafka.common.errors.SerializationException`), message `Error deserializing schema ID`, caused by `IllegalArgumentException`, message `Unknown magic byte!` |
| Avro deserializer with `PrefixSchemaIdDeserializer` configured | `IllegalArgumentException`, message `Unknown magic byte!`, not wrapped (`PrefixSchemaIdDeserializer` has no catch; the Avro `DeserializationContext` constructor, which reads the id, catches only `IOException`) |
| JSON Schema and Protobuf deserializers | The same chain, wrapped once more in a `SerializationException` whose message starts `Error deserializing JSON message for id ` or `Error deserializing Protobuf message for id ` |
| Console formatter (`SchemaMessageFormatter`, for example `io.confluent.kafka.formatter.AvroMessageFormatter`) | It calls the same deserializer, so the same exception. With `print.schema.ids=true`, a failure while printing the id is a `SerializationException` with message `Error deserializing schema id` |

— *Source:* SR `SchemaId.java` L70; `DualSchemaIdDeserializer.java` L23, L37-L50; `PrefixSchemaIdDeserializer.java` L28-L33; `KafkaAvroDeserializer.java` L107-L110; `AbstractKafkaAvroDeserializer.java` L309-L322, L637-L661; `AbstractKafkaJsonSchemaDeserializer.java` L172-L176, L299-L304; `AbstractKafkaProtobufDeserializer.java` L170-L174, L296-L302; `AbstractKafkaSchemaSerDe.java` L1243-L1249; `SchemaMessageFormatter.java` L44, L267-L287, L316-L329; `AvroMessageFormatter.java` L64, L86-L89, L131-L135

- `SchemaMessageFormatter.java` L325 has `throw new SerializationException("Unknown magic byte!")`. It is reached only when the id parsed without error but neither id nor GUID is set, and the `catch (Exception e)` on the next line wraps it as `Error deserializing schema id`.
  — *Source:* SR `SchemaMessageFormatter.java` L316-L329
- With the default `DualSchemaIdDeserializer`, every exception raised while the id is read is wrapped as `Error deserializing schema ID`, not only the magic-byte one. Read the cause.
  — *Source:* SR `DualSchemaIdDeserializer.java` L37-L50
- A `null` key or value is returned as `null` before any byte is read.
  — *Source:* SR `AbstractKafkaAvroDeserializer.java` L316-L318
- Bytes that were not written by these serializers but start with `0x0` or `0x1` pass the check. The next 4 (or 16) bytes are then taken as the schema id, and the record fails later, at the schema lookup or at decoding (table below), or not at all if the rest happens to decode.
  — *Source:* SR `SchemaId.java` L60-L76

The first row is the magic-byte failure this runbook is about. In the other rows the id was read, so they point at the registry, the schema or the data; the exception is a producer with another id format (remediation step 5), which can show up as a wrong id or a decoding error.

| Failure (v8.4.1) | Avro deserializer | JSON Schema, Protobuf deserializers |
|---|---|---|
| First byte not `0x0`/`0x1`, or too few bytes for the id | `Error deserializing schema ID` (default `DualSchemaIdDeserializer`); cause `Unknown magic byte!` for the first byte, a JDK exception for too few bytes | The same, wrapped in `Error deserializing JSON message for id ` / `Error deserializing Protobuf message for id ` |
| Schema id unknown to the registry, or another registry HTTP error without a mapping below | `SerializationException` `Error retrieving Avro value schema for id ` (`key` for keys) | `Error retrieving JSON schema for id ` / `Error retrieving Protobuf schema for id ` |
| Registry HTTP 401 or 403 | `AuthenticationException` or `AuthorizationException`, same message | Same |
| Registry HTTP 408, 500, 503, 504; 502; 429 | `TimeoutException`; `DisconnectException`; `ThrottlingQuotaExceededException` | Same |
| Registry not reachable (I/O error) | `SerializationException` `Error retrieving Avro value schema for id `; `TimeoutException` for an interrupted I/O | `Error deserializing JSON message for id ` / `Error deserializing Protobuf message for id ` (`TimeoutException` for an interrupted I/O) |
| Id and schema found, but the payload does not decode with it, or does not fit the reader schema or class | `Error deserializing Avro message for id ` | `Error deserializing JSON message for id ` / `Error deserializing Protobuf message for id `; JSON Schema with `json.fail.invalid.schema`: cause `JSON does not match schema of type ` |

— *Source:* SR `DualSchemaIdDeserializer.java` L37-L50; `AbstractKafkaAvroDeserializer.java` L309-L322, L637-L691, L776-L888, L937-L943; `AbstractKafkaSchemaSerDe.java` L1243-L1269; `AbstractKafkaJsonSchemaDeserializer.java` L172-L304, L590-L601; `KafkaJsonSchemaDeserializerConfig.java` L31; `AbstractKafkaProtobufDeserializer.java` L170-L302

## Diagnosis steps

1. Note which deserializer class the failing client uses for the key and which for the value: the Kafka consumer settings `key.deserializer` and `value.deserializer` ("Deserializer class for value that implements the `org.apache.kafka.common.serialization.Deserializer` interface."). The error comes from whichever is a schema-registry deserializer.
   — *Source:* apache/kafka-site `content/en/43/generated/consumer_config.html` L13-L24
2. Read the topic with the plain Kafka console consumer and print offsets, to see where the unexpected records are. From the Kafka docs (replace the topic name):
   ```
   $ bin/kafka-console-consumer.sh --topic tieredTopic --from-beginning --max-messages 1 --bootstrap-server localhost:9092 --formatter-property print.offset=true
   ```
   The docs also show the formatter properties `print.key=true`, `print.value=true`, `key.deserializer` and `value.deserializer`, each passed with `--formatter-property`. `--max-messages 1` stops after the first record; without it, "consumption is continual". To read from a known position, see [Which records the console consumer reads](#which-records-the-console-consumer-reads) below. NO OFFICIAL SOURCE FOUND in the Kafka docs read for a console option that prints the first bytes of a record in hexadecimal.
   — *Source:* Kafka docs `operations/tiered-storage.md` L136-L140; `streams/quickstart.md` L147-L153; Kafka tools `ConsoleConsumerOptions.java` L165
3. Read the same topic with the schema-registry formatter for the value's schema type. Each formatter reads every value with the deserializer of its own type; none chooses a decoder from the record's schema type. A failure with the formatter of another type is therefore no evidence of bad framing. Classify each error with the table under Symptoms.

   | Value schema type | `--formatter` class | Module |
   |---|---|---|
   | Avro | `io.confluent.kafka.formatter.AvroMessageFormatter` | `avro-serializer` |
   | JSON Schema | `io.confluent.kafka.formatter.json.JsonSchemaMessageFormatter` | `json-schema-serializer` |
   | Protobuf | `io.confluent.kafka.formatter.protobuf.ProtobufMessageFormatter` | `protobuf-serializer` |

   The JSON Schema and Protobuf names are corrected: the usage comments in those two classes write `io.confluent.kafka.formatter.JsonSchemaMessageFormatter` and `io.confluent.kafka.formatter.ProtobufMessageFormatter`, but the `package` lines of the source files are `io.confluent.kafka.formatter.json` and `io.confluent.kafka.formatter.protobuf`.

   Example 1 from the Avro source (for the other types, change the `--formatter` class), with one correction: the source has no trailing `\` after `localhost:9092`, which ends the command there; it is added here:
   ```
   bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
     --bootstrap-server localhost:9092 \
     --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
     --property schema.registry.url=http://localhost:8081
   ```
   The formatter jar and its dependencies must be on the classpath of `kafka-console-consumer.sh`. `SchemaMessageFormatter` reads the properties `print.partition`, `print.offset`, `print.headers`, `print.key`, `print.schema.ids`, `key.deserializer`; pass them as the source's example 2 passes `--property print.key=true`.
   - `print.schema.ids=true`: each framed record prints its schema id (or GUID) after the value.
   - `Partition:` and `Offset:` are written before the value is deserialized, so they are written for the failing record too.
   - `print.headers=true` prints header keys. A header `__value_schema_id` or `__key_schema_id` means the id is in the header and the value or key has no prefix.
   - `print.key=true` without a `key.deserializer` property: the key is also deserialized with the schema-registry deserializer, so a plain key fails here.

   The Kafka 4.3 docs list `--property` as deprecated in favor of `--formatter-property`, and `--consumer.config` in favor of `--command-config` (notable changes in 4.2.0).
   — *Source:* SR `AvroMessageFormatter.java` L17, L36-L61, L104; `json/JsonSchemaMessageFormatter.java` L17, L38-L67, L85-L86, L102; `protobuf/ProtobufMessageFormatter.java` L17, L38-L67, L85-L86, L120; `SchemaMessageFormatter.java` L93-L138, L193-L287; Kafka docs `getting-started/upgrade.md` L102, L115-L126

   <a id="which-records-the-console-consumer-reads"></a>**Which records the console consumer reads** (Kafka 4.3.2 tool source; applies to steps 2 and 3). Unless its config file names a group or sets `auto.offset.reset`, the example above reads only records written after it starts, so it can wait without ever showing the record that failed:
   - Without `--group`, and without `group.id` in the config file or in `--consumer-property`, the tool uses a new group named `console-consumer-` plus a random number, and sets `enable.auto.commit=false` unless the configuration sets it. That group has no committed position, and unless the configuration sets `auto.offset.reset`, the tool sets it to `latest`, the end of the log.
   - To read existing records, add `--from-beginning` ("If the consumer does not already have an established offset to consume from, start with the earliest message present in the log rather than the latest message"; the tool exits with an error if the configuration also sets `auto.offset.reset` to another value). Or go to the failing record: `--partition <partition> --offset <offset>`, with the values from step 2 or from the client's error; `--partition` alone "starts from the end of the partition". `--partition` cannot be combined with a group.
   - The tool stops at the first record the formatter cannot read. `--skip-message-on-error` logs the error and continues, so one run shows which offsets print a schema id and which fail. Without `--max-messages`, consumption is continual; `--timeout-ms` exits when no message arrives for that interval.
   - **Keep the real consumer group out of it.** Leave out `--group`, and check that the file passed with `--consumer.config` (or `--command-config`) has no `group.id`. With the failing application's `group.id` the tool consumes as a member of that group, and with the consumer default `enable.auto.commit=true` it commits positions for it. With no group id given, the random group is the isolated diagnostic group and the application's offsets are not touched.

   For example, the Avro formatter at a known partition and offset, continuing past failures:
   ```
   bin/kafka-console-consumer.sh --topic t1 \
     --bootstrap-server localhost:9092 \
     --partition 0 --offset 42 --skip-message-on-error \
     --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
     --formatter-property schema.registry.url=http://localhost:8081 \
     --formatter-property print.offset=true \
     --formatter-property print.schema.ids=true
   ```
   — *Source:* Kafka tools `ConsoleConsumerOptions.java` L83-L92, L94, L104-L111, L113, L139, L163-L165, L169, L173-L174, L195, L254-L301, L303-L334; `ConsoleConsumer.java` L105-L130; apache/kafka-site `content/en/43/generated/consumer_config.html` (`enable.auto.commit`, L243-L249)
4. Find which producer wrote the unexpected records. NO OFFICIAL SOURCE FOUND for identifying the producing client from a record. The sources support comparing configuration: each producer's `key.serializer` and `value.serializer`, and for schema-registry serializers `key.schema.id.serializer` and `value.schema.id.serializer`.
   — *Source:* apache/kafka-site `content/en/43/generated/producer_config.html` L13-L23; SR `AbstractKafkaSchemaSerDeConfig.java` L359-L382

## Remediation steps

Each item is a cause, a check, and a fix. The sources do not say which cause is most frequent.

1. **Cause: records written without a schema-registry serializer, read with a schema-registry deserializer.**
   - Check: diagnosis steps 2 and 3; the formatter fails at those offsets with a first-row error (Symptoms table).
   - Fix: set the consumer's `value.deserializer` (or `key.deserializer`) to match the serializer that the producers of that topic use, or correct the producer. A schema-unaware deserializer gives framed records to the application as their raw bytes, without an error; if the topic holds both kinds, see cause 2. The Apicurio docs give the same rule for their classes: "You must configure the Avro deserializer class to match the following configuration settings of the serializer", including the ID encoding.
   — *Source:* SR `SchemaId.java` L60-L71; Apicurio `assembly-configuring-kafka-client-serdes.adoc` L615-L624
2. **Cause: one topic holds both kinds of records** (schema-unaware and framed).
   - Check: diagnosis step 3 prints schema ids for some offsets and fails on others with a first-row error.
   - Fix: `io.confluent.kafka.serializers.wrapper.CompositeDeserializer` with `composite.old.deserializer` ("Old deserializer that is schema-unaware") and `composite.confluent.deserializer` ("Confluent deserializer, one of Avro, Protobuf, JSON Schema"). Per record it parses the id with `DualSchemaIdDeserializer`. If a GUID was read, or an integer id greater than 0 that the registry confirms for the subject, it calls the Confluent deserializer. Otherwise, including when parsing throws a `SerializationException`, it calls the old deserializer.
   - Caveat from the code: a framed record also goes to the old deserializer when the id check fails, and the check fails for any exception, including an unreachable or refusing registry (it then logs `Error while validating schema id`). The check uses the subject `<topic>-key` or `<topic>-value` ("We assume TopicNameStrategy"), so framed records registered under another subject go there too. A `SerializationException` from the Confluent deserializer, such as a payload that does not decode or an unknown schema id, also sends the record there. A schema-unaware old deserializer (bytes or strings) then returns the framed bytes as legacy data, with no error. Only the code was read; no prose documentation for this class is in the sources.
   — *Source:* SR `CompositeDeserializer.java` L49-L75, L90-L132; `CompositeDeserializerConfig.java` L25-L31
3. **Cause: key and value are serialized differently** (for example plain keys and framed values) while both `key.deserializer` and `value.deserializer` are schema-registry deserializers, or the reverse.
   - Check: diagnosis step 1, and step 3 with and without `print.key=true`.
   - Fix: set `key.deserializer` and `value.deserializer` independently, each to match its producer side. In the console formatter, set the `key.deserializer` property.
   — *Source:* apache/kafka-site `content/en/43/generated/consumer_config.html` L13-L24; SR `SchemaMessageFormatter.java` L120-L123, L234-L251
4. **Cause: the producer puts the schema id in a record header** (`HeaderSchemaIdSerializer` as `key.schema.id.serializer` or `value.schema.id.serializer`) and the consumer reads only the prefix (`PrefixSchemaIdDeserializer`).
   - Check: diagnosis step 3 with `print.headers=true` shows `__value_schema_id` or `__key_schema_id`.
   - Fix: leave `key.schema.id.deserializer` and `value.schema.id.deserializer` at the default `DualSchemaIdDeserializer`, which reads the header first and the prefix otherwise.
   — *Source:* SR `HeaderSchemaIdSerializer.java` L32-L50; `DualSchemaIdDeserializer.java` L27-L51; `AbstractKafkaSchemaSerDeConfig.java` L365-L367, L384-L386
5. **Cause: the producer uses another serializer family with a different id format.** For Apicurio Registry 3.3.4 SerDes, these settings differ from that layout (magic byte, 4-byte id, data):
   - `apicurio.registry.id-handler` = `io.apicurio.registry.serde.Legacy8ByteIdHandler` ("Stores the ID as an 8-byte long"). The default is `io.apicurio.registry.serde.Default4ByteIdHandler`; the v2 to v3 migration table gives the v2 default as "8 byte handler".
   - `apicurio.registry.headers.enabled` = `true`: the id is in message headers instead of the payload (default `false`).
   - Protobuf: by default the Apicurio SerDes write a type reference into the payload where the Confluent wire format has a message-index list.
   - Check: the producer's Apicurio configuration.
   - Fix, from the Apicurio docs: keep the 4-byte handler and the payload location. For Protobuf set `apicurio.registry.serde.send-type-ref=false` and `apicurio.registry.serde.send-indexes=true` on the serializer (`apicurio.registry.serde.read-type-ref=false` and `apicurio.registry.serde.read-indexes=true` on an Apicurio deserializer). "Enable the message indexes and disable the type reference together."
   - A wrong id, not a magic-byte error, results from an id-type mismatch: `apicurio.registry.use-id` (`contentId` by default, or `globalId`) on the client, `apicurio.ccompat.legacy-id-mode.enabled` on the registry.
   — *Source:* Apicurio `assembly-configuring-kafka-client-serdes.adoc` L364-L368, L388-L402, L571-L586, L885-L930; `assembly-migrating-registry-v2-v3.adoc` L872-L874; `assembly-confluent-schema-registry-compatibility.adoc` L106-L128
6. **Records that stay unreadable after the cause is fixed** (they are already in the topic). What the Kafka docs offer, by client type:
   - Kafka Connect: by default any error during conversion makes the connector fail (`errors.tolerance=none`). `errors.deadletterqueue.topic.name` reports errors within a connector's converter, transforms, or within the sink connector itself to a dead letter queue topic; `errors.tolerance=all` tolerates all errors by reporting them rather than failing the connector task; `errors.log.enable=true` logs the problem record's topic, partition and offset.
   - Kafka Streams: `deserialization.exception.handler`. `LogAndContinueExceptionHandler` logs and skips; `LogAndFailExceptionHandler` logs and stops.
   - These options act on the exception, not on its cause: while they are on, framed records that fail for a registry reason (Symptoms table) are skipped or sent to the dead letter queue too. Turn them off, or narrow a custom Streams handler, once the old records are past. The Connect converters were not read.
   - A plain consumer group: **[Changes consumer positions, skips records]** the `--reset-offsets` option of `kafka-consumer-groups.sh`, with `--shift-by` or `--to-offset`. Rules (inactive consumers, `--execute`): [consumer-group-position.md](consumer-group-position.md).
   — *Source:* Kafka docs `kafka-connect/user-guide.md` L396-L436; `streams/developer-guide/config-streams.md` L1224-L1229; `operations/basic-kafka-operations.md` L226-L252

## Verification

1. Framed records only: run diagnosis step 3 again, with the formatter for the value's schema type, `print.schema.ids=true`, and `--partition`/`--offset` or `--from-beginning` so that the affected records are read, over offsets that should now hold framed records (records written after a producer-side fix, or the framed records of a mixed topic): each prints a value and a schema id, with no exception. The formatter reads every value with the schema-registry deserializer of its type, so records that were never framed (remediation steps 1 and 2 when the consumer side was changed, and records skipped under step 6) still fail there; that is not a failed fix.
   — *Source:* SR `SchemaMessageFormatter.java` L127-L135, L267-L287
2. The consuming client, with its corrected deserializer configuration, moves past the offset where it failed. With `CompositeDeserializer` or a skip option (remediation steps 2 and 6) in place, moving past is not proof that records were read correctly: check the client log for `Error while validating schema id` and for skipped records. Where records were skipped (remediation step 6), check that consumption resumes after the skipped offsets: `CURRENT-OFFSET` and `LAG` from `--describe`, in [consumer-group-position.md](consumer-group-position.md). NO OFFICIAL SOURCE FOUND for a more specific check.

## Rollback or caveats

- The check is one byte. It does not prove that a schema-registry serializer wrote the record (last note under Symptoms).
  — *Source:* SR `SchemaId.java` L60-L76
- `HeaderSchemaIdSerializer` needs record headers: with `headers == null` it throws `SerializationException` with message `Missing headers`, on the producer side.
  — *Source:* SR `HeaderSchemaIdSerializer.java` L36-L38
- Schema ids are part of the data. Karapace docs: the id "is embedded in the wire format of every message a producer writes", and a migration that reassigned ids "would leave that data unreadable". Karapace's `IMPORT` mode registers schemas with their original schema ids.
  — *Source:* Karapace `website/docs/import-mode.md` L7-L19; `README.rst` L263-L270
- Other implementations report the same condition differently. Karapace: `InvalidMessageHeader`, message `Start byte is {start_byte:x} and should be {START_BYTE:x}`. Apicurio: `IllegalStateException`, message `Unknown magic byte!`; its deserializer docs add "If the magic byte is not found, the deserializer reads the ID from the message headers."
  — *Source:* Karapace `src/karapace/core/serialization.py` L424-L430; Apicurio `BaseSerde.java` L124-L130, `assembly-configuring-kafka-client-serdes.adoc` L636
- Skipping records (remediation step 6) loses them for that consumer. The Streams docs note that writing skipped records to a quarantine topic by hand is a side effect outside the Streams processing guarantees.
  — *Source:* Kafka docs `streams/developer-guide/config-streams.md` L1233

## Related runbooks

- [consumer-group-position.md](consumer-group-position.md)
- [../strimzi/consumer-lag.md](../strimzi/consumer-lag.md)
- [../strimzi/connect-restart-connector-and-task.md](../strimzi/connect-restart-connector-and-task.md)

## Sources

Retrieved, or verified at the stated commit, on 2026-11-16. Line ranges are those cited above.

| Source and licence | Files (lines) |
|---|---|
| **SR**: https://github.com/confluentinc/schema-registry tag `v8.4.1`, commit 2e7a638f235f5ef18acff5556652e6a17a5b2f0c. Apache-2.0: `schema-serializer/LICENSE`, `avro-serializer/LICENSE`, `json-schema-serializer/LICENSE`, `protobuf-serializer/LICENSE`, and the Apache header of every file listed | Under `schema-serializer/src/main/java/io/confluent/kafka/`: `serializers/schema/id/SchemaId.java` (L33-L37, L60-L113, L181-L231), `serializers/schema/id/PrefixSchemaIdSerializer.java` (L23-L39), `serializers/schema/id/PrefixSchemaIdDeserializer.java` (L23-L33), `serializers/schema/id/HeaderSchemaIdSerializer.java` (L27-L50), `serializers/schema/id/DualSchemaIdDeserializer.java` (L23, L27-L51), `serializers/schema/id/ConfigSchemaIdDeserializer.java` (L28-L80), `serializers/AbstractKafkaSchemaSerDeConfig.java` (L49, L352-L388, L502-L511), `serializers/AbstractKafkaSchemaSerDe.java` (L681-L690, L1243-L1269), `serializers/subject/AssociatedNameStrategy.java` (L40-L49), `serializers/subject/TopicNameStrategy.java` (L24-L42), `serializers/wrapper/CompositeDeserializer.java` (L49-L75, L90-L132), `serializers/wrapper/CompositeDeserializerConfig.java` (L25-L31), `formatter/SchemaMessageFormatter.java` (L44, L93-L138, L193-L287, L316-L329). Under `avro-serializer/src/main/java/io/confluent/kafka/`: `serializers/KafkaAvroDeserializer.java` (L107-L110), `serializers/AbstractKafkaAvroDeserializer.java` (L309-L322, L637-L691, L776-L888, L937-L943), `serializers/KafkaAvroSerializerConfig.java` (L24), `serializers/KafkaAvroDeserializerConfig.java` (L26), `formatter/AvroMessageFormatter.java` (L17, L36-L61, L64, L86-L89, L104, L131-L135). Under `json-schema-serializer/src/main/java/io/confluent/kafka/serializers/json/`: `AbstractKafkaJsonSchemaDeserializer.java` (L172-L304, L590-L601), `KafkaJsonSchemaSerializerConfig.java` (L28), `KafkaJsonSchemaDeserializerConfig.java` (L24, L31); and `json-schema-serializer/src/main/java/io/confluent/kafka/formatter/json/JsonSchemaMessageFormatter.java` (L17, L38-L67, L85-L86, L102). Under `protobuf-serializer/src/main/java/io/confluent/kafka/serializers/protobuf/`: `AbstractKafkaProtobufDeserializer.java` (L170-L302), `AbstractKafkaProtobufSerializer.java` (L196-L208), `KafkaProtobufSerializerConfig.java` (L27), `KafkaProtobufDeserializerConfig.java` (L24); and `protobuf-serializer/src/main/java/io/confluent/kafka/formatter/protobuf/ProtobufMessageFormatter.java` (L17, L38-L67, L85-L86, L120) |
| **Apicurio**: https://github.com/Apicurio/apicurio-registry tag `3.3.4`, commit 5662f79947dce0c09a3b9f61ddd5c4bc588fa004. Apache-2.0 (repository `LICENSE`) | Under `docs/modules/ROOT/pages/getting-started/`: `assembly-configuring-kafka-client-serdes.adoc` (L42-L44, L364-L368, L388-L402, L571-L586, L615-L636, L885-L930), `assembly-migrating-registry-v2-v3.adoc` (L872-L874), `assembly-confluent-schema-registry-compatibility.adoc` (L106-L128). `serdes/generic/serde-common/src/main/java/io/apicurio/registry/serde/BaseSerde.java` (L25, L124-L130) |
| **Karapace**: https://github.com/Aiven-Open/karapace tag `6.2.4`, commit 09cba53ffcc961038f608b34f7d00fc2df1d313c. Apache-2.0 (repository `LICENSE`) | `src/karapace/core/serialization.py` (L65-L67, L412-L430), `website/docs/import-mode.md` (L7-L19), `README.rst` (L263-L270) |
| **Kafka docs**: https://github.com/apache/kafka tag `4.3.2`, commit 0a6c96d3b61167f1141df2032f25f10fd6e118fe. Apache-2.0 (repository `LICENSE`, header of the docs files) | Under `docs/`: `implementation/message-format.md` (L31-L61), `operations/monitoring.md` (L330-L334), `operations/tiered-storage.md` (L136-L140), `streams/quickstart.md` (L147-L153), `getting-started/upgrade.md` (L102, L115-L126), `kafka-connect/user-guide.md` (L396-L436), `streams/developer-guide/config-streams.md` (L1224-L1233), `operations/basic-kafka-operations.md` (L226-L252). Tool source (Kafka tools): under `tools/src/main/java/org/apache/kafka/tools/consumer/`, `ConsoleConsumerOptions.java` (L83-L94, L104-L113, L139, L163-L165, L169, L173-L174, L195, L254-L334), `ConsoleConsumer.java` (L105-L130) |
| https://github.com/apache/kafka-site commit cd727c4dd8664857751a87cd0442e51a3d3aca8f (default branch, no tag). Apache-2.0 (repository `LICENSE`) | `content/en/43/generated/consumer_config.html` (L13-L24, L243-L249), `content/en/43/generated/producer_config.html` (L13-L23) |

## Not covered

Wanted, but not in an allowed source that was read, so left out above:

- The name of the byte order of the 4-byte schema id. The sources show only `putInt` / `getInt` on a `ByteBuffer` with no order set, and Karapace's format string `">bI"`. Their meaning is defined in JDK and Python documentation, which are not allowed sources here.
- A way to print the first bytes of a record in hexadecimal with tooling the Apache Kafka 4.3 docs document.
- How to find out which producer wrote a given record.
- The exception raised when a key or value is shorter than the id it should contain, with `PrefixSchemaIdDeserializer` (the code reads without a length check; the exception is defined by the JDK). With the default `DualSchemaIdDeserializer` it is wrapped as `Error deserializing schema ID`.
- Failures raised by data contract rules (`executeRules`) and schema migrations on the read path.
- How the Kafka consumer, Kafka Connect or Kafka Streams wrap the deserializer's exception before the application sees it.
- Whether serializer versions other than v8.4.1 write or accept the `0x1` GUID form and the schema-id headers, and since which version.
- Product documentation for `CompositeDeserializer`, `HeaderSchemaIdSerializer` and the four schema-id serializer and deserializer settings.
- The bytes that `ByteUtils.writeVarint` produces (Kafka client code, not read).
- Schema-registry Kafka Connect converters (`avro-converter` and similar modules were not read) and serializers for other languages, apart from the Karapace Python code cited.
- How often each cause occurs in practice.
