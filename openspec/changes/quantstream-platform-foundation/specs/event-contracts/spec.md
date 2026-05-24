## ADDED Requirements

### Requirement: Trade event Pydantic model
The system SHALL define a Pydantic BaseModel for trade events with the following typed fields: trade_id (UUID), exchange (str), symbol (str), price (Decimal with 8 decimal places), quantity (Decimal with 8 decimal places), side (Literal["BUY", "SELL"]), event_time (datetime with UTC timezone), ingest_time (datetime with UTC timezone), schema_version (int, default 1).

#### Scenario: Valid trade event JSON deserializes successfully
- **WHEN** a valid JSON object with all required trade event fields is passed to `TradeEvent.model_validate_json()`
- **THEN** the event SHALL be parsed into a TradeEvent instance with correct field types

#### Scenario: Invalid trade event raises validation error
- **WHEN** a JSON object missing required fields or with incorrect types is passed to `TradeEvent.model_validate_json()`
- **THEN** a Pydantic ValidationError SHALL be raised with specific field-level error details

### Requirement: Schema version registry
The system SHALL maintain a schema registry mapping schema_version integers to Pydantic model classes, enabling version-aware deserialization.

#### Scenario: Unknown schema version is rejected
- **WHEN** an event with an unregistered schema_version value is received
- **THEN** the system SHALL raise a SchemaVersionError with the offending version number

### Requirement: Event serialization to JSON
The system SHALL provide bidirectional serialization between TradeEvent model instances and JSON bytes, with consistent field naming (snake_case in Python, camelCase on wire).

#### Scenario: TradeEvent serializes to camelCase JSON
- **WHEN** a TradeEvent instance with field `trade_id` is serialized to JSON
- **THEN** the output JSON SHALL contain the field as `tradeId`

### Requirement: Schema evolution forward compatibility
The schema system SHALL support adding new optional fields in future schema versions without breaking deserialization of older events.

#### Scenario: New optional field in future version is handled
- **WHEN** a schema version 2 event includes a new field `commission` not present in version 1
- **THEN** the system SHALL parse the event and set unknown fields based on model configuration

### Requirement: Event envelope for messaging
The system SHALL define an EventEnvelope model wrapping any event with metadata: event_id (UUID), event_type (Literal["trade"]), timestamp (datetime), source (str, e.g. "binance"), partition_key (str).

#### Scenario: Event envelope wraps a trade event
- **WHEN** a TradeEvent is wrapped in an EventEnvelope
- **THEN** the envelope SHALL contain the trade event in the payload field and routing metadata for the messaging layer
