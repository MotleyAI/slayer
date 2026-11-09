## ADDED Requirements

### Requirement: The library-granularity settle reads only stored datasource documents

The one-time step that renames library-named granularities before a backend's first model or memory load SHALL read the backend's stored datasource documents directly, and SHALL NOT call `list_datasources` or `get_datasource`. A backend that exposes no stored datasource documents, which is the default for a `StorageBackend` subclass, SHALL settle nothing, without overriding any hook. The YAML and SQLite stores SHALL expose exactly the datasource documents their `get_datasource` reads, and SHALL still rename before the first model or memory load. A stored datasource document that cannot be parsed, or that parses to a mapping which is not a valid datasource, SHALL NOT prevent the settle from renaming another datasource.

#### Scenario: A backend that builds its own datasources pays nothing

- **WHEN** a `StorageBackend` subclass that overrides `get_datasource` and `list_datasources` and stores no raw datasource documents loads models and memories through the base load pipeline, model first on one fresh instance and memory first on another
- **THEN** the settle runs on each instance and neither `list_datasources` nor `get_datasource` is called

#### Scenario: The built-in stores still rename first

- **WHEN** a YAML or SQLite store whose datasource defines the granularity `contains` loads a model, or a memory, before anything else
- **THEN** the loaded document references `contains_granularity` and the stored datasource defines `contains_granularity`

#### Scenario: An unparseable datasource document does not block the settle

- **WHEN** a YAML or SQLite store holds a datasource document that does not parse beside a datasource defining `contains`, and loads a model first
- **THEN** the load succeeds and `contains` is renamed to `contains_granularity`

#### Scenario: An invalid datasource mapping does not block the settle

- **WHEN** a YAML or SQLite store holds a datasource document that parses to a mapping which is not a valid datasource beside a datasource defining `contains`, and loads a model first
- **THEN** the load succeeds and `contains` is renamed to `contains_granularity`

#### Scenario: A YAML datasource file the store does not read is left alone

- **WHEN** a YAML store holds `datasources/shop.yml` defining the granularity `contains` and no `datasources/shop.yaml`, and loads a model first
- **THEN** `shop.yml` is unchanged and no `shop.yaml` is created
