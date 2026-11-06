## ADDED Requirements

### Requirement: Config fields of newer versions survive a rewrite
When SLayer rewrites the usage-reporting config, it SHALL keep every field it does not know, unchanged, so that a newer SLayer version's state survives an older version's update.

#### Scenario: Unknown field kept
- **WHEN** the config holds a field this version does not define and a process start rewrites the config
- **THEN** the rewritten config still holds that field with its value

## MODIFIED Requirements

### Requirement: Exactly-once-per-batch delivery bookkeeping
Only one process SHALL send at a time. Spool files included in a send SHALL be deleted, and the last-send time recorded, only after a 2xx response; otherwise they SHALL remain for a later send. Files claimed by a sender that died SHALL be recovered by a later send. Each report SHALL carry a random batch ID so the backend can drop duplicates. Each spool file's name SHALL carry the report schema version that wrote it, such that a SLayer version of an older schema does not recognise it as a spool file; a sender SHALL claim the spool files of its own and of older schema versions and leave those of newer ones untouched. The spool SHALL be capped (about 100 files / 1 MB, oldest dropped first); spool files SHALL be readable only by the owner, and entries that are not regular files SHALL be ignored.

#### Scenario: Two simultaneous senders
- **WHEN** two processes find a send due at the same moment
- **THEN** one report is sent

#### Scenario: Failed send retained
- **WHEN** a send gets a 500 response
- **THEN** the spool files remain and the last-send time is unchanged

#### Scenario: Sender crash recovered
- **WHEN** a sender dies after claiming spool files
- **THEN** a later send includes those files

#### Scenario: Older spool files are sent
- **WHEN** the spool holds a file written by a schema-1 SLayer and a send is due
- **THEN** the report includes that file's counts and the file is deleted after the 2xx response

#### Scenario: An older sender leaves newer spool files alone
- **WHEN** the spool holds a file written by this version and a schema-1 SLayer completes a send
- **THEN** the file is still present and a later send by this version includes its counts

### Requirement: Install identity and rotation
The install ID SHALL be a random UUID created on first use and stored in the platform config directory (not in SLayer's model storage). It SHALL be replaced by a new random UUID at the first start or send after it is 11 months old, so no report carries an install ID older than that. Creating or replacing the install ID SHALL clear the stored first-successful-query date.

#### Scenario: Rotation
- **WHEN** a process starts and the install ID was created 12 months ago
- **THEN** a new install ID is created and used for subsequent reports

#### Scenario: Rotation in a long-running process
- **WHEN** a server started while its install ID was 10 months old sends a report two months later
- **THEN** the report carries a new install ID

#### Scenario: Rotation clears the first-query date
- **WHEN** the install ID is replaced while a first-successful-query date is stored
- **THEN** no date is stored, and reports of the new install ID carry none until its first successful query
