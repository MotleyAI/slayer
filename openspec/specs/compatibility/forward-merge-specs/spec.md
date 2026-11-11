# compatibility/forward-merge-specs Specification

## Purpose
Keeps a merge of the `1.x` maintenance line into a later line from silently dropping a `1.x` spec requirement: each requirement `1.x` adds or changes must survive the merge verbatim or be recorded as an intentional divergence.

## Requirements

### Requirement: A forward merge keeps every 1.x requirement it brings in

A requirement is its `### Requirement:` title and its whitespace-normalised body, scenarios included, in a spec under `openspec/specs/`. For a pull request, the 1.x range is the `1.x` commits reachable from the head and not from the base: from the merge base of the base and `1.x` to the merge base of the head and `1.x`. The check SHALL run in CI on every pull request. When the head's merge base with `1.x` is not a strict descendant of the base's, the check SHALL pass without comparing. Otherwise every requirement that the 1.x range adds or modifies SHALL exist on the head with the same title and body, under any capability path, or SHALL be listed in the forward-merge ledger by its title and the hash of its `1.x` body, with a non-empty reason. A requirement the 1.x range removes SHALL NOT be checked. When comparing, a ledger entry whose title and hash match no requirement at the head's merge base with `1.x` SHALL fail the check as stale. When either side has more than one merge base with `1.x`, the check SHALL fail naming the bases. A failure SHALL name each missing requirement's capability, title and `1.x` body hash, and the ledger entry that would accept it.

#### Scenario: No 1.x commits enter
- **WHEN** a pull request's head and base share their merge base with `1.x`
- **THEN** the check passes

#### Scenario: An added 1.x requirement is missing
- **WHEN** a pull request merges a `1.x` commit that adds a requirement, and the head lacks it
- **THEN** the check fails naming the requirement's capability, title and `1.x` body hash

#### Scenario: A modified 1.x requirement is resolved toward the base
- **WHEN** a pull request merges a `1.x` commit that changes a requirement's body, and the head keeps the base's body
- **THEN** the check fails naming the requirement

#### Scenario: A requirement under a moved capability
- **WHEN** the merged `1.x` requirement exists on the head with the same title and body under a different capability path
- **THEN** the check passes

#### Scenario: A ledgered divergence
- **WHEN** the head lacks a merged `1.x` requirement and the ledger lists its title and `1.x` body hash with a reason
- **THEN** the check passes

#### Scenario: A ledger entry without a reason
- **WHEN** a pull request merges `1.x` commits and the ledger lists a requirement with an empty reason
- **THEN** the check fails naming the entry

#### Scenario: A stale ledger entry
- **WHEN** a pull request merges `1.x` commits and the ledger lists a title and hash that no requirement has at the head's merge base with `1.x`
- **THEN** the check fails naming the entry as stale

#### Scenario: A removal on 1.x
- **WHEN** a pull request merges a `1.x` commit that removes a requirement the head still has
- **THEN** the check passes

#### Scenario: A branch behind 1.x
- **WHEN** a pull request into `1.x` comes from a branch cut from an older `1.x` commit
- **THEN** the check passes without comparing

#### Scenario: A cherry-picked change is checked at the next forward merge
- **WHEN** a `1.x` commit adding a requirement was cherry-picked without the requirement, and a later pull request merges `1.x` past that commit
- **THEN** that later pull request's check fails naming the requirement
