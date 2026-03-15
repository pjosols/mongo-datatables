# Changelog

All notable changes to this project will be documented in this file.

## [1.17.2] - 2026-03-14
### Fixed
- `projection` property now resolves UI column aliases to actual MongoDB field names via `field_mapper.get_db_field()`. Previously, aliased DataFields (e.g. `DataField('author.fullName', 'string', alias='Author')`) caused MongoDB to silently omit those fields from query results.
