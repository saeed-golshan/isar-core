# Implementing

## Query support
- [x] Filter
- [x] WhereClause
- [ ] WhereExecutor
- [ ] Query
- [ ] Query ffi interface

## Migration
- [x] Schema diff
- [ ] Auto migration

## Multi threading
- [ ] Concept required

# Testing
Currently, unit tests either do not exist or fail. The stable parts of the code require tests.

- [ ] The lmdb module is almost feature complete and unit tests are missing
- [ ] Index is stable and already has a few unit tests
- [ ] Property is stable and requires tests
- [ ] ObjectId is stable and requires tests
- [ ] We need a way to test the ffi interface
