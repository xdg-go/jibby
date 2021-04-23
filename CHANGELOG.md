# Changelog

## Unreleased

## v0.1.7 - 2021-04-23

- Fix potential panic reading surrogate pairs across buffer boundaries

## v0.1.6 – 2021-04-04

### Bug fixes

- Add support for surrogate pair unicode escapes

- Fix rune conversion warning for Go 1.15+

## v0.1.5 – 2020-03-17

### Bug fixes

- Fix incorrect regex for `$options`

## v0.1.4 – 2020-03-17

### Behavior changes

- Parse errors returned as `ParseError` type

### Bug fixes

- Parses legacy $date with numeric literal value

## v0.1.3 – 2020-03-06

### Behavior changes

- Added `ErrUnsupportedBOM` as an error type returned for unsupported BOMs

## v0.1.2 – 2020-03-04

### Behavior changes

- Legacy Extended JSON parsing now will not error on ambiguous legacy Extended
  JSON keys: `$regex`, `$options`, `$type`.

### Bug Fixes

- Fix loop closure bug
- Fix special conversion of binary subtype 0x02

## v0.1.1 – 2020-03-03

### Prereqs

- Drop Go driver dependency version to v1.2.1  (tag: v0.1.1)

## v0.1.0 – 2020-03-03

- Initial release
