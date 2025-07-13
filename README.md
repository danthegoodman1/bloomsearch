# bloomsearch <!-- omit in toc -->

Keyword index with bloom filter trees. Pluggable interface for massive keyword search.

Really good for logs, JSON path, and JSON value search.

Search by `field`, `token`, or `field:token` with combinators (OR, AND, NOT, etc.).

- [Usage](#usage)
  - [Gotchas](#gotchas)
- [How it works](#how-it-works)
- [Contributing](#contributing)
  - [File format](#file-format)

## Usage

### Gotchas

If you change the bloom filter parameters against existing data, they cannot be efficiently merged, and have to be
fully recalculated. This means merging will take significantly longer, as it has to re-process every row. Normally, we can just concatenate bytes, and merge the bloom filter.

## How it works

WIP some nice diagrams

## Contributing

Do not submit random PRs, they will be closed.

For feature requests and bugs, create an Issue.

For questions, create a Discussion.

### File format

See [FILE_FORMAT.md](./FILE_FORMAT.md)
