# OpenMove Sui Blockchain Indexer

A modern Sui blockchain indexer that processes events and stores them in PostgreSQL for webhook service consumption.

## Features

- Indexes Sui blockchain events with detailed metadata
- Stores events in PostgreSQL for efficient querying
- Tracks processing progress via checkpoint processing
- Configurable via environment variables or configuration files
- Supports concurrent checkpoint processing

## Architecture

This indexer uses a modern Rust architecture:

- **Config**: Configuration loading from files and environment variables
- **DB**: PostgreSQL database connection pool and migrations
- **Models**: Database entity models and repository methods
- **Processors**: Logic for processing Sui blockchain data
- **Error**: Error handling and custom error types

## Setup

### Prerequisites

- Rust 2024 or later
- PostgreSQL 14 or later

### Database Setup

1. Create a PostgreSQL database named `indexer`:

```bash
createdb indexer
```

2. The application will automatically run migrations on startup.

### Configuration

Configuration can be provided through:

1. `.env` file (copy from `.env.example`)
2. Configuration files in `config/` directory
3. Environment variables

Environment variables use the format `INDEXER__SECTION__KEY` (note the double underscore).

## Running the Indexer

1. Clone the repository
2. Setup your PostgreSQL database
3. Copy `.env.example` to `.env` and adjust values if needed
4. Run the indexer:

```bash
cargo run --release
```

## Development

### Building

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

## Integration with Webhook Service

This indexer is designed to store events in PostgreSQL that can be consumed by a webhook service. The webhook service can:

1. Query for new events since the last processed event
2. Forward events to subscribed endpoints
3. Support filtering by package, module, or event type

The database schema is optimized for these queries with appropriate indexes.

## License

MIT