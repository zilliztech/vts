# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

VTS (Vector Transport Service) is an open-source tool for moving vectors and unstructured data, built on Apache SeaTunnel. It provides data synchronization capabilities between various vector databases, traditional search engines, and data stores.

## Build Commands

### Maven Build
```bash
# Build the entire project
./mvnw clean compile

# Build with tests
./mvnw clean test

# Build without tests
./mvnw clean compile -DskipTests

# Build distribution package
./mvnw clean package -DskipTests

# Install to local repository
./mvnw clean install -DskipTests
```

### SeaTunnel Engine Commands
```bash
# Start cluster mode (recommended)
mkdir -p ./logs
./bin/seatunnel-cluster.sh -d

# Run job in cluster mode
./bin/seatunnel.sh --config ./path/to/config.conf

# Run job in local mode
./bin/seatunnel.sh --config ./path/to/config.conf -m local

# Stop cluster
./bin/stop-seatunnel-cluster.sh
```

### UI Development (seatunnel-engine-ui)
```bash
cd seatunnel-engine/seatunnel-engine-ui

# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build

# Run tests
npm run test:unit

# Run E2E tests
npm run test:e2e

# Lint code
npm run lint

# Format code
npm run format
```

## Testing

### Unit Tests
```bash
# Run all unit tests
./mvnw test

# Run tests for specific module
./mvnw test -pl seatunnel-engine/seatunnel-engine-core

# Run specific test class
./mvnw test -Dtest=ClassName

# Skip unit tests
./mvnw compile -DskipUT=true
```

### Integration Tests
```bash
# Run integration tests (disabled by default)
./mvnw verify -DskipIT=false

# Run E2E tests for connectors
./mvnw test -pl seatunnel-e2e/seatunnel-connector-v2-e2e
```

### Test Frameworks
- **JUnit 5** - Primary testing framework
- **Mockito** - Mocking framework
- **TestContainers** - Integration testing with Docker containers
- **Awaitility** - Asynchronous testing utilities

## Code Quality

### Code Formatting
```bash
# Check code style with Spotless
./mvnw spotless:check

# Apply code formatting
./mvnw spotless:apply
```

### Pre-commit Hooks
```bash
# Run pre-commit checks
./tools/spotless_check/pre-commit.sh
```

## Architecture Overview

### Core Modules
- **seatunnel-api** - Core API definitions and interfaces
- **seatunnel-engine** - SeaTunnel execution engine (cluster management, job execution)
- **seatunnel-connectors-v2** - Data connectors (Milvus, Elasticsearch, Pinecone, etc.)
- **seatunnel-transforms-v2** - Data transformation components
- **seatunnel-translation** - Translation layer between APIs and execution engines

### Engine Components
- **seatunnel-engine-core** - Core engine logic and execution
- **seatunnel-engine-server** - Server-side cluster management
- **seatunnel-engine-client** - Client API for job submission
- **seatunnel-engine-ui** - Web-based management interface (Vue.js)

### Data Flow
1. **Source Connectors** - Read data from various sources (Milvus, ES, etc.)
2. **Transforms** - Process and transform data (field mapping, embedding, etc.)
3. **Sink Connectors** - Write data to target systems

### Connector Architecture
- **connector-common** - Shared connector utilities
- **Vector-specific connectors** - Milvus, Pinecone, Qdrant, etc.
- **Traditional connectors** - Elasticsearch, PostgreSQL, etc.

## Development Workflow

### Adding New Connectors
1. Extend base connector classes in `seatunnel-connectors-v2/connector-common`
2. Implement source/sink interfaces from `seatunnel-api`
3. Add configuration schemas and validation
4. Create unit tests in `src/test/java`
5. Add E2E tests in `seatunnel-e2e/seatunnel-connector-v2-e2e`
6. Update documentation and examples

### Configuration Files
- Job configurations use HOCON format (`.conf` files)
- Examples located in `seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/`
- Engine configuration in `config/seatunnel.yaml`

### Key Patterns
- **Factory Pattern** - Connector creation via factory classes
- **Plugin Discovery** - Dynamic loading of connectors via `seatunnel-plugin-discovery`
- **Checkpoint/Savepoint** - State management for fault tolerance
- **Pipeline Execution** - Minimum granularity for job execution

## Common Issues

### Memory Configuration
- Adjust JVM options in `config/jvm_options`, `config/jvm_master_options`, `config/jvm_worker_options`
- Configure parallelism in job configuration based on data volume

### Performance Tuning
- Set appropriate `batch_size` in connector configurations
- Configure `parallelism` in job environment settings
- Monitor resource usage during data migration

### Vector Database Specific
- Ensure Milvus version >= 2.3.6 for compatibility
- Configure proper authentication tokens and connection parameters
- Handle vector dimension mismatches in transformations

## File Structure Notes

- Configuration files use YAML/HOCON format
- Java follows Google Java Format (AOSP style)
- Vue.js components follow standard Vue 3 + TypeScript patterns
- Test files use `*Test.java` and `*IT.java` naming conventions