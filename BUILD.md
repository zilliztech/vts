# VTS Build Guide

This guide provides instructions for building VTS (Vector Transport Service) locally and with Docker.

## Prerequisites

- **Java**: OpenJDK 11 or higher (17 recommended)
- **Maven**: 3.6+ or Maven Daemon (mvnd) for faster builds
- **Docker**: For containerized builds
- **Node.js**: 16+ (only if building UI components)
- **Git**: For cloning and version control

## Quick Build Commands

### Using Maven Daemon (Recommended)

Maven Daemon (mvnd) provides faster builds through background process reuse:

```bash
# Install mvnd (if not already installed)
curl -L "https://github.com/apache/maven-mvnd/releases/download/1.0.2/maven-mvnd-1.0.2-linux-amd64.zip" -o mvnd.zip
unzip mvnd.zip && sudo mv maven-mvnd-1.0.2-linux-amd64 /opt/mvnd
export PATH="/opt/mvnd/bin:$PATH"

# Build distribution package (fastest)
mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease

# Build with all modules
mvnd clean compile -Dmaven.test.skip=true

# Build specific module
mvnd clean package -pl seatunnel-connectors-v2/connector-milvus -am -Dmaven.test.skip=true
```

### Using Standard Maven

```bash
# Build distribution package
./mvnw clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease

# Build all modules
./mvnw clean compile -Dmaven.test.skip=true

# Build with tests (slower)
./mvnw clean test
```

## Build Profiles

### Release Profile (Default)
```bash
mvnd clean package -Prelease
```
- Includes distribution module
- Optimized for production
- Tests skipped by default

### CI Profile
```bash
mvnd clean package -Pci
```
- Excludes distribution module
- Faster for development
- Used in continuous integration

## Docker Builds

### Using Optimized Dockerfile with mvnd

```bash
# Build Docker image with mvnd (faster)
docker build -f ci/docker/Dockerfile.build -t vts:latest .

# Build with specific version
docker build -f ci/docker/Dockerfile.build --build-arg VERSION=2.3.11-SNAPSHOT -t vts:2.3.11 .

# Run the built image
docker run -it --rm -p 5801:5801 vts:latest
```

### Traditional Build

```bash
# Build using standard Dockerfile
docker build -f ci/docker/Dockerfile -t vts:latest .
```

## Module-Specific Builds

### Core Engine
```bash
mvnd clean package -pl seatunnel-engine -am -Dmaven.test.skip=true
```

### Connectors
```bash
# Build all connectors
mvnd clean package -pl seatunnel-connectors-v2 -am -Dmaven.test.skip=true

# Build specific connector (e.g., Milvus)
mvnd clean package -pl seatunnel-connectors-v2/connector-milvus -am -Dmaven.test.skip=true
```

### Web UI
```bash
cd seatunnel-engine/seatunnel-engine-ui

# Install dependencies
npm install

# Development build
npm run dev

# Production build
npm run build
```

### Transforms
```bash
mvnd clean package -pl seatunnel-transforms-v2 -am -Dmaven.test.skip=true
```

## Build Optimizations

### Skip Components

```bash
# Skip UI build
mvnd clean package -D"skip.ui"=true

# Skip tests completely
mvnd clean package -Dmaven.test.skip=true

# Skip unit tests only
mvnd clean package -DskipUT=true

# Skip integration tests
mvnd clean package -DskipIT=true

# Skip Spotless formatting checks
mvnd clean package -D"skip.spotless"=true
```

### Parallel Builds

```bash
# Use multiple threads (mvnd does this automatically)
mvnd clean package -T 4

# With standard Maven
./mvnw clean package -T 4
```

## Common Build Issues

### Memory Issues
```bash
# Increase Maven memory
export MAVEN_OPTS="-Xmx4g -Xms2g"

# For mvnd
export MVND_ARGS="-Xmx4g -Xms2g"
```

### Java Module Access (Java 17+)
The build is configured to handle Java module restrictions automatically. If you encounter issues:

```bash
export MAVEN_OPTS="--add-opens java.base/java.net=ALL-UNNAMED"
```

### Test Failures
Tests are skipped by default in the pom.xml. To run tests:

```bash
# Enable tests temporarily
mvnd clean test -Dmaven.test.skip=false -DskipUT=false
```

## Build Artifacts

After a successful build, you'll find:

- **Distribution**: `seatunnel-dist/target/apache-seatunnel-{version}-bin.tar.gz`
- **Connector JARs**: `seatunnel-connectors-v2/*/target/*.jar`
- **Engine JARs**: `seatunnel-engine/*/target/*.jar`
- **Web UI**: `seatunnel-engine/seatunnel-engine-ui/dist/`

## Performance Comparison

| Build Method | Typical Time | Memory Usage | Notes |
|--------------|--------------|--------------|--------|
| mvnd (clean) | 2-4 minutes | 2-4 GB | Fastest, daemon reuse |
| mvnw (clean) | 4-8 minutes | 3-5 GB | Standard Maven |
| Docker build | 8-15 minutes | 4-8 GB | Includes image creation |

## Development Workflow

1. **Initial Setup**:
   ```bash
   git clone <repository>
   cd vts
   mvnd clean compile -Dmaven.test.skip=true
   ```

2. **Incremental Changes**:
   ```bash
   # Build only changed modules
   mvnd compile -pl <changed-module> -am
   ```

3. **Full Release Build**:
   ```bash
   mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease
   ```

4. **Testing Specific Features**:
   ```bash
   # Build and test connector
   mvnd clean test -pl seatunnel-connectors-v2/connector-milvus -Dmaven.test.skip=false
   ```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MAVEN_OPTS` | JVM options for Maven | `-Xmx2g` |
| `MVND_ARGS` | JVM options for mvnd | `-Xmx4g` |
| `skip.ui` | Skip UI build | `false` |
| `maven.test.skip` | Skip all tests | `true` |
| `skip.spotless` | Skip code formatting | `true` |

## Troubleshooting

### Build Hangs
- Check available memory
- Use mvnd instead of mvnw
- Disable parallel builds: `-T 1`

### Dependency Issues
```bash
# Clear local repository
rm -rf ~/.m2/repository/org/apache/seatunnel
mvnd clean compile
```

### Docker Build Issues
```bash
# Clean Docker cache
docker system prune -a

# Build with more memory
docker build --memory=8g -f ci/docker/Dockerfile.build -t vts:latest .
```

For additional help, check the main [README.md](./README.md) or [CLAUDE.md](./CLAUDE.md) files.
