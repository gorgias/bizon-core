# Development Guide

## 🏗️ Project Structure

```
bizon/
├── alerting/         # Alert handling (Slack, etc.)
├── cli/             # Command-line interface
├── common/          # Shared models and utilities
├── connectors/      # Source and destination connectors
│   ├── destinations/
│   └── sources/
├── destination/     # Destination abstractions
├── engine/          # Core pipeline engine
│   ├── backend/     # State management backends
│   ├── pipeline/    # Pipeline orchestration
│   ├── queue/       # Queue adapters
│   └── runner/      # Execution runners
├── monitoring/      # Monitoring and metrics
├── source/          # Source abstractions
└── transform/       # Data transformation
```

## 🛠️ Development Setup

### Prerequisites
- Python 3.9-3.12
- Poetry for dependency management
- Docker (for testing with message brokers)

### Quick Start
```bash
# Clone the repository
git clone <repo-url>
cd bizon

# Install dependencies and pre-commit hooks
make install

# Run all quality checks
make all-checks
```

## 🧪 Testing

### Running Tests
```bash
# Basic test run
make test

# With coverage report
make test-cov

# Run specific test files
poetry run pytest tests/engine/test_engine.py -v
```

### Test Categories
- **Unit tests**: `tests/` - Test individual components
- **Integration tests**: `tests/connectors/` - Test connector integrations
- **E2E tests**: `tests/e2e/` - End-to-end pipeline tests

### Test Infrastructure
The project uses `conftest.py` for shared fixtures including:
- SQLite in-memory backend for fast testing
- Mock configurations for different connectors
- Docker compose setups for integration testing

## 🔍 Code Quality

### Static Analysis Tools
- **Black**: Code formatting (120 char line length)
- **isort**: Import sorting
- **flake8**: Linting and style checks
- **mypy**: Type checking
- **bandit**: Security vulnerability scanning
- **pydocstyle**: Docstring style checking

### Running Quality Checks
```bash
# Format code
make format

# Run linting
make lint

# Type checking
make type-check

# Security scan
make security

# All checks
make all-checks
```

### Pre-commit Hooks
Pre-commit hooks run automatically on commit and include all quality tools. To run manually:
```bash
pre-commit run --all-files
```

## 🏛️ Architecture Patterns

### Plugin Architecture
Bizon uses a plugin-based architecture for:
- **Sources**: Data extraction from APIs, databases, files
- **Destinations**: Data loading to warehouses, files, streams
- **Queues**: Message brokers for data transport
- **Backends**: State persistence for checkpointing
- **Runners**: Execution models (thread, process, streaming)

### Adding New Connectors

#### New Source
1. Create directory: `bizon/connectors/sources/your_source/`
2. Implement required files:
   ```
   src/
   ├── __init__.py
   ├── config.py      # Pydantic config models
   ├── source.py      # Source implementation
   └── auth.py        # Authentication (if needed)
   tests/
   └── test_*.py      # Unit tests
   ```
3. Inherit from `AbstractSource`
4. Implement required methods: `get_records()`, `get_schema()`
5. Add to source registry

#### New Destination
1. Create directory: `bizon/connectors/destinations/your_destination/`
2. Implement similar structure as sources
3. Inherit from `AbstractDestination`
4. Implement `write_records()` method

### Error Handling Patterns
- Use structured logging with `loguru`
- Implement retry logic with `tenacity` for transient failures
- Graceful degradation for non-critical features
- Clear error messages with context

### Type Safety
- Use Pydantic models for configuration validation
- Type hints for all function signatures
- Generic types for reusable components
- Optional types for nullable values

## 🚀 Release Process

### Version Management
- Use semantic versioning (MAJOR.MINOR.PATCH)
- Update version in `pyproject.toml`
- Create release notes

### Publishing
```bash
# Build package
poetry build

# Publish to PyPI (requires credentials)
poetry publish
```

## 🐛 Debugging

### Common Issues
1. **Import errors**: Check optional dependencies installation
2. **Connection failures**: Verify service configurations
3. **Memory issues**: Monitor data batch sizes
4. **Performance**: Use profiling tools (`snakeviz`, `yappi`)

### Logging
- Use structured logging with context
- Log levels: DEBUG, INFO, WARNING, ERROR
- Include relevant metadata (job_id, stream_name, etc.)

### Profiling
```bash
# Install profiling tools
poetry install --with dev

# Use snakeviz for visual profiling
# Use yappi for multi-threaded profiling
```

## 📊 Performance Considerations

### Memory Management
- Use Polars for efficient data processing
- Implement configurable batch sizes
- Monitor memory usage in large datasets

### Concurrency
- Thread-based runner for I/O bound tasks
- Process-based runner for CPU bound tasks
- Streaming runner for memory-constrained environments

### Optimization Tips
- Use connection pooling for databases
- Implement proper backoff strategies
- Monitor queue sizes and processing rates
- Profile critical paths regularly

## 🤝 Contributing Guidelines

### Code Style
- Follow Google docstring convention
- Use type hints consistently
- Keep functions focused and small
- Write self-documenting code

### Commit Messages
- Use conventional commits format
- Include context and reasoning
- Reference issues when applicable

### Pull Request Process
1. Create feature branch from main
2. Implement changes with tests
3. Run all quality checks locally
4. Submit PR with description
5. Address review feedback
6. Ensure CI passes

### Documentation
- Update docstrings for public APIs
- Add examples for complex functionality
- Update README for new features
- Include migration guides for breaking changes