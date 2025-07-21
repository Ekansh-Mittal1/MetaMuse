# MetaMuse

Agentic Metadata Curation for GEO and PubMed data extraction and linking.

## Quick Start with UV

This project uses [UV](https://github.com/astral-sh/uv) for fast Python package management.

### Prerequisites

1. **Install UV** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Set up environment variables**:
   ```bash
   # Copy the template
   cp env_template.txt .env
   
   # Edit .env with your credentials
   NCBI_EMAIL=your_email@example.com
   OPENROUTER_API_KEY=your_openrouter_api_key
   NCBI_API_KEY=your_ncbi_api_key  # Optional but recommended
   ```

### Installation

```bash
# Install dependencies
uv sync

# Install with development dependencies
uv sync --dev
```

### Usage

#### Main Workflows

```bash
# List available workflows
uv run python main.py --list-workflows

# Run agentic workflow (extraction + linking)
uv run python main.py full_pipeline "Extract metadata for GSM1000981"

# Run single-agent extraction only
uv run python main.py geo_extraction "Extract metadata for GSM1000981"

# Run deterministic workflow
uv run python src/workflows/data_intake.py -i "GSM1000981" --type complete
```

#### Development Commands

```bash
# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src

# Format code
uv run black src/ tests/

# Lint code
uv run ruff check src/ tests/

# Type checking
uv run mypy src/

# Clean build artifacts
rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .ruff_cache/
```

#### Direct Python Execution

```bash
# Run any Python script with UV environment
uv run python main.py --list-workflows
uv run python src/workflows/data_intake.py -i "GSM1000981" --type complete
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `NCBI_EMAIL` | ✅ | Email for NCBI E-Utilities API |
| `OPENROUTER_API_KEY` | ✅ | API key for OpenRouter (LLM provider) |
| `NCBI_API_KEY` | ⚠️ | NCBI API key for higher rate limits |

## Project Structure

```
MetaMuse/
├── src/
│   ├── agents/          # Agent definitions
│   ├── tools/           # Tool implementations
│   ├── workflows/       # Workflow definitions
│   └── prompts/         # Agent prompts
├── sandbox/             # Session outputs
├── unittests/           # Unit tests
├── main.py              # Agentic workflow entry point
├── pyproject.toml       # Project configuration
└── uv.lock              # UV dependency lock file
```

## Workflows

### Agentic Workflows (main.py)

- **`geo_extraction`**: Single-agent metadata extraction
- **`full_pipeline`**: Complete extraction + linking pipeline
- **`linking`**: Linker-only workflow
- **`multi_agent_geo`**: Multi-agent pipeline

### Deterministic Workflows (data_intake.py)

- **`ingestion`**: Metadata extraction only
- **`linker`**: Data linking only  
- **`complete`**: Full ingestion + linking

## Development

### Adding Dependencies

```bash
# Add production dependency
uv add package-name

# Add development dependency
uv add --dev package-name

# Add with specific version
uv add "package-name>=1.0.0"
```

### Updating Dependencies

```bash
# Update all dependencies
uv lock --upgrade

# Update specific package
uv lock --upgrade-package package-name
```

### Virtual Environment

UV automatically manages virtual environments. To activate:

```bash
# Activate the virtual environment
source .venv/bin/activate

# Or use UV run (recommended)
uv run python script.py
```

## Troubleshooting

### Environment Variable Issues

If you get environment variable errors:

1. Check that `.env` file exists and has correct values
2. Verify variables are not empty or have typos
3. Restart your terminal after creating `.env`

### UV Issues

```bash
# Reinstall dependencies
uv sync --reinstall

# Clear UV cache
uv cache clean

# Check UV version
uv --version

# Verify setup
uv run python verify_uv_setup.py
```

### Workflow Issues

- **PubMed abstract extraction fails**: Check `NCBI_EMAIL` and `NCBI_API_KEY`
- **Agentic workflow handoff fails**: Use `full_pipeline` instead of `geo_extraction`
- **HTTP 502 errors**: Temporary server issues, retry later

## Contributing

1. Install development dependencies: `uv sync --dev`
2. Run tests: `uv run test`
3. Format code: `uv run format`
4. Lint code: `uv run lint`

## License

[Add your license here]
