# DendroForge

**An Autonomous, Multi-Agent System for Biological Data Analysis**

DendroForge is an advanced, agentic system designed to perform end-to-end biological data analysis. It leverages a sequential chain of specialized Large Language Model (LLM) agents to autonomously discover data, plan and execute complex coding workflows, and generate comprehensive reports. The system is built using the `openai-agents` SDK and is designed to be run from the command line.

## System Architecture

DendroForge operates on a linear, sequential handoff architecture, where each specialized agent performs its task and then passes control to the next agent in the chain.

The agentic workflow is as follows:

1.  **Initial Planning Agent**: Receives the initial user prompt. Its sole responsibility is to identify the path to the data file and hand off control to the Data Discovery Agent.
2.  **Data Discovery Agent**: Receives the file path. It copies the data into a secure sandbox, analyzes it to determine its structure (e.g., file type, dimensions, schema), and then hands off a summary of its findings to the Coding Planning Agent.
3.  **Coding Planning Agent**: This is the core orchestrator for the analysis. It receives the data summary and the original request, then enters an iterative loop:
    *   It creates a step-by-step coding plan.
    *   It calls a specialized **Coding Agent** as a tool to execute each step of the plan.
    *   It uses file management tools to list, read, and delete files within the session sandbox, keeping the workspace clean.
    *   After the coding plan is complete, it summarizes the work and hands off to the final agent.
4.  **Report Agent**: The final agent in the chain. It receives the summary from the coding planner, reads all the generated files (code, plots, results), and compiles a final, comprehensive `report.md` in the session directory.

This architecture ensures a clear separation of concerns and allows for robust, stateful execution of complex, multi-step tasks.

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd DendroForge

# Install dependencies with uv (recommended)
uv sync
```

## Environment Configuration

Create a `.env` file in the project root. This file is essential for configuring the sandbox directory and the LLM provider.

```bash
# .env

# -- Sandbox Configuration --
# Path to the directory where session-specific subfolders will be created.
# Each agent run gets a unique subfolder containing the sandboxed environment.
# Important: for the current version, the sandbox must be located out of the DendroForge program directory to avoid issues with uv.
SANDBOX_DIR=../sandbox

# -- LLM Provider Configuration (OpenRouter) --
OPENROUTER_API_KEY="your-openrouter-api-key-here"
OPENROUTER_BASE_URL="https://openrouter.ai/api/v1"
OPENROUTER_MODEL_NAME="anthropic/claude-3.5-sonnet"
```

**Setup Steps:**

1.  Copy the configuration above into a `.env` file.
2.  Replace `"your-openrouter-api-key-here"` with your actual API key from [OpenRouter](https://openrouter.ai/keys).
3.  Ensure the directory specified in `SANDBOX_DIR` exists and is writable.

## Usage

DendroForge is run via the command line. You provide a single argument: the path to a text file containing the user prompt.

1.  Create a prompt file (e.g., `prompt.txt`):

    ```text
    I have a large dataset of bulkRNAseq human samples, stored at /path/to/your/data.h5ad. I want to find the top 100 most variable genes across all samples and make a volcano plot with the top 100 marked in a different color. Output your method, the results, and the path to the plot.
    ```

2.  Run the application:

```bash
# With uv
    uv run main.py prompt.txt
```

The system will then execute the entire workflow, printing real-time updates, tool calls, and handoffs to the console. The final report and all artifacts will be saved in a new subdirectory within your specified sandbox directory.