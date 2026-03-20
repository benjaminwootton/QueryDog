#!/bin/bash
#
# QueryDog Installation Script
# Sets up the 'querydog' command to run via Docker
#

set -e

DOCKER_IMAGE="ghcr.io/benjaminwootton/querydog"
SHELL_RC=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "  QueryDog Installer"
echo "  ==================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed.${NC}"
    echo ""
    echo "Please install Docker first:"
    echo "  - macOS: https://docs.docker.com/desktop/install/mac-install/"
    echo "  - Linux: https://docs.docker.com/engine/install/"
    echo "  - Windows: https://docs.docker.com/desktop/install/windows-install/"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} Docker is installed"

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker daemon is not running.${NC}"
    echo ""
    echo "Please start Docker and try again."
    exit 1
fi

echo -e "${GREEN}✓${NC} Docker daemon is running"

# Detect shell and config file
detect_shell_config() {
    local shell_name=$(basename "$SHELL")

    case "$shell_name" in
        bash)
            if [ -f "$HOME/.bash_profile" ]; then
                SHELL_RC="$HOME/.bash_profile"
            elif [ -f "$HOME/.bashrc" ]; then
                SHELL_RC="$HOME/.bashrc"
            else
                SHELL_RC="$HOME/.bash_profile"
            fi
            ;;
        zsh)
            SHELL_RC="$HOME/.zshrc"
            ;;
        fish)
            SHELL_RC="$HOME/.config/fish/config.fish"
            ;;
        *)
            SHELL_RC="$HOME/.profile"
            ;;
    esac
}

detect_shell_config
echo -e "${GREEN}✓${NC} Detected shell config: $SHELL_RC"

# The querydog function to add
QUERYDOG_FUNCTION='
# QueryDog CLI
querydog() {
    local DOCKER_IMAGE="ghcr.io/benjaminwootton/querydog"
    local CONFIG_FILE=""
    local QUERIES_DIR=""

    # Find querydog.yaml: check current dir first, then home dir
    if [ -f "./querydog.yaml" ]; then
        CONFIG_FILE="$(pwd)/querydog.yaml"
    elif [ -f "$HOME/querydog.yaml" ]; then
        CONFIG_FILE="$HOME/querydog.yaml"
    fi

    # Find queries folder: check current dir first, then home dir
    if [ -d "./queries" ]; then
        QUERIES_DIR="$(pwd)/queries"
    elif [ -d "$HOME/queries" ]; then
        QUERIES_DIR="$HOME/queries"
    fi

    # Build docker args array for proper quoting
    local DOCKER_ARGS=()
    if [ -n "$CONFIG_FILE" ]; then
        DOCKER_ARGS+=("-v" "${CONFIG_FILE}:/app/querydog.yaml")
    fi
    if [ -n "$QUERIES_DIR" ]; then
        DOCKER_ARGS+=("-v" "${QUERIES_DIR}:/app/queries")
    fi

    if [ "$1" = "ui" ]; then
        shift
        local PORT="${1:-3001}"
        local CONTAINER_NAME="querydog-ui"

        # Check if container is already running
        if docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
            echo "QueryDog UI is already running at http://localhost:$(docker port $CONTAINER_NAME 3001 | cut -d: -f2)"
            echo "Stop it with: docker stop $CONTAINER_NAME"
            return 0
        fi

        # Remove stopped container if exists
        docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

        echo "Starting QueryDog UI..."
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "${PORT}:3001" \
            "${DOCKER_ARGS[@]}" \
            "$DOCKER_IMAGE" server

        echo ""
        echo "QueryDog UI is running at http://localhost:$PORT"
        echo ""
        echo "To stop: docker stop $CONTAINER_NAME"
    else
        # Run CLI command
        docker run --rm -it \
            "${DOCKER_ARGS[@]}" \
            "$DOCKER_IMAGE" "$@"
    fi
}
'

# Check if already installed
if grep -q "# QueryDog CLI" "$SHELL_RC" 2>/dev/null; then
    echo -e "${YELLOW}!${NC} QueryDog is already installed in $SHELL_RC"
    echo ""
    read -p "Do you want to reinstall? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
    # Remove existing installation
    sed -i.bak '/# QueryDog CLI/,/^}$/d' "$SHELL_RC"
    echo -e "${GREEN}✓${NC} Removed existing installation"
fi

# Add the function to shell config
echo "$QUERYDOG_FUNCTION" >> "$SHELL_RC"
echo -e "${GREEN}✓${NC} Added querydog function to $SHELL_RC"

# Pull the Docker image
echo ""
echo "Pulling Docker image..."
if docker pull "$DOCKER_IMAGE"; then
    echo -e "${GREEN}✓${NC} Docker image pulled successfully"
else
    echo -e "${YELLOW}!${NC} Could not pull image (will be pulled on first use)"
fi

echo ""
echo -e "${GREEN}Installation complete!${NC}"
echo ""
echo "To start using querydog, either:"
echo "  1. Open a new terminal window, or"
echo "  2. Run: source $SHELL_RC"
echo ""
echo "Usage:"
echo "  querydog              - Show available commands"
echo "  querydog -e <env>     - Run CLI with environment"
echo "  querydog tables -e 1  - List tables from first environment"
echo "  querydog ui           - Start the web UI on port 3001"
echo "  querydog ui 8080      - Start the web UI on port 8080"
echo ""
