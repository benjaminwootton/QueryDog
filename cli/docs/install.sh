#!/bin/bash
# QueryDog Skills Installer
# Copies QueryDog Claude Code skills to ~/.claude/skills

set -e

SKILLS_DIR="${HOME}/.claude/skills"

# Skill folders to install
SKILLS=(
    "querydog-analyse-clickhouse"
    "querydog-find-not-null-candidates"
    "querydog-general-usage"
)

echo "QueryDog Skills Installer"
echo "========================="
echo ""

# Find the directory containing the skills by locating this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Look for skills in the script's directory
SKILLS_SRC="$SCRIPT_DIR"

# Verify at least one skill exists
if [ ! -d "${SKILLS_SRC}/${SKILLS[0]}" ]; then
    # Try to find skills by searching for querydog pattern
    SKILLS_SRC=$(find "$HOME" -type d -name "querydog-general-usage" 2>/dev/null | head -1 | xargs dirname 2>/dev/null || echo "")

    if [ -z "$SKILLS_SRC" ] || [ ! -d "$SKILLS_SRC" ]; then
        echo "Error: Could not find QueryDog skills directory"
        echo "Please run this script from the directory containing the skill folders"
        exit 1
    fi
fi

echo "Source: $SKILLS_SRC"
echo ""

# Create skills directory if it doesn't exist
if [ ! -d "$SKILLS_DIR" ]; then
    echo "Creating skills directory: $SKILLS_DIR"
    mkdir -p "$SKILLS_DIR"
fi

# Copy each skill folder
for skill in "${SKILLS[@]}"; do
    src="${SKILLS_SRC}/${skill}"
    dest="${SKILLS_DIR}/${skill}"

    if [ -d "$src" ]; then
        echo "Installing: $skill"
        rm -rf "$dest"
        cp -r "$src" "$dest"
    else
        echo "Warning: Skill folder not found: $src"
    fi
done

echo ""
echo "Installation complete!"
echo "Skills installed to: $SKILLS_DIR"
echo ""
echo "Installed skills:"
for skill in "${SKILLS[@]}"; do
    if [ -d "${SKILLS_DIR}/${skill}" ]; then
        echo "  - $skill"
    fi
done
