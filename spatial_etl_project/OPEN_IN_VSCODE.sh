#\!/bin/bash
# Open Spatial ETL Project in VS Code

echo "🚀 Opening Spatial ETL Project in VS Code..."
echo ""

# Check if VS Code is installed
if \! command -v code &> /dev/null; then
    echo "❌ VS Code 'code' command not found"
    echo "Install it: Open VS Code → Command Palette → 'Shell Command: Install code command in PATH'"
    exit 1
fi

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Open in VS Code
code "$DIR"

echo "✅ Project opened in VS Code"
echo ""
echo "Quick Start:"
echo "  1. Open Terminal in VS Code (Ctrl + \`)"
echo "  2. Run: python examples/01_simple_geojson.py"
echo "  3. Or press F5 to debug any example"
echo ""
