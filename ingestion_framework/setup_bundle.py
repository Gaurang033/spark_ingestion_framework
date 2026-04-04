"""
Databricks Asset Bundle Setup Script

This script generates all necessary files for creating a Databricks Asset Bundle
that packages the metadata ingestion framework as a Python wheel file.

Run this script to create:
- databricks.yml (bundle configuration with automatic wheel building)
- pyproject.toml (Python package configuration)
- setup.py (Python package setup)
- .gitignore (ignore build artifacts)

Usage:
    python setup_bundle.py
"""

import os
from pathlib import Path


# Get the root directory of the project
ROOT_DIR = Path(__file__).parent


def create_databricks_yml():
    """Create databricks.yml bundle configuration with automatic build."""
    content = """# Databricks Asset Bundle Configuration
# Documentation: https://docs.databricks.com/dev-tools/bundles/

bundle:
  name: metadata-ingestion-framework

# Include all pipeline source files
include:
  - "framework/**/*.py"
  - "examples/**/*.py"
  - "pyproject.toml"
  - "setup.py"
  - "MANIFEST.in"
  - "README.md"

# Variables that can be overridden per environment
variables:
  catalog:
    description: "Unity Catalog name"
    default: "main"
  schema:
    description: "Schema name for ingestion tables"
    default: "bronze"

# Artifacts - Bundle will automatically build the wheel during deployment
artifacts:
  metadata_ingestion_framework_whl:
    type: whl
    # Build command - executed automatically during 'databricks bundle deploy'
    build: python -m build
    # Source path
    path: .

# Deployment targets
targets:
  # Development environment
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
    
    # Artifacts will be uploaded to user's workspace folder
    artifacts:
      metadata_ingestion_framework_whl:
        path: /Workspace/${workspace.current_user.userName}/.bundles/${bundle.name}/dev/artifacts
    
    # Optional: Run as current user
    run_as:
      user_name: ${workspace.current_user.userName}

  # Staging environment
  staging:
    mode: development
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
      root_path: /Shared/.bundles/${bundle.name}/staging
    
    artifacts:
      metadata_ingestion_framework_whl:
        path: /Workspace/Shared/.bundles/${bundle.name}/staging/artifacts

  # Production environment  
  prod:
    mode: production
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
      root_path: /Shared/.bundles/${bundle.name}/prod
    
    artifacts:
      metadata_ingestion_framework_whl:
        path: /Workspace/Shared/.bundles/${bundle.name}/prod/artifacts

    # Require service principal for production
    run_as:
      service_principal_name: "<service-principal-id>"
    
    # Optional: Require approval for production deployments
    permissions:
      - level: CAN_MANAGE
        service_principal_name: "<admin-service-principal-id>"

# Optional: Define pipeline resources that use the wheel
# resources:
#   pipelines:
#     ingestion_pipeline:
#       name: "Metadata Ingestion - ${bundle.target}"
#       catalog: ${var.catalog}
#       target: ${var.schema}
#       
#       # Reference the built wheel
#       libraries:
#         - whl: ../artifacts/metadata_ingestion_framework_whl/*.whl
#       
#       # Pipeline configuration
#       configuration:
#         "pipelines.useV2DetailsApi": "true"
#       
#       clusters:
#         - label: default
#           autoscale:
#             min_workers: 1
#             max_workers: 5
#             mode: ENHANCED
"""
    
    output_path = ROOT_DIR / "databricks.yml"
    with open(output_path, "w") as f:
        f.write(content)
    print(f"✓ Created: {output_path}")


def create_pyproject_toml():
    """Create pyproject.toml for Python package configuration."""
    content = """[build-system]
requires = ["setuptools>=45", "wheel", "setuptools_scm[toml]>=6.2"]
build-backend = "setuptools.build_meta"

[project]
name = "metadata-ingestion-framework"
version = "2.3.0"
description = "Metadata-driven ingestion framework for Databricks Spark Declarative Pipelines"
readme = "README.md"
authors = [
    {name = "Databricks Data Engineer", email = "gaurangnshah@gmail.com"}
]
license = {text = "Apache-2.0"}
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
]
requires-python = ">=3.9"
dependencies = [
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "black>=23.0",
    "flake8>=6.0",
    "mypy>=1.0",
]
excel = [
    "openpyxl>=3.0",  # For Excel file handling if needed
]

[project.urls]
Homepage = "https://github.com/your-org/metadata-ingestion-framework"
Documentation = "https://github.com/your-org/metadata-ingestion-framework/wiki"
Repository = "https://github.com/your-org/metadata-ingestion-framework"

[tool.setuptools]
packages = ["framework", "framework.core", "framework.readers", "framework.notifications", "framework.utils"]

[tool.setuptools.package-data]
framework = ["py.typed"]

[tool.black]
line-length = 100
target-version = ['py39', 'py310', 'py311']
include = '\\.pyi?$'

[tool.mypy]
python_version = "3.9"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = false

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"
python_classes = "Test*"
python_functions = "test_*"
addopts = "-v --cov=framework --cov-report=term-missing"
"""
    
    output_path = ROOT_DIR / "pyproject.toml"
    with open(output_path, "w") as f:
        f.write(content)
    print(f"✓ Created: {output_path}")


def create_setup_py():
    """Create setup.py for backward compatibility."""
    content = """#!/usr/bin/env python
\"\"\"
Setup script for metadata-ingestion-framework.

This setup.py exists for backward compatibility and delegates to pyproject.toml.
For new installations, use: pip install -e .
\"\"\"

from setuptools import setup

# All configuration is in pyproject.toml
setup()
"""
    
    output_path = ROOT_DIR / "setup.py"
    with open(output_path, "w") as f:
        f.write(content)
    print(f"✓ Created: {output_path}")


def create_manifest():
    """Create MANIFEST.in to include non-Python files in the package."""
    content = """# Include documentation
include README.md
include LICENSE

# Include configuration examples
recursive-include examples *.py
recursive-include examples *.yaml

# Include type hints
recursive-include framework py.typed

# Exclude test files and cache
global-exclude __pycache__
global-exclude *.py[co]
global-exclude .DS_Store
"""
    
    output_path = ROOT_DIR / "MANIFEST.in"
    with open(output_path, "w") as f:
        f.write(content)
    print(f"✓ Created: {output_path}")


def create_gitignore():
    """Create .gitignore for build artifacts."""
    content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Databricks
.databricks/
.bundle/
*.log

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Testing
.pytest_cache/
.coverage
htmlcov/

# OS
.DS_Store
Thumbs.db
"""
    
    output_path = ROOT_DIR / ".gitignore"
    with open(output_path, "w") as f:
        f.write(content)
    print(f"✓ Created: {output_path}")


def create_readme_md():
    """Create README.md from README.py if it doesn't exist."""
    readme_py = ROOT_DIR / "README.py"
    readme_md = ROOT_DIR / "README.md"
    
    if readme_md.exists():
        print(f"ℹ README.md already exists, skipping...")
        return
    
    if readme_py.exists():
        with open(readme_py, "r") as f:
            content = f.read()
        
        # Extract docstring content
        if content.startswith('"""'):
            content = content.split('"""')[1].strip()
        
        with open(readme_md, "w") as f:
            f.write(content)
        print(f"✓ Created: {readme_md}")
    else:
        print(f"⚠ README.py not found, skipping README.md creation")


def main():
    """Generate all bundle configuration files."""
    print("=" * 80)
    print("Databricks Asset Bundle Setup")
    print("=" * 80)
    print()
    
    print("Creating bundle configuration files...")
    print()
    
    create_databricks_yml()
    create_pyproject_toml()
    create_setup_py()
    create_manifest()
    create_gitignore()
    create_readme_md()
    
    print()
    print("=" * 80)
    print("✓ Bundle configuration complete!")
    print("=" * 80)
    print()
    print("📦 SIMPLIFIED WORKFLOW - Asset Bundle handles everything!")
    print("=" * 80)
    print()
    print("1. Update workspace URLs in databricks.yml:")
    print("   - Replace <workspace-id> with your actual workspace ID")
    print("   - Configure service principal for production")
    print()
    print("2. Validate the bundle:")
    print("   databricks bundle validate")
    print()
    print("3. Deploy to dev (builds wheel automatically):")
    print("   databricks bundle deploy --target dev")
    print()
    print("4. Deploy to staging:")
    print("   databricks bundle deploy --target staging")
    print()
    print("5. Deploy to production:")
    print("   databricks bundle deploy --target prod")
    print()
    print("The bundle will automatically:")
    print("  ✓ Build the Python wheel using 'python -m build'")
    print("  ✓ Upload it to the configured workspace path")
    print("  ✓ Make it available for your pipelines")
    print()
    print("Wheel will be available at:")
    print("  Dev: /Workspace/<user>/.bundles/metadata-ingestion-framework/dev/artifacts/")
    print("  Prod: /Workspace/Shared/.bundles/metadata-ingestion-framework/prod/artifacts/")
    print()
    print("To use in your pipeline:")
    print("  %pip install /Workspace/<path-to-wheel>/*.whl")
    print()
    print("For local development:")
    print("  pip install -e .")
    print()


if __name__ == "__main__":
    main()
