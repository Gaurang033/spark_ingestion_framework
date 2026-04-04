=============================================================================
PACKAGING AND DEPLOYMENT
=============================================================================

RECOMMENDED: DATABRICKS ASSET BUNDLES (Automated Build & Deploy)
-----------------------------------------------------------------
Asset Bundles provide the simplest workflow - they automatically build the
wheel and deploy it in ONE command. No need to manually run 'python -m build'!

1. Generate bundle configuration:
   python setup_bundle.py
   
   This creates:
   - databricks.yml (bundle config with automatic build)
   - pyproject.toml (package metadata v2.3.0)
   - setup.py (package setup)
   - MANIFEST.in (file includes)
   - .gitignore (build artifacts)

2. Update databricks.yml:
   - Replace <workspace-id> with your actual workspace ID
   - Configure target environments (dev, staging, prod)
   - Set service principal for production

3. Validate the bundle:
   databricks bundle validate

4. Deploy (builds wheel automatically):
   # Development
   databricks bundle deploy --target dev
   
   # Staging
   databricks bundle deploy --target staging
   
   # Production
   databricks bundle deploy --target prod

That's it! The bundle automatically:
✓ Runs 'python -m build' to create the wheel
✓ Uploads the wheel to workspace
✓ Makes it available at the configured path

Wheel locations after deployment:
- Dev: /Workspace/<user>/.bundles/metadata-ingestion-framework/dev/artifacts/
- Staging: /Workspace/Shared/.bundles/metadata-ingestion-framework/staging/artifacts/
- Prod: /Workspace/Shared/.bundles/metadata-ingestion-framework/prod/artifacts/


ALTERNATIVE: MANUAL WHEEL BUILD (Not Recommended)
--------------------------------------------------
If you need to build the wheel manually without Asset Bundles:

1. Generate configuration files:
   python setup_bundle.py

2. Install build tools:
   pip install build

3. Build the wheel manually:
   python -m build
   
   Creates:
   - dist/metadata_ingestion_framework-2.3.0-py3-none-any.whl
   - dist/metadata_ingestion_framework-2.3.0.tar.gz

4. Upload to Unity Catalog Volume:
   dbfs cp dist/*.whl dbfs:/Volumes/catalog/schema/packages/


INSTALLING THE WHEEL IN PIPELINES
----------------------------------
After deploying with Asset Bundle:

Option 1: Install from bundle artifacts
  %pip install /Workspace/<user>/.bundles/metadata-ingestion-framework/dev/artifacts/*.whl

Option 2: Install from Unity Catalog Volume
  %pip install /Volumes/catalog/schema/packages/metadata_ingestion_framework-2.3.0-py3-none-any.whl

Option 3: Reference in databricks.yml resources
  resources:
    pipelines:
      ingestion_pipeline:
        name: "Metadata Ingestion - ${bundle.target}"
        catalog: ${var.catalog}
        target: ${var.schema}
        libraries:
          - whl: ../artifacts/metadata_ingestion_framework_whl/*.whl

Then use in your pipeline:
  from framework import metadata_driven_table
  
  @metadata_driven_table('data_sources/customers.yaml')
  def ingest_customers():
      pass


DEVELOPMENT INSTALLATION
------------------------
For local development and testing:

1. Install in editable mode:
   pip install -e .

2. With development dependencies:
   pip install -e ".[dev]"

3. With Excel support:
   pip install -e ".[excel]"

This allows you to make code changes without rebuilding the wheel.


VERSION MANAGEMENT
------------------
Version is defined in pyproject.toml:
  [project]
  version = "2.3.0"

Update version for releases following semantic versioning:
- MAJOR.MINOR.PATCH (e.g., 2.3.0)
- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes

After version update:
1. Commit changes to version control
2. Redeploy: databricks bundle deploy --target <env>
   (Bundle automatically rebuilds with new version)


BUNDLE CONFIGURATION DETAILS
-----------------------------
The databricks.yml defines how the bundle builds and deploys:

artifacts:
  metadata_ingestion_framework_whl:
    type: whl
    build: python -m build  # Executed automatically during deploy
    path: .                 # Source code location

When you run 'databricks bundle deploy':
1. Bundle executes 'python -m build' in the project directory
2. Finds the wheel in dist/ directory
3. Uploads to the configured artifact path
4. Wheel is now available for pipelines

Variables can be customized per environment:
variables:
  catalog:
    default: "main"
  schema:
    default: "bronze"

Use in resources:
resources:
  pipelines:
    ingestion_pipeline:
      catalog: ${var.catalog}
      target: ${var.schema}


CONTINUOUS INTEGRATION/DEPLOYMENT
----------------------------------
For CI/CD pipelines (GitHub Actions, Azure DevOps, etc.):

1. Install Databricks CLI:
   pip install databricks-cli

2. Configure authentication:
   export DATABRICKS_HOST="https://adb-<workspace-id>.azuredatabricks.net"
   export DATABRICKS_TOKEN="<your-token>"

3. Deploy in CI/CD:
   databricks bundle validate
   databricks bundle deploy --target prod

4. Example GitHub Actions workflow:
   - name: Deploy to Databricks
     run: |
       pip install databricks-cli
       databricks bundle validate
       databricks bundle deploy --target prod
     env:
       DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
       DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
