"""
Databricks Asset Bundle Configuration Generator

This script generates the necessary YAML configuration files for deploying
the ingestion framework pipeline across multiple environments (dev, staging, prod).

Usage:
    python generate_bundle_config.py

This will create:
    - databricks.yml (root bundle configuration)
    - resources/pipeline_dev.yml (development environment)
    - resources/pipeline_staging.yml (staging environment)
    - resources/pipeline_prod.yml (production environment)

After generating, deploy with:
    databricks bundle deploy -t dev
    databricks bundle deploy -t staging
    databricks bundle deploy -t prod
"""

import os
import yaml
from pathlib import Path


def generate_root_bundle_config():
    """Generate the main databricks.yml configuration"""
    config = {
        'bundle': {
            'name': 'ingestion_framework'
        },
        'include': [
            'resources/*.yml'
        ],
        'workspace': {
            'host': '${var.workspace_url}'
        },
        'targets': {
            'dev': {
                'mode': 'development',
                'default': True,
                'workspace': {
                    'host': '${var.workspace_url}'
                },
                'variables': {
                    'catalog': 'dev',
                    'schema': 'bronze',
                    'environment': 'dev'
                }
            },
            'staging': {
                'mode': 'development',
                'workspace': {
                    'host': '${var.workspace_url}'
                },
                'variables': {
                    'catalog': 'staging',
                    'schema': 'bronze',
                    'environment': 'staging'
                }
            },
            'prod': {
                'mode': 'production',
                'workspace': {
                    'host': '${var.workspace_url}'
                },
                'variables': {
                    'catalog': 'prod',
                    'schema': 'bronze',
                    'environment': 'prod'
                },
                'run_as': {
                    'service_principal_name': '${var.prod_service_principal}'
                }
            }
        },
        'variables': {
            'workspace_url': {
                'description': 'Databricks workspace URL',
                'default': 'https://your-workspace.cloud.databricks.com'
            },
            'prod_service_principal': {
                'description': 'Service principal for production deployment',
                'default': 'prod-ingestion-sp'
            }
        }
    }
    return config


def generate_pipeline_config(environment):
    """Generate pipeline configuration for specific environment"""
    config = {
        'resources': {
            'pipelines': {
                f'ingestion_framework_{environment}': {
                    'name': f'ingestion_framework_${var.environment}',
                    'target': '${var.catalog}.${var.schema}',
                    'libraries': [
                        {
                            'file': {
                                'path': '${workspace.file_path}/examples/ingest_customer.py'
                            }
                        },
                        {
                            'file': {
                                'path': '${workspace.file_path}/examples/audit_metadata_setup.py'
                            }
                        }
                    ],
                    'configuration': {
                        'pipelines.environment': '${var.environment}'
                    },
                    'continuous': False,
                    'photon': True,
                    'serverless': True,
                    'development': environment != 'prod',
                    'channel': 'CURRENT',
                    'edition': 'ADVANCED',
                    'notifications': [
                        {
                            'alerts': ['on-update-failure', 'on-flow-failure'],
                            'email_recipients': [
                                'dataeng-team@company.com'
                            ]
                        }
                    ] if environment == 'prod' else []
                }
            }
        }
    }
    return config


def generate_job_config(environment):
    """Generate job configuration for automated pipeline runs"""
    config = {
        'resources': {
            'jobs': {
                f'ingestion_job_{environment}': {
                    'name': f'ingestion_framework_job_${var.environment}',
                    'tasks': [
                        {
                            'task_key': 'run_pipeline',
                            'pipeline_task': {
                                'pipeline_id': '${resources.pipelines.ingestion_framework_' + environment + '.id}',
                                'full_refresh': False
                            }
                        },
                        {
                            'task_key': 'record_audit_stats',
                            'depends_on': [
                                {'task_key': 'run_pipeline'}
                            ],
                            'sql_task': {
                                'warehouse_id': '${var.warehouse_id}',
                                'query': {
                                    'query_id': '${var.audit_query_id}'
                                }
                            }
                        }
                    ],
                    'schedule': {
                        'quartz_cron_expression': '0 0 2 * * ?',  # Daily at 2 AM
                        'timezone_id': 'America/Los_Angeles',
                        'pause_status': 'UNPAUSED' if environment == 'prod' else 'PAUSED'
                    },
                    'email_notifications': {
                        'on_failure': ['dataeng-team@company.com']
                    } if environment == 'prod' else {},
                    'max_concurrent_runs': 1,
                    'timeout_seconds': 3600
                }
            }
        }
    }
    return config


def save_yaml(config, filepath):
    """Save configuration as YAML file"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    print(f"✓ Generated: {filepath}")


def generate_readme():
    """Generate README for asset bundle"""
    readme_content = """# Databricks Asset Bundle - Ingestion Framework

This folder contains Databricks Asset Bundle configurations for deploying the ingestion framework across multiple environments.

## Prerequisites

1. **Databricks CLI** (version 0.200.0 or higher)
   ```bash
   databricks --version
   ```

2. **Authentication**
   ```bash
   databricks configure
   ```

3. **Unity Catalog Setup**
   - Catalogs must exist: `dev`, `staging`, `prod`
   - Schema `bronze` must exist in each catalog
   - Appropriate permissions for deployment

## Structure

```
asset_bundle/
├── databricks.yml              # Root bundle configuration
├── resources/
│   ├── pipeline_dev.yml        # Dev pipeline configuration
│   ├── pipeline_staging.yml    # Staging pipeline configuration
│   ├── pipeline_prod.yml       # Prod pipeline configuration
│   ├── job_dev.yml            # Dev job configuration (optional)
│   ├── job_staging.yml        # Staging job configuration (optional)
│   └── job_prod.yml           # Prod job configuration (optional)
└── README.md                   # This file
```

## Configuration

Update `databricks.yml` variables:
- `workspace_url`: Your Databricks workspace URL
- `prod_service_principal`: Service principal for production runs

Each environment targets a different Unity Catalog:
- **Dev:** `dev.bronze.*`
- **Staging:** `staging.bronze.*`
- **Prod:** `prod.bronze.*`

## Deployment

### Deploy to Development
```bash
databricks bundle deploy -t dev
```

### Deploy to Staging
```bash
databricks bundle deploy -t staging
```

### Deploy to Production
```bash
databricks bundle deploy -t prod
```

## Running the Pipeline

### Via Bundle
```bash
# Development
databricks bundle run -t dev ingestion_framework_dev

# Staging
databricks bundle run -t staging ingestion_framework_staging

# Production
databricks bundle run -t prod ingestion_framework_prod
```

### Via Databricks UI
Navigate to **Workflows** → **Delta Live Tables** → Select your pipeline → **Start**

## Environment-Specific Setup

### Before First Deployment

Each environment requires:

1. **Create audit table** (run once per environment)
   ```sql
   -- For dev
   USE CATALOG dev;
   USE SCHEMA bronze;
   -- Run examples/setup/01_create_audit_table.sql

   -- For staging
   USE CATALOG staging;
   USE SCHEMA bronze;
   -- Run examples/setup/01_create_audit_table.sql

   -- For prod
   USE CATALOG prod;
   USE SCHEMA bronze;
   -- Run examples/setup/01_create_audit_table.sql
   ```

2. **Upload source data** to appropriate volumes
   - Dev: `/Volumes/dev/raw/files/`
   - Staging: `/Volumes/staging/raw/files/`
   - Prod: `/Volumes/prod/raw/files/`

3. **Update YAML configs** in `examples/data_sources/` if paths differ per environment

## Validation

After deployment, validate:

```bash
# List deployed resources
databricks bundle resources list -t prod

# Check pipeline status
databricks pipelines get <pipeline-id>
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy Ingestion Framework

on:
  push:
    branches:
      - main
      - develop

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      
      - name: Deploy to Dev
        if: github.ref == 'refs/heads/develop'
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          cd asset_bundle
          databricks bundle deploy -t dev
      
      - name: Deploy to Prod
        if: github.ref == 'refs/heads/main'
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          cd asset_bundle
          databricks bundle deploy -t prod
```

## Troubleshooting

### Bundle Validation Failed
```bash
databricks bundle validate -t dev
```

### View Deployment Logs
```bash
databricks bundle deploy -t dev --verbose
```

### Destroy Resources
```bash
databricks bundle destroy -t dev
```

## Best Practices

1. **Always deploy to dev first** before staging/prod
2. **Test in dev** thoroughly before promoting
3. **Use service principals** for production deployments
4. **Enable notifications** for production pipelines
5. **Monitor audit tables** regularly for data quality
6. **Version control** all configuration changes
7. **Document** environment-specific customizations

## Support

For issues or questions:
- Check pipeline logs in Databricks UI
- Review audit tables for ingestion stats
- Contact data engineering team
"""
    return readme_content


def main():
    """Main execution"""
    print("Generating Databricks Asset Bundle Configuration...\n")
    
    # Generate root bundle config
    root_config = generate_root_bundle_config()
    save_yaml(root_config, 'databricks.yml')
    
    # Generate pipeline configs for each environment
    for env in ['dev', 'staging', 'prod']:
        pipeline_config = generate_pipeline_config(env)
        save_yaml(pipeline_config, f'resources/pipeline_{env}.yml')
        
        # Generate job configs (optional)
        job_config = generate_job_config(env)
        save_yaml(job_config, f'resources/job_{env}.yml')
    
    # Generate README
    readme_content = generate_readme()
    with open('README.md', 'w') as f:
        f.write(readme_content)
    print("✓ Generated: README.md")
    
    print("\n✅ Bundle configuration generated successfully!")
    print("\nNext steps:")
    print("1. Review and customize databricks.yml variables")
    print("2. Update workspace_url in databricks.yml")
    print("3. Create audit tables in each environment (see examples/setup/)")
    print("4. Deploy to dev: databricks bundle deploy -t dev")


if __name__ == '__main__':
    main()
