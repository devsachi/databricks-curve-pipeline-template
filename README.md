# Curve Formation Pipeline

## Overview
This pipeline automates the construction and management of financial market curves using Databricks. It processes raw market data from Unity Catalog, applies sophisticated curve construction techniques, and stores the results back in Unity Catalog for downstream consumption.

## Architecture

### High-Level Flow
1. **Data Ingestion**
   - Raw market data loaded from Unity Catalog
   - Data quality checks performed on input
   - Market data normalized and validated

2. **Curve Construction**
   - Multiple curve types supported:
     - Interest Rate Curves
     - FX Volatility Curves
     - Credit Curves
     - Inflation Curves
   - Business logic applied per curve type
   - Interpolation and smoothing techniques

3. **Data Processing**
   - Parallel processing of curve construction
   - Data validation at each step
   - Results aggregation and combination

4. **Output Generation**
   - Final curve data validated
   - Results written to Unity Catalog
   - Audit logs and metrics captured

### Component Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│   Unity Catalog │     │  Curve Formation │     │ Unity Catalog  │
│   (Input Data)  │────▶│     Pipeline    │────▶│  (Output Data) │
└─────────────────┘     └──────────────────┘     └────────────────┘
                               │
                        ┌──────┴───────┐
                        │ Business     │
                        │ Logic Layer  │
                        └──────────────┘
```

## Features
- **Modular Architecture**
  - Separate business logic from processing
  - Pluggable curve construction modules
  - Extensible data quality framework

- **Curve Types**
  - Interest rate curves (zero rates, forwards)
  - FX volatility surfaces
  - Credit spread curves
  - Inflation curves
  - Volatility curves

- **Data Quality**
  - Input data validation
  - Business rule checks
  - Output data verification
  - Comprehensive reporting

- **Infrastructure**
  - Databricks integration
  - Unity Catalog management
  - Terraform-based deployment

## Setup and Installation

### Prerequisites
- Python 3.9+
- UV (pip install uv)
- VS Code (recommended)
- Databricks workspace access
- Unity Catalog permissions

### Local Development Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/devsachi/databricks-curve-pipeline-template.git
   cd databricks-curve-pipeline-template
   ```

2. Create and activate virtual environment:
   ```bash
   uv venv .venv
   source .venv/bin/activate  # Linux/Mac
   .\.venv\Scripts\activate   # Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # for development
   ```

### Configuration
1. Environment Setup
   - Copy `.env.example` to `.env`
   - Configure environment variables

2. Update Configurations
   - Modify `configs/base.yaml` for common settings
   - Use `configs/dev.yaml` for development
   - Use `configs/prod.yaml` for production

## Usage

### Running the Pipeline

1. **Configure Input Data**
   - Ensure market data is available in Unity Catalog
   - Verify table permissions and access

2. **Update Configuration**
   ```yaml
   # configs/dev.yaml
   input:
     market_data_table: "market_data.raw_prices"
     as_of_date: "2025-08-15"
   
   output:
     curve_table: "curves.processed_curves"
   ```

3. **Execute Pipeline**
   - Use provided Databricks notebook:
     ```python
     dbutils.notebook.run("databricks/notebooks/curve_formation_orchestrator", timeout_seconds=600)
     ```
   - Or run via API/CLI

### Development Workflow

1. **Adding New Curve Types**
   - Create new class in `src/curve_formation/curves/`
   - Implement required interfaces
   - Add unit tests

2. **Modifying Business Logic**
   - Update rules in `src/curve_formation/business_logic/`
   - Validate changes with unit tests
   - Update documentation

3. **Data Quality Changes**
   - Modify checks in `src/utils/data_quality/`
   - Update validation rules
   - Test with sample data

## Testing

### Unit Tests
```bash
pytest tests/unit/
```

### Integration Tests
```bash
pytest tests/integration/
```

### Sample Data
- Use `tests/data/sample_market_data.json` for testing
- Modify sample data as needed

## Infrastructure Management

### Terraform Setup
1. Initialize Terraform:
   ```bash
   cd infra
   terraform init
   ```

2. Plan and Apply:
   ```bash
   terraform plan
   terraform apply
   ```

### Resources Created
- Databricks workspace configurations
- Unity Catalog tables and permissions 
- Required compute resources
- Automated job configuration

### Job Configuration
The pipeline is configured to run automatically every day at 5:30 UTC. The job:
- Creates a dedicated auto-scaling cluster (2-8 nodes)
- Executes the curve formation notebook
- Sends email notifications on success/failure
- Has a 2-hour timeout

To modify the job schedule:
1. Update the cron expression in `infra/modules/databricks/job.tf`:
   ```hcl
   schedule {
     quartz_cron_expression = "0 30 5 * * ?" # Daily at 5:30 UTC
     timezone_id = "UTC"
   }
   ```

To configure the job:
1. Copy `terraform.tfvars.example` to `terraform.tfvars`
2. Set your Databricks workspace URL and notification email
3. Set your Databricks token via environment variable:
   ```bash
   export TF_VAR_databricks_token="your-token-here"
   ```
4. Apply the configuration:
   ```bash
   terraform apply
   ```

## Monitoring and Maintenance

### Logs and Metrics
- Pipeline execution logs in Databricks
- Data quality reports
- Performance metrics

### Troubleshooting
1. Check input data quality
2. Verify configuration settings
3. Review error logs
4. Validate business logic

## CI/CD Pipeline

The project implements a comprehensive CI/CD pipeline using GitHub Actions, integrating code quality checks, testing, and automated deployment to Databricks. Here's a detailed breakdown of each component:

### Pipeline Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│   Code Push/PR  │────▶│   Code Quality   │────▶│ Quality Gates  │
└─────────────────┘     │   & Testing      │     └────────────────┘
                        └──────────────────┘            │
                                                       │
┌─────────────────┐     ┌──────────────────┐          ▼
│   Production    │◀────│    Terraform     │◀────┌────────────────┐
│   Databricks    │     │     Apply        │     │  Terraform     │
└─────────────────┘     └──────────────────┘     │    Plan        │
                                                 └────────────────┘
```

### Detailed Pipeline Stages

#### 1. Code Quality & Testing Stage
This stage runs on every push and pull request:

**a. Environment Setup**
- Uses Python 3.9 runtime
- Installs UV package manager for faster dependency resolution
- Creates isolated virtual environment
```bash
uv venv .venv
source .venv/bin/activate
```

**b. Dependency Management**
- Uses UV for deterministic package installation
- Installs both production and development dependencies:
```bash
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt
```

**c. Code Quality Checks**
- **Ruff**: Fast Python linter
  - Checks code style (PEP 8)
  - Finds potential bugs
  - Enforces best practices
```bash
ruff check .
```

- **Black**: Code formatter
  - Ensures consistent code style
  - Automatically formats Python code
```bash
black --check .
```

- **isort**: Import organization
  - Sorts imports alphabetically
  - Groups imports by type
```bash
isort --check .
```

**d. Testing & Coverage**
- Runs pytest with coverage reporting
- Generates XML reports for SonarQube
- Executes both unit and integration tests
```bash
pytest tests/ --cov=src --cov-report=xml --junitxml=test-results.xml
```

**e. SonarQube Analysis**
- Static code analysis
- Measures code quality metrics:
  - Code coverage
  - Code smells
  - Bugs and vulnerabilities
  - Technical debt
- Configuration in `sonar-project.properties`:
  ```properties
  sonar.sources=src
  sonar.tests=tests
  sonar.python.coverage.reportPaths=coverage.xml
  sonar.python.xunit.reportPath=test-results.xml
  ```

#### 2. Terraform Planning Stage
Runs on pull requests to validate infrastructure changes:

**a. Initialization**
- Sets up Terraform environment
- Configures backend and providers
```bash
terraform init
```

**b. Format Checking**
- Ensures consistent Terraform code style
```bash
terraform fmt -check -recursive
```

**c. Plan Generation**
- Creates execution plan
- Validates infrastructure changes
- Stores plan as artifact
```bash
terraform plan -out=tfplan
```

#### 3. Terraform Apply Stage
Executes on merges to main branch:

**a. Production Deployment**
- Applies infrastructure changes
- Configures Databricks resources:
  - Workspace settings
  - Job configurations
  - Cluster definitions
  - Unity Catalog permissions

**b. Job Configuration**
- Sets up daily curve formation job
- Configures auto-scaling cluster
- Establishes monitoring and notifications

### Environment Configuration

#### Required Secrets
Configure these in GitHub repository settings:

**1. SonarQube Configuration**
- `SONAR_TOKEN`: Authentication token
- `SONAR_HOST_URL`: Analysis server URL

**2. AWS Access**
- `AWS_ACCESS_KEY_ID`: Access key for AWS
- `AWS_SECRET_ACCESS_KEY`: Secret for AWS authentication

**3. Databricks Configuration**
- `DATABRICKS_HOST`: Workspace URL
- `DATABRICKS_TOKEN`: Access token
- `NOTIFICATION_EMAIL`: Alert recipient

### Pipeline Triggers

1. **Pull Requests**:
   - Runs code quality checks
   - Generates Terraform plan
   - Reports status to PR

2. **Main Branch Pushes**:
   - Runs full quality checks
   - Deploys to production
   - Updates infrastructure

3. **Manual Trigger**:
   - Available via workflow_dispatch
   - Useful for debugging

### Pipeline Safety Features

1. **Environment Protection**
   - Production deployment requires approval
   - Protected branch policies
   - Secret scanning

2. **Failure Handling**
   - Job dependency chains
   - Automatic failure notifications
   - Detailed error logging

3. **State Management**
   - Remote state storage
   - State locking
   - Version control integration

## Contributing
1. Fork the repository
2. Create feature branch
3. Submit pull request
4. Add tests and documentation
5. Ensure CI/CD pipeline passes

## License
See LICENSE for details.

## Support
Contact the maintainers for support and questions.