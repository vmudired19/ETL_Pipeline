# OpenFEC ETL Pipeline

A production-grade ETL pipeline that extracts Federal Election Commission (FEC) data from the OpenFEC API, loads it into Snowflake, and transforms it using dbt for analytics.

## 🏗️ Architecture

```
OpenFEC API → Python Extract → Snowflake (Raw) → dbt (Staging/Curated) → Analytics
```

## 📋 Features

- **Incremental Data Extraction**: Watermark-based incremental loading to avoid reprocessing
- **Multiple Data Sources**:
  - Schedule A (Itemized Receipts)
  - Candidates Data
  - Committees Data
- **Control Framework**: Run tracking and metadata management
- **Infrastructure as Code**: Snowflake resources managed via Terraform
- **Data Transformation**: dbt models for staging and curated layers

## 🛠️ Tech Stack

- **Language**: Python 3.x
- **Data Warehouse**: Snowflake
- **Transformation**: dbt (data build tool)
- **Infrastructure**: Terraform
- **API**: OpenFEC API v1
- **Dependencies**:
  - `snowflake-connector-python`
  - `requests`
  - `python-dotenv`

## 📁 Project Structure

```
ETL_Pipeline/
├── src/
│   ├── openfec_schedule_a_raw.py    # Schedule A receipts extraction
│   ├── openfc_candidates_raw.py     # Candidates data extraction
│   ├── openfec_committees_raw.py    # Committees data extraction
│   ├── extract.py                   # Generic extraction utilities
│   ├── load_raw.py                  # Data loading utilities
│   └── run_pipeline.py              # Pipeline orchestration
├── dbt/
│   ├── models/
│   │   ├── staging/                 # Staging layer models
│   │   └── curated/                 # Curated/analytical models
│   ├── dbt_project.yml
│   └── profiles.yml
├── terraform/
│   ├── main.tf                      # Snowflake warehouse resources
│   ├── providers.tf
│   ├── variables.tf
│   └── outputs.tf
├── docker-compose.yml
├── .gitignore
└── README.md
```

## 🚀 Setup

### Prerequisites

- Python 3.8+
- Snowflake account
- OpenFEC API key ([Get one here](https://api.open.fec.gov/developers/))
- Terraform installed
- dbt installed

### 1. Clone the Repository

```bash
git clone <repository-url>
cd ETL_Pipeline
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment Variables

Create a `.env` file in the root directory:

```env
# OpenFEC API
OPENFEC_API_KEY=your_api_key_here

# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=DE_WH
SNOWFLAKE_DATABASE=your_database
```

### 5. Provision Snowflake Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### 6. Initialize Control Schema

Run the following in Snowflake to create the control framework:

```sql
CREATE SCHEMA IF NOT EXISTS CONTROL;

CREATE TABLE IF NOT EXISTS CONTROL.ingest_runs (
    run_id NUMBER AUTOINCREMENT,
    source VARCHAR,
    endpoint VARCHAR,
    status VARCHAR,
    last_indexed_date TIMESTAMP_NTZ,
    rows_loaded NUMBER,
    notes VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (run_id)
);
```

### 7. Run the Pipeline

```bash
# Extract Schedule A data
python src/openfec_schedule_a_raw.py

# Extract Candidates data
python src/openfc_candidates_raw.py

# Extract Committees data
python src/openfec_committees_raw.py
```

## 🔄 Data Flow

1. **Extract**: Python scripts pull data from OpenFEC API with incremental watermarking
2. **Load**: Data loaded into Snowflake RAW schema
3. **Transform**: dbt models create staging and curated layers
4. **Control**: Metadata tracked in CONTROL.ingest_runs table

### Watermark Logic

The pipeline uses `last_indexed_date` from the OpenFEC API to track incremental loads:
- First run: Pulls all historical data
- Subsequent runs: Only pulls data modified since last successful run
- Watermarks stored in `CONTROL.ingest_runs` table

## 📊 dbt Models

```bash
cd dbt
dbt deps
dbt run
dbt test
```

## 🐳 Docker Support

```bash
docker-compose up -d
```

## 📝 Data Sources

- **Schedule A**: Itemized receipts (contributions) to campaigns
- **Candidates**: Federal candidate information
- **Committees**: Political committee details

## 🔐 Security

- Never commit `.env` files
- Store credentials in environment variables
- Use Snowflake role-based access control
- Rotate API keys regularly

## 🧪 Testing

```bash
# Test Snowflake connection
python src/test_snowfalee.py

# Run dbt tests
cd dbt
dbt test
```

## 📈 Monitoring

- Check `CONTROL.ingest_runs` for pipeline status
- Monitor row counts and load times
- Set up alerts for failed runs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License.

## 🔗 Resources

- [OpenFEC API Documentation](https://api.open.fec.gov/developers/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Terraform Snowflake Provider](https://registry.terraform.io/providers/Snowflake-Labs/snowflake/latest/docs)

## 📧 Contact

For questions or issues, please open an issue in the repository.