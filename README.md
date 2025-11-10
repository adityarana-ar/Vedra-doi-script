# Vedra-doi-script

Script to upload files to AWS S3 and register DOIs with DataCite. This script processes `publications_metadata.csv`, uploads corresponding files from the `/data` directory to S3, creates DOIs via the DataCite REST API, and updates the CSV with S3 URLs and DOI values.

## Features

- Uploads files from `/data` directory to AWS S3 bucket
- Creates DOIs via DataCite REST API (test environment)
- Maps CSV metadata to DataCite JSON schema format
- Updates CSV with `file_url` (S3 URL) and `doi` columns
- Skips rows that already have both `file_url` and `doi` values
- Comprehensive error handling and logging

## Prerequisites

- Python 3.7 or higher
- AWS account with S3 access
- DataCite test account credentials
- Files in `/data` directory matching entries in `publications_metadata.csv`

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the project root with your credentials:

```bash
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=ap-south-1
S3_BUCKET_NAME=mvedra-repo-1

# DataCite API Configuration (Test Environment)
DATACITE_USERNAME=your_datacite_username
DATACITE_PASSWORD=your_datacite_password
DATACITE_REPOSITORY_ID=your_repository_id
```

You can use `.env.example` as a template (note: `.env.example` creation may be blocked by system settings, but you can create it manually).

### 3. Prepare Your Data

- Ensure `publications_metadata.csv` exists in the project root
- Place files referenced in the `file_name` column in the `/data` directory
- The CSV should have columns for all metadata fields (see CSV structure below)

## Usage

Run the script:

```bash
python upload_and_register_doi.py
```

The script will:

1. Read `publications_metadata.csv`
2. For each row:
   - Skip if both `file_url` and `doi` already exist
   - Upload file from `/data` directory to S3 (if `file_url` is missing)
   - Create DOI via DataCite API (if `doi` is missing)
   - Update CSV with new values
3. Save the updated CSV

## CSV Structure

The script expects `publications_metadata.csv` with the following columns:

### Required Fields (for DOI creation)

These fields are **required** and the script will validate them before attempting to create a DOI:

- **`resource_type`** - Publication type (e.g., "Publication: Journal article", "Dissertation")
- **`title_main`** - Main title of the publication
- **`publication_date`** - Publication date (must contain a valid year, e.g., "2022-07-30" or "30-Jul-2022")
- **`creator_1_name`** - At least one creator name is required
- **`publisher`** - Publisher name (required for published DOIs)
  - **OR** `thesis_university` - For dissertations, this can be used as publisher if `publisher` is empty
- **`file_name`** - Name of file in `/data` directory

### Optional Fields

- `title_translated` - Translated title
- `title_main_language`, `title_translated_language` - Language codes
- `creator_2_name`, `creator_3_name` - Additional creators
- `creator_1_affiliation`, `creator_2_affiliation`, `creator_3_affiliation` - Creator affiliations
- `creator_1_orcid`, `creator_2_orcid`, `creator_3_orcid` - ORCID IDs
- `creator_1_role`, `creator_2_role`, `creator_3_role` - Creator roles
- `description` - Description/abstract
- `keywords` - Keywords (pipe-separated: `keyword1|keyword2`)
- `languages` - Language of the publication
- `journal_title`, `journal_issn`, `journal_volume`, `journal_issue`, `journal_page_number` - Journal info (for articles)
- `thesis_university`, `thesis_department`, `thesis_type`, `thesis_submission_date`, `thesis_defense_date` - Thesis info (for dissertations)
- `funder_1_name`, `funder_1_award_title` - Funding information
- `reference_1`, `reference_2`, `reference_3` - References
- `file_type`, `file_size` - File metadata

### Auto-populated Fields (by script)

- `file_url` - S3 URL (populated after upload)
- `doi` - DOI (populated after registration)

## Output

The script will:

- Log progress for each row processed
- Update `publications_metadata.csv` with `file_url` and `doi` values
- Display summary: processed, skipped, and errors count

## Error Handling

The script includes comprehensive validation and error handling:

### Pre-validation (Before API Calls)

The script validates all required fields **before** attempting to create a DOI:

- Checks for missing required fields (title_main, creator_1_name, publication_date, publisher/thesis_university, resource_type)
- Validates that publication_date contains a valid year
- Ensures at least one creator is present
- Provides clear error messages listing all missing fields

### Runtime Error Handling

The script handles:

- Missing files in `/data` directory
- S3 upload failures
- DataCite API errors (400, 401, 403, 422, etc.)
- Missing environment variables
- Invalid CSV format
- Validation errors (missing required fields, invalid date formats)

Errors are logged with detailed messages, and the script continues processing other rows.

## DataCite API

This script uses the DataCite REST API test environment:

- Base URL: `https://api.test.datacite.org`
- Endpoint: `/repositories/{DATACITE_REPOSITORY_ID}/dois`
- Authentication: Basic Auth (username/password)
- Format: JSON API specification

The `DATACITE_REPOSITORY_ID` environment variable is required and is used to construct the API endpoint URL.

For production use, change `DATACITE_API_BASE_URL` in the script from `https://api.test.datacite.org` to `https://api.datacite.org`.

## Notes

- The script uses the DataCite test environment by default
- Files are uploaded to S3 bucket `mvedra-repo-1` in `ap-south-1` region
- Rows with existing `file_url` and `doi` values are automatically skipped
- The script preserves the original CSV structure and formatting
