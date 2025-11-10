#!/usr/bin/env python3
"""
Script to upload files to AWS S3 and register DOIs with DataCite.
Processes publications_metadata.csv, uploads files from /data directory,
creates DOIs via DataCite REST API, and updates the CSV with file_url and doi.
"""

import os
import sys
import logging
import base64
import re
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "mvedra-repo-1")

DATACITE_USERNAME = os.getenv("DATACITE_USERNAME")
DATACITE_PASSWORD = os.getenv("DATACITE_PASSWORD")
DATACITE_REPOSITORY_ID = os.getenv("DATACITE_REPOSITORY_ID")
# Repository-level authentication (optional - if set, uses repository ID as username)
DATACITE_REPOSITORY_PASSWORD = os.getenv("DATACITE_REPOSITORY_PASSWORD")
DATACITE_API_BASE_URL = "https://api.test.datacite.org"

# Store the repository prefix (set during verification)
REPOSITORY_PREFIX = None

# File paths
DATA_DIR = Path(__file__).parent / "data"
CSV_FILE = Path(__file__).parent / "publications_metadata.csv"


def get_datacite_credentials():
    """
    Get DataCite credentials - either repository-level or account-level.

    Returns:
        Tuple of (username, password) for authentication
    """
    # If repository password is set, use repository-level authentication
    if DATACITE_REPOSITORY_PASSWORD:
        logger.info("Using repository-level authentication (repository ID as username)")
        return (DATACITE_REPOSITORY_ID, DATACITE_REPOSITORY_PASSWORD)
    else:
        # Otherwise use account-level credentials
        logger.info("Using account-level authentication")
        return (DATACITE_USERNAME, DATACITE_PASSWORD)


def get_datacite_credentials_with_fallback():
    """
    Get DataCite credentials with fallback option.
    Tries repository-level first, then account-level if available.

    Returns:
        Tuple of (username, password, auth_type) for authentication
    """
    # If repository password is set, use repository-level authentication
    if DATACITE_REPOSITORY_PASSWORD:
        return (DATACITE_REPOSITORY_ID, DATACITE_REPOSITORY_PASSWORD, "repository")
    elif DATACITE_USERNAME and DATACITE_PASSWORD:
        return (DATACITE_USERNAME, DATACITE_PASSWORD, "account")
    else:
        return (None, None, None)


def validate_environment():
    """Validate that all required environment variables are set."""
    required_vars = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "DATACITE_REPOSITORY_ID": DATACITE_REPOSITORY_ID,
    }

    # Check for either repository-level OR account-level credentials
    has_repo_auth = bool(DATACITE_REPOSITORY_PASSWORD)
    has_account_auth = bool(DATACITE_USERNAME and DATACITE_PASSWORD)

    if not has_repo_auth and not has_account_auth:
        logger.error("Missing DataCite authentication credentials.")
        logger.error("You must provide either:")
        logger.error(
            "  1. Repository-level: DATACITE_REPOSITORY_PASSWORD (uses repository ID as username)"
        )
        logger.error("  2. Account-level: DATACITE_USERNAME and DATACITE_PASSWORD")
        logger.error("Please create a .env file with the required credentials.")
        sys.exit(1)

    missing = [var for var, value in required_vars.items() if not value]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error("Please create a .env file with the required credentials.")
        sys.exit(1)


def initialize_s3_client():
    """Initialize and return S3 client."""
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        sys.exit(1)


def upload_file_to_s3(s3_client, file_path: Path, s3_key: str) -> Optional[str]:
    """
    Upload a file to S3 and return the public URL.

    Args:
        s3_client: Boto3 S3 client
        file_path: Path to local file
        s3_key: S3 object key (filename)

    Returns:
        S3 URL if successful, None otherwise
    """
    try:
        logger.info(f"Uploading {file_path.name} to S3...")
        s3_client.upload_file(str(file_path), S3_BUCKET_NAME, s3_key)

        # Generate S3 URL
        s3_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        logger.info(f"Successfully uploaded to {s3_url}")
        return s3_url

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except NoCredentialsError:
        logger.error("AWS credentials not found or invalid")
        return None
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error uploading to S3: {e}")
        return None


def validate_required_fields(row: pd.Series, file_name: str) -> Tuple[bool, List[str]]:
    """
    Validate that all required fields for DOI creation are present in the CSV row.

    Args:
        row: Pandas Series representing a CSV row
        file_name: Name of the file being processed (for error messages)

    Returns:
        Tuple of (is_valid, list_of_missing_fields)
    """
    missing_fields = []

    # Required: title_main
    if pd.isna(row.get("title_main")) or str(row.get("title_main", "")).strip() == "":
        missing_fields.append("title_main")

    # Required: at least one creator
    has_creator = False
    for i in range(1, 4):
        creator_name = row.get(f"creator_{i}_name")
        if pd.notna(creator_name) and str(creator_name).strip():
            has_creator = True
            break
    if not has_creator:
        missing_fields.append("creator_1_name (at least one creator is required)")

    # Required: publication_date (needed for publicationYear)
    if (
        pd.isna(row.get("publication_date"))
        or str(row.get("publication_date", "")).strip() == ""
    ):
        missing_fields.append("publication_date")

    # Required for published DOIs: publisher (or thesis_university for dissertations)
    has_publisher = False
    if pd.notna(row.get("publisher")) and str(row.get("publisher", "")).strip():
        has_publisher = True
    elif (
        pd.notna(row.get("thesis_university"))
        and str(row.get("thesis_university", "")).strip()
    ):
        has_publisher = True  # Can use thesis_university as fallback

    if not has_publisher:
        missing_fields.append("publisher (or thesis_university for dissertations)")

    # Required: resource_type
    if (
        pd.isna(row.get("resource_type"))
        or str(row.get("resource_type", "")).strip() == ""
    ):
        missing_fields.append("resource_type")

    if missing_fields:
        logger.error(f"Missing required fields for {file_name}:")
        for field in missing_fields:
            logger.error(f"  - {field}")
        return False, missing_fields

    return True, []


def map_csv_to_datacite_schema(row: pd.Series, s3_url: str) -> Dict[str, Any]:
    """
    Map CSV row data to DataCite JSON API schema.

    Args:
        row: Pandas Series representing a CSV row
        s3_url: S3 URL of the uploaded file

    Returns:
        Dictionary with DataCite JSON API format

    Raises:
        ValueError: If required fields are missing
    """
    # Determine resource type
    resource_type_str = str(row.get("resource_type", ""))
    if "Journal article" in resource_type_str:
        resource_type_general = "Text"
        resource_type = "JournalArticle"
    elif "Dissertation" in resource_type_str:
        resource_type_general = "Text"
        resource_type = "Dissertation"
    else:
        resource_type_general = "Other"
        resource_type = resource_type_str

    # Build titles array (title_main is REQUIRED)
    titles = []
    if pd.notna(row.get("title_main")) and str(row.get("title_main", "")).strip():
        titles.append(
            {
                "title": str(row["title_main"]).strip(),
                "lang": str(row.get("title_main_language", "en")),
            }
        )
    else:
        raise ValueError("Required field 'title_main' is missing or empty")

    if (
        pd.notna(row.get("title_translated"))
        and str(row.get("title_translated", "")).strip()
    ):
        titles.append(
            {
                "title": str(row["title_translated"]).strip(),
                "lang": str(row.get("title_translated_language", "en")),
            }
        )

    # Build creators array (at least one creator is REQUIRED)
    creators = []
    for i in range(1, 4):  # creator_1, creator_2, creator_3
        name = row.get(f"creator_{i}_name")
        if pd.notna(name) and str(name).strip():
            creator = {"name": str(name).strip(), "nameType": "Personal"}

            # Add affiliation if available
            affiliation = row.get(f"creator_{i}_affiliation")
            if pd.notna(affiliation) and str(affiliation).strip():
                creator["affiliation"] = [{"name": str(affiliation).strip()}]

            # Add ORCID if available
            orcid = row.get(f"creator_{i}_orcid")
            if pd.notna(orcid) and str(orcid).strip():
                orcid_value = str(orcid).strip()
                if not orcid_value.startswith("http"):
                    orcid_value = f"https://orcid.org/{orcid_value}"
                creator["nameIdentifiers"] = [
                    {"nameIdentifier": orcid_value, "nameIdentifierScheme": "ORCID"}
                ]

            creators.append(creator)

    if not creators:
        raise ValueError("At least one creator (creator_1_name) is required")

    # Build dates array
    dates = []
    publication_year = None
    if pd.notna(row.get("publication_date")):
        pub_date = str(row["publication_date"])
        dates.append({"date": pub_date, "dateType": "Issued"})
        # Extract year from publication_date for publicationYear field
        # Try to parse the date and extract year
        try:
            # Try common date formats
            for fmt in ["%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%Y"]:
                try:
                    dt = datetime.strptime(pub_date, fmt)
                    publication_year = dt.year
                    break
                except ValueError:
                    continue
            # If no format matched, try to extract 4-digit year
            if publication_year is None:
                year_match = re.search(r"\b(19|20)\d{2}\b", pub_date)
                if year_match:
                    publication_year = int(year_match.group())
        except Exception:
            # If parsing fails, try to extract year from string
            year_match = re.search(r"\b(19|20)\d{2}\b", pub_date)
            if year_match:
                publication_year = int(year_match.group())

    # Build descriptions array
    descriptions = []
    if pd.notna(row.get("description")):
        descriptions.append(
            {"description": str(row["description"]), "descriptionType": "Abstract"}
        )

    # Build subjects (keywords) array
    subjects = []
    if pd.notna(row.get("keywords")):
        keywords = str(row["keywords"]).split("|")
        for keyword in keywords:
            keyword = keyword.strip()
            if keyword:
                subjects.append({"subject": keyword})

    # Build container (for journal articles)
    container = None
    if resource_type == "JournalArticle":
        if pd.notna(row.get("journal_title")):
            container = {"title": str(row["journal_title"]), "type": "Journal"}
            if pd.notna(row.get("journal_issn")):
                container["identifier"] = str(row["journal_issn"])
                container["identifierType"] = "ISSN"
            if pd.notna(row.get("journal_volume")):
                container["volume"] = str(row["journal_volume"])
            if pd.notna(row.get("journal_issue")):
                container["issue"] = str(row["journal_issue"])
            if pd.notna(row.get("journal_page_number")):
                container["firstPage"] = str(row["journal_page_number"])

    # Build publisher
    publisher = None
    if pd.notna(row.get("publisher")):
        publisher = str(row["publisher"])

    # Build DataCite JSON structure
    # Include "event": "publish" to create published DOIs (tested and working)
    attributes = {
        "event": "publish",
        "url": s3_url,
        "titles": titles,
        "creators": creators,
        "types": {
            "resourceTypeGeneral": resource_type_general,
            "resourceType": resource_type,
        },
    }

    # Add prefix explicitly (matches working test payload format)
    if REPOSITORY_PREFIX:
        attributes["prefix"] = REPOSITORY_PREFIX

    # publicationYear is REQUIRED for publishing
    if publication_year:
        attributes["publicationYear"] = publication_year
    elif pd.notna(row.get("publication_date")):
        # Fallback: try to extract year from date string
        pub_date_str = str(row["publication_date"])
        year_match = re.search(r"\b(19|20)\d{2}\b", pub_date_str)
        if year_match:
            attributes["publicationYear"] = int(year_match.group())
        else:
            raise ValueError(
                f"Could not extract publicationYear from publication_date: {pub_date_str}. "
                "Please ensure publication_date contains a valid year (YYYY format or date with year)."
            )
    else:
        raise ValueError(
            "Required field 'publication_date' is missing. This is needed to extract publicationYear."
        )

    if dates:
        attributes["dates"] = dates
    if descriptions:
        attributes["descriptions"] = descriptions
    if subjects:
        attributes["subjects"] = subjects

    # Publisher is REQUIRED for published DOIs (when event is "publish")
    if not publisher and attributes.get("event") == "publish":
        # Try to use thesis_university as publisher for dissertations
        if pd.notna(row.get("thesis_university")):
            publisher = str(row["thesis_university"]).strip()
            logger.warning(
                f"Publisher field is empty. Using thesis_university as publisher: {publisher}"
            )
        else:
            logger.error(
                "ERROR: 'publisher' field is required for published DOIs but is missing in CSV."
            )
            logger.error(
                "Please add a publisher value to your CSV, or use thesis_university for dissertations."
            )
            raise ValueError("Missing required 'publisher' field for published DOI")

    if publisher:
        attributes["publisher"] = publisher

    if container:
        attributes["container"] = container

    # Add funder information if available
    if pd.notna(row.get("funder_1_name")):
        attributes["fundingReferences"] = [{"funderName": str(row["funder_1_name"])}]
        if pd.notna(row.get("funder_1_award_title")):
            attributes["fundingReferences"][0]["awardTitle"] = str(
                row["funder_1_award_title"]
            )

    # Add language
    if pd.notna(row.get("languages")):
        attributes["language"] = str(row["languages"]).lower()

    # Build the complete JSON API structure
    data = {"data": {"type": "dois", "attributes": attributes}}

    return data


def list_datacite_repositories() -> Optional[list]:
    """
    List all repositories accessible with the current credentials.

    Returns:
        List of repository dictionaries with id and name, or None if failed
    """
    try:
        # List repositories endpoint
        repos_url = f"{DATACITE_API_BASE_URL}/repositories"

        username, password = get_datacite_credentials()
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Basic {encoded_credentials}",
        }

        logger.info(f"Fetching repositories from {repos_url}...")
        response = requests.get(repos_url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            repositories = []
            for repo in data.get("data", []):
                attrs = repo.get("attributes", {})
                repo_info = {
                    "id": repo.get("id", ""),
                    "name": attrs.get("name", ""),
                    "prefix": attrs.get("prefix", ""),
                }
                repositories.append(repo_info)
            return repositories
        else:
            logger.error(f"Failed to list repositories: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error listing repositories: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error listing repositories: {e}")
        return None


def verify_datacite_repository() -> bool:
    """
    Verify that the DataCite repository exists and credentials are valid.

    Returns:
        True if repository is accessible, False otherwise
    """
    global REPOSITORY_PREFIX
    try:
        # Try to get repository info
        repo_url = f"{DATACITE_API_BASE_URL}/repositories/{DATACITE_REPOSITORY_ID}"

        username, password = get_datacite_credentials()
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Basic {encoded_credentials}",
        }

        logger.info(f"Verifying repository access at {repo_url}...")
        response = requests.get(repo_url, headers=headers, timeout=30)

        if response.status_code == 200:
            repo_data = response.json()
            repo_attrs = repo_data.get("data", {}).get("attributes", {})
            repo_id = repo_data.get("data", {}).get("id", DATACITE_REPOSITORY_ID)

            # Check if repository has a DOI prefix in attributes
            prefix = repo_attrs.get("prefix", "") or repo_attrs.get("doiPrefix", "")
            name = repo_attrs.get("name", "Unknown")

            # If prefix not in attributes, check relationships
            if not prefix:
                repo_relationships = repo_data.get("data", {}).get("relationships", {})
                prefixes_rel = repo_relationships.get("prefixes", {})
                if prefixes_rel and prefixes_rel.get("data"):
                    # Extract prefix from relationships
                    prefix_data = prefixes_rel["data"]
                    if isinstance(prefix_data, list) and len(prefix_data) > 0:
                        prefix = prefix_data[0].get("id", "")
                        logger.info(f"Found prefix via relationships: {prefix}")
                        # Store globally for use in DOI creation
                        REPOSITORY_PREFIX = prefix

            # If still no prefix, try checking the prefixes endpoint
            if not prefix:
                try:
                    username, password = get_datacite_credentials()
                    creds = f"{username}:{password}"
                    encoded_creds = base64.b64encode(creds.encode()).decode()
                    prefix_headers = {
                        "Content-Type": "application/vnd.api+json",
                        "Authorization": f"Basic {encoded_creds}",
                    }
                    prefixes_url = f"{DATACITE_API_BASE_URL}/repositories/{DATACITE_REPOSITORY_ID}/prefixes"
                    prefixes_response = requests.get(
                        prefixes_url, headers=prefix_headers, timeout=30
                    )
                    if prefixes_response.status_code == 200:
                        prefixes_data = prefixes_response.json()
                        prefixes_list = prefixes_data.get("data", [])
                        if prefixes_list:
                            # Use the first prefix found
                            prefix = prefixes_list[0].get("id", "")
                            logger.info(f"Found prefix via prefixes endpoint: {prefix}")
                            # Store globally for use in DOI creation
                            REPOSITORY_PREFIX = prefix
                except Exception as e:
                    logger.debug(f"Could not check prefixes endpoint: {e}")

            logger.info("Repository access verified successfully")
            logger.info(f"Repository Name: {name}")
            logger.info(f"Repository ID: {repo_id}")
            if prefix:
                logger.info(f"DOI Prefix: {prefix}")
                # Store globally for use in DOI creation
                REPOSITORY_PREFIX = prefix
            else:
                logger.warning(
                    "WARNING: Repository does not have a DOI prefix assigned in API."
                )
                logger.warning("A DOI prefix is required to create DOIs.")
                logger.warning(
                    "If you just assigned a prefix in the DataCite dashboard, there may be a"
                )
                logger.warning(
                    "propagation delay. However, we'll proceed and let the API handle validation."
                )
                # Don't return False - let the API handle the validation
                # The prefix might be assigned but not yet reflected in the API response

            return True
        elif response.status_code == 404:
            logger.error(f"Repository '{DATACITE_REPOSITORY_ID}' not found (404).")
            logger.error("Please verify:")
            logger.error("  1. The repository ID is correct")
            logger.error("  2. The repository exists in the DataCite test environment")
            logger.error("  3. Your credentials have access to this repository")
            logger.error("")
            logger.error("Attempting to list your available repositories...")
            repositories = list_datacite_repositories()
            if repositories:
                logger.error("Available repositories:")
                for repo in repositories:
                    logger.error(
                        f"  - ID: {repo['id']}, Name: {repo['name']}, Prefix: {repo.get('prefix', 'N/A')}"
                    )
                logger.error("")
                logger.error(
                    f"Update DATACITE_REPOSITORY_ID in your .env file with one of the IDs above."
                )
            return False
        elif response.status_code == 401:
            logger.error(
                "Authentication failed (401). Please check your DATACITE_USERNAME and DATACITE_PASSWORD."
            )
            return False
        elif response.status_code == 403:
            logger.error(
                "Access forbidden (403). Your credentials may not have permission to access this repository."
            )
            return False
        else:
            logger.warning(
                f"Repository verification returned status {response.status_code}: {response.text}"
            )
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error verifying repository: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error verifying repository: {e}")
        return False


def create_datacite_doi(metadata: Dict[str, Any]) -> Optional[str]:
    """
    Create a DOI via DataCite REST API.

    Args:
        metadata: DataCite JSON API formatted metadata

    Returns:
        DOI string if successful, None otherwise
    """
    try:
        # Build API URL with repository ID (dots are valid in URLs, so no encoding needed)
        api_url = f"{DATACITE_API_BASE_URL}/repositories/{DATACITE_REPOSITORY_ID}/dois"

        # Create Basic Auth header
        username, password = get_datacite_credentials()
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Basic {encoded_credentials}",
        }

        logger.info(f"Creating DOI via DataCite API at {api_url}...")

        # Log the payload for debugging
        logger.info("Request payload (first 500 chars):")
        payload_str = json.dumps(metadata, indent=2)
        logger.info(payload_str[:500] + ("..." if len(payload_str) > 500 else ""))

        response = requests.post(api_url, headers=headers, json=metadata, timeout=30)

        if response.status_code == 201:
            response_data = response.json()
            doi = response_data.get("data", {}).get("id", "")
            state = (
                response_data.get("data", {})
                .get("attributes", {})
                .get("state", "unknown")
            )
            logger.info(f"Successfully created DOI: {doi} (state: {state})")
            if state == "draft":
                logger.info(
                    "Note: DOI is in draft state. To publish it, update the DOI with 'event': 'publish'"
                )
            return doi
        elif response.status_code == 404:
            logger.error(
                f"Failed to create DOI: 404 - Repository or endpoint not found"
            )
            logger.error(f"Repository ID: {DATACITE_REPOSITORY_ID}")
            logger.error(f"API URL: {api_url}")
            logger.error(f"Response: {response.text}")
            logger.error("")
            logger.error(
                "This usually means you need to use REPOSITORY-LEVEL authentication."
            )
            logger.error("")
            logger.error(
                "IMPORTANT: Based on Fabrica's prompt, you must use repository credentials:"
            )
            logger.error("  - Repository ID as username: VEDRA.GBIPMY")
            logger.error(
                "  - Repository password (the one you use to log into Fabrica)"
            )
            logger.error("")
            logger.error(
                "Account-level credentials don't have access to the /dois endpoint."
            )
            logger.error("")
            logger.error("SOLUTION:")
            logger.error("  1. Set DATACITE_REPOSITORY_PASSWORD in your .env file")
            logger.error(
                "  2. Remove or comment out DATACITE_USERNAME and DATACITE_PASSWORD"
            )
            logger.error(
                "  3. The script will automatically use repository-level authentication"
            )
            logger.error("")
            logger.error(
                "This matches what Fabrica requires: 'Sign in with VEDRA.GBIPMY Repository account'"
            )
            logger.error("")
            logger.error(
                "Since it's been more than a few hours, this is likely a CONFIGURATION ISSUE:"
            )
            logger.error(
                "  1. The prefix may need to be explicitly activated in DataCite Fabrica"
            )
            logger.error(
                "  2. The repository may need additional configuration or permissions"
            )
            logger.error("  3. There may be a missing step in the repository setup")
            logger.error("")
            logger.error("TROUBLESHOOTING STEPS:")
            logger.error("  1. Try creating a DOI manually in DataCite Fabrica:")
            logger.error("     - Go to your repository in Fabrica (test environment)")
            logger.error("     - Navigate to the 'DOIs' tab")
            logger.error("     - Try creating a test DOI through the UI")
            logger.error(
                "     - If this works, the issue is with API permissions/configuration"
            )
            logger.error(
                "     - If this fails, the prefix may not be properly activated"
            )
            logger.error("")
            logger.error("  2. Check prefix status in Fabrica:")
            logger.error("     - Go to repository > Prefixes tab")
            logger.error(
                "     - Verify the prefix shows as 'Active' (not just 'Assigned')"
            )
            logger.error("     - Look for any activation buttons or pending steps")
            logger.error("")
            logger.error("  3. Verify repository settings:")
            logger.error(
                "     - Check repository Settings tab for any missing configurations"
            )
            logger.error(
                "     - Ensure the repository is fully set up and not in draft mode"
            )
            logger.error("")
            logger.error("  4. Contact DataCite Support:")
            logger.error(
                "     - If manual creation also fails, contact support@datacite.org"
            )
            logger.error("     - Provide your repository ID: VEDRA.GBIPMY")
            logger.error("     - Mention the prefix: 10.83545")
            logger.error(
                "     - Explain that the /dois endpoint returns 404 after a day"
            )
            return None
        elif response.status_code == 401:
            logger.error(
                "Authentication failed (401). Please check your DATACITE_USERNAME and DATACITE_PASSWORD."
            )
            logger.error(f"Response: {response.text}")
            return None
        elif response.status_code == 403:
            logger.error(
                "Access forbidden (403). Your credentials may not have permission to create DOIs in this repository."
            )
            logger.error(f"Response: {response.text}")
            logger.error("")
            logger.error("DEBUG INFO:")
            logger.error(f"  Repository ID: {DATACITE_REPOSITORY_ID}")
            logger.error(
                f"  Using repository-level auth: {bool(DATACITE_REPOSITORY_PASSWORD)}"
            )
            logger.error(f"  Prefix in payload: {REPOSITORY_PREFIX}")
            logger.error(
                f"  Payload includes 'event': 'publish': {metadata.get('data', {}).get('attributes', {}).get('event') == 'publish'}"
            )
            logger.error("")
            logger.error("NOTE: The test payload worked with the same credentials.")
            logger.error("The test payload explicitly included 'prefix' in attributes.")
            logger.error("")
            logger.error("Possible issues:")
            logger.error(
                "  1. Repository password in .env might be incorrect or different"
            )
            logger.error(
                "  2. Payload structure might be different (check logged payload above)"
            )
            logger.error(
                "  3. Some metadata fields might be causing validation/permission issues"
            )
            logger.error("")
            logger.error("Try:")
            logger.error(
                "  - Verify repository password matches what you use in Fabrica"
            )
            logger.error(
                "  - Check the logged payload structure matches the working test"
            )
            return None
        elif response.status_code == 422:
            logger.error(
                "Validation error (422). The metadata may be missing required fields."
            )
            logger.error(f"Response: {response.text}")
            logger.error("")
            logger.error("Common causes of 422 errors:")
            logger.error("  - Missing 'publisher' field (required for published DOIs)")
            logger.error(
                "  - Missing 'publicationYear' field (required for published DOIs)"
            )
            logger.error("  - Invalid date format")
            logger.error("  - Missing or invalid creator information")
            logger.error("")
            logger.error(
                "Check your CSV data to ensure all required fields are filled."
            )
            return None
        else:
            logger.error(f"Failed to create DOI: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error creating DOI: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error creating DOI: {e}")
        return None


def process_csv():
    """Main function to process CSV, upload files, and create DOIs."""
    # Validate environment
    validate_environment()

    # Verify DataCite repository access
    if not verify_datacite_repository():
        logger.error(
            "Cannot proceed without repository access. Please fix the repository configuration."
        )
        sys.exit(1)

    # Initialize S3 client
    s3_client = initialize_s3_client()

    # Read CSV
    try:
        df = pd.read_csv(CSV_FILE)
        logger.info(f"Loaded {len(df)} rows from {CSV_FILE}")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        sys.exit(1)

    # Ensure file_url and doi columns exist
    if "file_url" not in df.columns:
        df["file_url"] = None
    if "doi" not in df.columns:
        df["doi"] = None

    # Process each row
    processed = 0
    skipped = 0
    errors = 0

    for index, row in df.iterrows():
        # Skip empty rows
        if pd.isna(row.get("file_name")) or str(row["file_name"]).strip() == "":
            continue

        # Check if file_url is a valid S3 URL (starts with https://)
        file_url_value = (
            str(row.get("file_url", "")).strip()
            if pd.notna(row.get("file_url"))
            else ""
        )
        has_file_url = file_url_value.startswith("https://")
        has_doi = pd.notna(row.get("doi")) and str(row["doi"]).strip() != ""

        # Skip if both file_url (valid S3 URL) and doi already exist
        if has_file_url and has_doi:
            logger.info(f"Skipping row {index + 1}: file_url and doi already exist")
            skipped += 1
            continue

        file_name = str(row["file_name"]).strip()
        file_path = DATA_DIR / file_name

        logger.info(f"Processing row {index + 1}: {file_name}")

        # Upload to S3 if file_url doesn't exist or is not a valid S3 URL
        s3_url = None
        if not has_file_url:
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                errors += 1
                continue

            s3_url = upload_file_to_s3(s3_client, file_path, file_name)
            if s3_url:
                df.at[index, "file_url"] = s3_url
            else:
                logger.error(f"Failed to upload {file_name} to S3")
                errors += 1
                continue
        else:
            s3_url = file_url_value
            logger.info(f"Using existing file_url: {s3_url}")

        # Create DOI if doi doesn't exist
        if not has_doi:
            # Validate required fields before attempting to create DOI
            is_valid, missing_fields = validate_required_fields(row, file_name)
            if not is_valid:
                logger.error(
                    f"Skipping DOI creation for {file_name} due to missing required fields."
                )
                logger.error("Please add the missing fields to your CSV and try again.")
                errors += 1
                continue

            # Map CSV data to DataCite schema
            try:
                metadata = map_csv_to_datacite_schema(row, s3_url)
            except ValueError as e:
                logger.error(f"Validation error for {file_name}: {e}")
                logger.error("Skipping DOI creation for this row.")
                errors += 1
                continue

            # Create DOI
            doi = create_datacite_doi(metadata)
            if doi:
                df.at[index, "doi"] = doi
                processed += 1
            else:
                logger.error(f"Failed to create DOI for {file_name}")
                errors += 1
        else:
            logger.info(f"Using existing DOI: {row['doi']}")
            processed += 1

    # Save updated CSV
    try:
        df.to_csv(CSV_FILE, index=False)
        logger.info(f"Successfully saved updated CSV to {CSV_FILE}")
        logger.info(
            f"Summary: {processed} processed, {skipped} skipped, {errors} errors"
        )
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        sys.exit(1)


if __name__ == "__main__":
    process_csv()
