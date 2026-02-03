# Databricks notebook source
# DBTITLE 1,Import Internal Libs
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

"""
This notebook takes in the following args:
* ARG_TOMTOM_VERSION:
Pass in "" to return the latest downloaded version.
Pass in KEYWORD_FETCH_LATEST to redownload the newest dataset.
Pass in "YYYYMM000" to skip downloading and return an older version.
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: {tomtom_version}")


"""
* IS_TOMTOM_DECOUPLED:
Boolean flag to indicate if the tomtom data is decoupled
"""
dbutils.widgets.text(ARG_IS_TOMTOM_DECOUPLED, "")
is_tomtom_decoupled = dbutils.widgets.get(ARG_IS_TOMTOM_DECOUPLED).lower() == "true"
print(f"{ARG_IS_TOMTOM_DECOUPLED}: {is_tomtom_decoupled}")


# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get update && apt-get install -y p7zip

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install patool pyunpack

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from dataclasses import dataclass
import gzip
import os
import shutil
import time
from typing import List

import boto3
import requests

from pyunpack import Archive

FAMILIES = ["MultiNet", "Logistics"]
PRODUCTS = ["EUR", "NAM"]
DELIVERY_TYPE = "full"
ITERATION_TYPE = "commercial"
TOMTOM_API_URL = "https://api.tomtom.com/mcapi"

# Directories
BASE_DIR = "/tmp"
RAW_DATA_DIR = "tomtom-raw"
EXTRACTED_DATA_DIR = "tomtom-extracted"

# S3
LATEST_KEY = "latest"
RELEASES_KEY = "releases"


@dataclass
class Family:
    product_id: str
    name: str
    location: str


@dataclass
class Release:
    """class for keeping track of release metadata"""

    product_id: str
    product_name: str
    family: str
    # Version corresponding to the full incremental release. I.e. '2021-12-001'
    version: str
    # Version corresponding to the monthly release. I.e. '2021-12'
    major_version: str
    location: str


@dataclass
class FileContent:
    name: str
    location: str


def set_up_session() -> requests.Session:
    sess = requests.Session()

    ssm_client = get_ssm_client("tomtom-api-token-ssm", "us-west-2")
    api_key = get_ssm_parameter(ssm_client, "TOMTOM_API_KEY")
    sess.headers.update({"Authorization": f"Bearer {api_key}"})
    return sess


def make_raw_file_path(release_name: str, fname: str) -> str:
    return os.path.join(BASE_DIR, RAW_DATA_DIR, release_name, fname)


def make_extracted_file_path(release_name: str, fname: str) -> str:
    return os.path.join(BASE_DIR, EXTRACTED_DATA_DIR, release_name, fname)


def set_up_directories(release_name: str):
    os.makedirs(os.path.join(BASE_DIR, RAW_DATA_DIR, release_name), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, EXTRACTED_DATA_DIR, release_name), exist_ok=True)


def get_families(sess: requests.Session) -> List[Family]:
    """
    Grab all the families that we care about specifically Multinet and Logistics
    """

    print("getting tomtom families")

    families = []
    response = sess.get(f"{TOMTOM_API_URL}/families")
    for family in response.json()["content"]:
        if family["name"] in FAMILIES:
            families.append(Family(family["id"], family["name"], family["location"]))
    print(families)
    return families


def get_latest_releases_per_family(
    sess: requests.Session, families: List[Family]
) -> List[Release]:
    """
    For each family grab the releases we care about
    """
    print("getting releases")
    # we only care about europe and north america product

    # Grab the latest releases for each family we care about
    # The release must be in the products we care about, must have a delivery type of full, and iteration type of commercial
    releases = []
    for family in families:
        # TODO: Add better error handling?
        product_release_url = f"{family.location}/latest-releases"
        response = sess.get(product_release_url)
        products = response.json()["products"]
        for product in products:
            product_name = product["product"]["name"]
            if product_name not in PRODUCTS:
                continue
            # For each family, get the specific product id. Then get the latest full release for that product_id.
            product_id = product["product"]["id"]
            product_url = f"https://api.tomtom.com/mcapi/releases?filter=deliveryType eq '{DELIVERY_TYPE}' and product.id eq {product_id} and iterationType eq '{ITERATION_TYPE}'&sort=state.date,desc&size=1"
            response = sess.get(product_url)
            release = response.json()["content"][0]
            version = release["version"].replace(".", "-")
            version_components = version.split("-")
            major_version = "-".join(version_components[:2])
            releases.append(
                Release(
                    release["product"]["id"],
                    product_name,
                    family.name,
                    version,
                    major_version,
                    release["location"],
                )
            )

    # Print list of releases for debug purposes
    for release in releases:
        print(release)

    return releases


def get_files(sess: requests.Session, release: Release) -> List[FileContent]:
    """
    Grab all the relevant files for a release
    """
    files = []

    print(f"grabbing all files for release {release.family}-{release.product_name}")
    url = f"{release.location}?label=shpd"

    # TODO: Add better error handling?
    response = sess.get(url)
    contents = response.json()
    for content in contents["contents"]:
        if release.family == "Logistics" or "-mn-" in content["name"]:
            files.append(FileContent(content["name"], content["location"]))

    print(
        f"Number of total files to download for release family {release.family} release product {release.product_name} is {len(files)}"
    )
    return files


def download_file(sess: requests.Session, file: FileContent, release_name: str):
    """
    Download the files and store on disk
    """
    print(f"Downloading {file.name}")
    # manipulate file name so it doesn't have last .xxx in it
    fname = file.name[0 : file.name.rfind(".")]

    # TODO: Better error handling around TomTom API?
    with sess.get(file.location, stream=True) as r:
        with open(make_raw_file_path(release_name, fname), "wb+") as f:
            shutil.copyfileobj(r.raw, f)
    print("Finished downloading file")


def extract_from_7zip(file: FileContent, release_name: str) -> List[str]:
    """
    Extract the 7zip files on disk. NOTE: For whatever reason
    shutil.unpack_archive will extract to the / directory of the instance
    even when we define an extract directory.
    """

    print(f"unpacking {file.name}")
    fname = file.name[0 : file.name.rfind(".")]
    from_ = make_raw_file_path(release_name, fname)
    Archive(from_).extractall(os.path.join(BASE_DIR, EXTRACTED_DATA_DIR, release_name))

    # Clean up raw files
    os.remove(from_)
    print("finished unpacking file")


def is_multinet_file(fname: str) -> bool:
    if ".gz" in fname:
        if (
            # NW data → provides geometry attributes
            "nw." in fname
            # ST data → provides speed restrictions time domains
            or "st.dbf" in fname
            # SR data → provides speed restrictions
            or "sr.dbf" in fname
            # TA data (ta.dbf database file) → Transportation Element Belonging to Area. The Transportation Element belonging
            # to Area describes the relation between the Transportation Elements and the areas in which they belong.
            or "ta.dbf" in fname
            # A1 data(a1.shp, a1.shx, a1.prj, a1.dbf database files) → Administrative Area Order 1,Geometry with Basic
            # Attributes - This is where the State information is specified
            or "a1." in fname
        ):
            return True
    return False


def is_logistics_file(fname: str) -> bool:
    if ".gz" in fname:
        if "lrs.dbf" in fname or "lvc.dbf" in fname or "ltd.dbf" in fname:
            return True
    return False


def find_files_to_unzip(release_name: str) -> List[str]:
    files_to_unzip = []
    for root, _, files in os.walk(
        os.path.join(BASE_DIR, EXTRACTED_DATA_DIR, release_name)
    ):
        for fname in files:
            fpath = os.path.join(root, fname)
            # "/ax/" folder contains all Administrative Areas (a1 to a9) and their Boundary Lines (bl) for a complete country
            # and is not relevant for our use case. Since this is not a map region, including this will break downstream tomtom to table conversion
            # since we assume the presence of map data for each folder.
            if "/ax/" in fpath:
                continue
            if is_multinet_file(fname) or is_logistics_file(fname):
                files_to_unzip.append(fpath)
            else:
                os.remove(fpath)

    return files_to_unzip


def unzip(fpath: str) -> str:
    new_fpath = fpath[0:-3]
    print(f"File Path we need to unzip {fpath}")
    print(f"Unzipping to this file path {new_fpath}")
    with gzip.open(fpath, "rb") as f_in:
        with open(new_fpath, "wb+") as f_out:
            shutil.copyfileobj(f_in, f_out)
            print("completed unzipping file")
    return new_fpath


def copy_to_s3(release: Release, fpath: str, is_tomtom_decoupled: bool):
    s3_file_path = _make_s3_file_path(fpath, release, is_tomtom_decoupled)
    print(f"Uploading {fpath} to S3 bucket {s3_file_path}")
    # TODO: Better error handling around S3 API?
    # boto3.client is only used for non-UC enabled clusters. (instance-profile: safetyplatform-cluster)
    # Please get s3 client from boto3_helpers.get_s3_client for UC enabled clusters. (instance-profile: unity-catalog-cluster)
    client = boto3.client("s3")
    client.upload_file(
        fpath,
        TOMTOM_S3.BUCKET,
        s3_file_path,
        ExtraArgs={"ACL": "bucket-owner-full-control"},
    )


def _make_s3_file_path(fpath: str, release: Release, is_tomtom_decoupled: bool) -> str:
    """
    example fpath: /tmp/tomtom-extracted/multinet-nam-2021-12-003/nam2021_12_003/shpd/mn/usa/mn1/usamn1___________nw.dbf
    return /usa/mn1/nw.dbf
    """
    path = _format_file_path(fpath)

    s3_file_path = os.path.join(
        TOMTOM_S3.S3_PREFIX
        if not is_tomtom_decoupled
        else TOMTOM_S3.S3_PREFIX_DECOUPLED,
        release.version,
        release.family,
    )

    product_name = release.product_name
    if product_name == "EUR":
        s3_file_path = os.path.join(s3_file_path, product_name, path)
    elif product_name == "NAM":
        # We don't add `NAM` to file path b/c
        # its an unknown identifier in our tile gen system
        s3_file_path = os.path.join(s3_file_path, path)

    return s3_file_path.lower()


def _format_file_path(fpath: str) -> str:
    """
    example fpath: /tmp/tomtom-extracted/multinet-nam-2021-12-003/nam2021_12_003/shpd/mn/usa/mn1/usamn1___________nw.dbf
    return usa/mn1/nw.dbf
    """
    split_file_path = fpath.split("/")
    fname = split_file_path.pop()
    formatted_fname = fname.split("_")[-1]
    final_file_path = [
        formatted_fname,
        split_file_path.pop(),
        split_file_path.pop(),
    ]
    final_file_path.reverse()

    return "/".join(final_file_path)


assert (
    _format_file_path(
        "/tmp/tomtom-extracted/logistics-nam-2021-12-003/nam2021_12_003/shpd/mnl/can/cmb/cancmb___________ltd.dbf",
    )
    == "can/cmb/ltd.dbf"
)
assert (
    _format_file_path(
        "/tmp/tomtom-extracted/multinet-eur-2021-12-000/eur2021_12_000/shpd/mn/fra/f26/fraf26___________nw.shp"
    )
    == "fra/f26/nw.shp"
)
assert (
    _format_file_path(
        "/tmp/tomtom-extracted/multinet-nam-2021-12-003/nam2021_12_003/shpd/mn/usa/mn1/usamn1___________nw.dbf"
    )
    == "usa/mn1/nw.dbf"
)


def download_and_extract(sess: requests.Session, release_name: str, file: FileContent):
    download_file(sess, file, release_name)
    extract_from_7zip(file, release_name)


def unzip_and_upload_s3(release: Release, fpath: str, is_tomtom_decoupled: bool):
    new_fpath = unzip(fpath)
    copy_to_s3(release, new_fpath, is_tomtom_decoupled)
    os.remove(fpath)


# COMMAND ----------


def run_download() -> List[Release]:
    start = time.time()
    sess = set_up_session()

    families = get_families(sess)
    releases = get_latest_releases_per_family(sess, families)

    for release in releases:
        files = get_files(sess, release)

        release_name = (
            f"{release.family}-{release.product_name}-{release.version}".lower()
        )

        set_up_directories(release_name)
        # Process files sequentially instead of using a thread pool
        for file in files:
            download_and_extract(sess, release_name, file)

        file_paths_to_unzip = find_files_to_unzip(release_name)
        # Process files sequentially instead of using a thread pool
        for fpath in file_paths_to_unzip:
            unzip_and_upload_s3(release, fpath, is_tomtom_decoupled)

        # delete raw and extracted folders
        shutil.rmtree(
            os.path.join(BASE_DIR, RAW_DATA_DIR, release_name), ignore_errors=True
        )
        shutil.rmtree(
            os.path.join(BASE_DIR, EXTRACTED_DATA_DIR, release_name), ignore_errors=True
        )
    shutil.rmtree(BASE_DIR)

    end = time.time()
    print(f"Script took: {(end - start) / 60} minutes to run")
    return releases


# COMMAND ----------

if is_tomtom_decoupled:
    manifest_s3_prefix = TOMTOM_S3.S3_PREFIX_DECOUPLED
else:
    manifest_s3_prefix = TOMTOM_S3.S3_PREFIX

latest_version = get_manifest(TOMTOM_S3.BUCKET, manifest_s3_prefix, LATEST_KEY)

# If no latest version, attempt a download.
# If we explicitly want to fetch the latest, attempt a download.
if latest_version is None or (tomtom_version) == KEYWORD_FETCH_LATEST:
    releases = run_download()
elif (
    len(tomtom_version) == 0
):  # If desired version is blank, return the latest downloaded version.
    ret_val = {ARG_TOMTOM_VERSION: latest_version}
    exit_notebook(None, ret_val)
else:  # Pass-through, return desired tomtom_version for next step in the pipeline
    ret_val = {ARG_TOMTOM_VERSION: tomtom_version}
    exit_notebook(None, ret_val)

print(releases)

# Verify that all releases have the same version.
versions = set()
for release in releases:
    version = release.version.replace("-", "")
    versions.add(release.version)

if len(versions) == 0:
    exit_notebook("no versions found, aborting.")
if len(versions) > 1:
    print(
        f"WARN: multiple versions found {str(versions)}. Using first release version. See manifest for specific release details."
    )

full_version = releases[0].version
# Update manifest
update_manifest(TOMTOM_S3.BUCKET, manifest_s3_prefix, LATEST_KEY, full_version)
# Write releases to the manifest, so we have a record for the dataset.
releases_metadata = list(
    map(
        lambda r: {
            "product_name": r.product_name,
            "family": r.family,
            "version": r.version,
        },
        releases,
    )
)
update_manifest(TOMTOM_S3.BUCKET, manifest_s3_prefix, RELEASES_KEY, releases_metadata)
# Update version information manifest for the full version.
update_manifest(
    TOMTOM_S3.BUCKET,
    os.path.join(manifest_s3_prefix, full_version),
    RELEASES_KEY,
    releases_metadata,
)


# COMMAND ----------

ret_val = {ARG_TOMTOM_VERSION: full_version}
exit_notebook(None, ret_val)

# COMMAND ----------
