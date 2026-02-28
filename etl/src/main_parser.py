import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
import duckdb
from google.cloud import bigquery
import dotenv

dotenv.load_dotenv()

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

GCS_MOUNT_PATH = os.getenv("GCS_MOUNT_PATH", "/mnt/gcs")

ITEM_24_PATH = os.path.join(GCS_MOUNT_PATH, "item_24")
ITEM_25_PATH = os.path.join(GCS_MOUNT_PATH, "item_25")

OUTPUT_PARQUET = os.path.join(GCS_MOUNT_PATH, "dmd_full_mapping.parquet")
OUTPUT_CSV = os.path.join(GCS_MOUNT_PATH, "dmd_full_mapping.csv")

OUTPUT_VMP_PARQUET = os.path.join(GCS_MOUNT_PATH, "item_24.parquet")
OUTPUT_BNF_PARQUET = os.path.join(GCS_MOUNT_PATH, "item_25.parquet")
OUTPUT_DUCKDB = os.path.join(GCS_MOUNT_PATH, "dmd_data.duckdb")

BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "trud_dataset")


# ─────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────

def strip_namespace(tag):
    """
    Remove XML namespace prefix from an ElementTree tag.

    Args:
        tag (str): The full XML tag string, potentially containing a namespace.

    Returns:
        str: The tag string without the namespace prefix.
    """
    return tag.split("}")[-1]

def normalise_ddd_to_mg(val, uom):
    """
    Normalise a DDD value to milligrams (mg) based on its Unit of Measure.

    Args:
        val (float): The numeric DDD value.
        uom (str): The unit of measure (e.g., 'g', 'mg', 'microgram').

    Returns:
        float or None: The calculated value in mg, or None if conversion is not possible.
    """
    if pd.isna(val) or val is None:
        return None
        
    uom_clean = str(uom).strip().lower()
        
    if uom_clean == "mg":
        return float(val)
    elif uom_clean == "g":
        return float(val) * 1000.0
    elif uom_clean in ("microgram", "mcg"):
        return float(val) / 1000.0
        
    # For Million Units (MU) or Units (U), there is no direct mass conversion 
    # without knowing the specific drug substance. They are left as None.
    return None



# ─────────────────────────────────────────────────────────────
# Parse VMP (Item 24)
# ─────────────────────────────────────────────────────────────

def parse_vmp_dataframe(path: str) -> pd.DataFrame:
    """
    Parse TRUD Item 24 Virtual Medicinal Product (VMP) XML files into a DataFrame.

    Iterates through XMLs matching the pattern `f_vmp2*.xml` found within the
    provided directory path. Extracts SNOMED VPID, VMP Name, VTMID, and VPIDPREV
    nodes into a normalized structure.

    Args:
        path (str): The local or mounted path containing Item 24 XML files.

    Returns:
        pd.DataFrame: A populated DataFrame containing VMP reference data.

    Raises:
        RuntimeError: If no matching XML files are found or the resulting DataFrame is empty.
    """
    pattern = os.path.join(path, "**/f_vmp2*.xml")
    files = glob.glob(pattern, recursive=True)

    if not files:
        raise RuntimeError("No f_vmp2 XML files found in Item 24.")

    rows = []

    for file in files:
        print(f"Parsing VMP file: {file}")

        # Use iterative parsing for memory safety
        for event, elem in ET.iterparse(file, events=("end",)):
            if strip_namespace(elem.tag) == "VMP":

                def get(tag):
                    el = elem.find(tag)
                    return el.text.strip() if el is not None and el.text else ""

                rows.append({
                    "vpid": get("VPID"),                 # SNOMED concept ID
                    "vmp_nm": get("NM"),
                    "vtmid": get("VTMID"),
                    "vpid_prev": get("VPIDPREV")
                })

                elem.clear()

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("VMP dataframe is empty.")

    print(f"Loaded {len(df)} VMP records.")
    return df


# ─────────────────────────────────────────────────────────────
# Parse BNF / ATC (Item 25)
# ─────────────────────────────────────────────────────────────

def parse_bnf_dataframe(path: str) -> pd.DataFrame:
    """
    Parse TRUD Item 25 BNF/ATC Supplementary XML files into a DataFrame.

    Searches for and iteratively parses XMLs matching `f_bnf*.xml` to extract
    BNF classification and ATC code linkage to the SNOMED VPID.

    Args:
        path (str): The local or mounted path containing Item 25 XML files.

    Returns:
        pd.DataFrame: A populated DataFrame containing VPID, BNF Code, ATC code, and DDD.

    Raises:
        RuntimeError: If no matching XML files are found or the resulting DataFrame is empty.
    """
    pattern = os.path.join(path, "**/f_bnf*.xml")
    files = glob.glob(pattern, recursive=True)

    if not files:
        raise RuntimeError("No f_bnf XML files found in Item 25.")

    rows = []

    for file in files:
        print(f"Parsing BNF file: {file}")

        for event, elem in ET.iterparse(file, events=("end",)):
            tag = strip_namespace(elem.tag)

            # Schema tolerant: any element containing VPID
            vpid_el = elem.find("VPID")
            if vpid_el is not None and vpid_el.text:

                def get(tag):
                    el = elem.find(tag)
                    return el.text.strip() if el is not None and el.text else ""

                rows.append({
                    "vpid": vpid_el.text.strip(),
                    "bnf_code": get("BNF_CODE"),
                    "atc_code": get("ATC"),
                    "ddd": get("DDD"),
                    "ddd_uom": get("DDD_UOM")
                })

                elem.clear()

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("BNF dataframe is empty.")

    print(f"Loaded {len(df)} BNF records.")
    return df


# ─────────────────────────────────────────────────────────────
# Build Canonical Spine
# ─────────────────────────────────────────────────────────────

def build_spine():
    """
    Execute the core data engineering transformation to create the analytical spine.

    This function calls the VMP and BNF parsing logic, performs a left join
    to append BNF and ATC classification codes onto the primary SNOMED VMP dataset,
    and enforces appropriate data types. Finally, it exports data into Parquet,
    DuckDB, and Google BigQuery as a resilient multi-format target architecture.

    Returns:
        None
    """

    print("Parsing Item 24 (VMP)...")
    vmp_df = parse_vmp_dataframe(ITEM_24_PATH)

    print("Parsing Item 25 (BNF/ATC)...")
    bnf_df = parse_bnf_dataframe(ITEM_25_PATH)

    print("Joining VMP ← BNF on VPID...")

    merged = (
        vmp_df
        .merge(bnf_df, on="vpid", how="left", validate="one_to_many")
    )

    # Type enforcement
    merged["vpid"] = merged["vpid"].astype(str)
    merged["vtmid"] = merged["vtmid"].astype(str)
    merged["ddd"] = pd.to_numeric(merged["ddd"], errors="coerce")
    
    # ─────────────────────────────────────────────────────────────
    # Custom fallback mapping for known missing DDDs
    # ─────────────────────────────────────────────────────────────
    # This dictionary allows patching gaps in the TRUD dataset by applying 
    # WHO ATC/DDD standard values to Virtual Medicinal Products based on string matches.
    custom_ddd_mapping = {
        "co-amoxiclav": {"ddd": 3.0, "ddd_uom": "g", "atc_code": "J01CR02"},
        "co-fluampicil": {"ddd": 2.0, "ddd_uom": "g", "atc_code": "J01CA51"},
        "piperacillin / tazobactam": {"ddd": 14.0, "ddd_uom": "g", "atc_code": "J01CR05"}
    }
    
    print("Applying custom fallback mappings for missing DDDs...")
    for key, mapping in custom_ddd_mapping.items():
        # Find rows where the VMP name contains our target drug AND the DDD is currently missing
        mask = (merged["vmp_nm"].str.lower().str.contains(key, na=False)) & (merged["ddd"].isna())
        
        # Apply the fallback values
        merged.loc[mask, "ddd"] = mapping["ddd"]
        
        if "ddd_uom" in merged.columns:
            merged.loc[mask & merged["ddd_uom"].isna(), "ddd_uom"] = mapping["ddd_uom"]
            
        # Optional: Also patch missing ATC code if it doesn't exist
        merged.loc[mask & merged["atc_code"].isna(), "atc_code"] = mapping["atc_code"]
    
    # ─────────────────────────────────────────────────────────────
    # Normalise DDD to milligrams (mg) where possible
    # ─────────────────────────────────────────────────────────────
    if "ddd_uom" in merged.columns:
        print("Normalising DDD values to mg...")
        merged["ddd_mg"] = merged.apply(lambda row: normalise_ddd_to_mg(row["ddd"], row["ddd_uom"]), axis=1)

    print("Final row count:", len(merged))
    print("With ATC codes:", merged["atc_code"].notna().sum())

    # Deterministic ordering
    merged = merged.sort_values("vpid").reset_index(drop=True)

    # Write individual tables to Parquet
    print(f"Writing VMP parquet to {OUTPUT_VMP_PARQUET}")
    vmp_df.to_parquet(OUTPUT_VMP_PARQUET, index=False)
    
    print(f"Writing BNF parquet to {OUTPUT_BNF_PARQUET}")
    bnf_df.to_parquet(OUTPUT_BNF_PARQUET, index=False)

    print(f"Writing spine parquet to {OUTPUT_PARQUET}")
    merged.to_parquet(OUTPUT_PARQUET, index=False)

    print(f"Writing spine CSV to {OUTPUT_CSV}")
    merged.to_csv(OUTPUT_CSV, index=False)

    # ─────────────────────────────────────────────────────────────
    # Write to DuckDB
    # ─────────────────────────────────────────────────────────────
    print(f"Saving to DuckDB at {OUTPUT_DUCKDB}...")
    with duckdb.connect(OUTPUT_DUCKDB) as con:
        con.execute("CREATE OR REPLACE TABLE vmp AS SELECT * FROM vmp_df")
        con.execute("CREATE OR REPLACE TABLE bnf AS SELECT * FROM bnf_df")
        con.execute("CREATE OR REPLACE TABLE spine AS SELECT * FROM merged")
        print("✅ DuckDB tables created/updated.")

    # ─────────────────────────────────────────────────────────────
    # Upload to BigQuery (if running in GCP with permissions)
    # ─────────────────────────────────────────────────────────────
    print("Uploading to BigQuery...")
    try:
        client = bigquery.Client()
        project_id = client.project
        dataset_id = f"{project_id}.{BQ_DATASET_ID}"
        
        # Create dataset if it doesn't exist
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU" # Or whichever location you prefer
        dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        # Load tables
        print("  -> Uploading vmp table to BigQuery...")
        job1 = client.load_table_from_dataframe(vmp_df, f"{dataset_id}.vmp", job_config=job_config)
        job1.result()
        
        print("  -> Uploading bnf table to BigQuery...")
        job2 = client.load_table_from_dataframe(bnf_df, f"{dataset_id}.bnf", job_config=job_config)
        job2.result()
        
        print("  -> Uploading spine table to BigQuery...")
        job3 = client.load_table_from_dataframe(merged, f"{dataset_id}.spine", job_config=job_config)
        job3.result()
        
        print("✅ BigQuery upload complete.")
    except Exception as e:
        print(f"⚠️ Could not upload to BigQuery. Ensure GCP credentials and permissions are correctly configured. Error: {e}")

    print("✅ dm+d spine build complete.")


# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    build_spine()