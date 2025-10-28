import pandas as pd
from openai import OpenAI
import re
from datetime import datetime
import os
import time

import kagglehub
from kagglehub import KaggleDatasetAdapter


def clean_csv(input_file):
    # Read CSV
    df_scrape = pd.read_csv(input_file)

    # Load the latest version
    df_kaggle = kagglehub.dataset_load(
    KaggleDatasetAdapter.PANDAS,
    "bwandowando/dpwh-flood-control-projects",
    "dpwh_flood_control_projects.csv",
    # Provide any additional arguments like 
    # sql_query or pandas_kwargs. See the 
    # documenation for more information:
    # https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
    )

    df_kaggle = df_kaggle[["ContractId", "LegislativeDistrict", "Municipality", "DistrictEngineeringOffice", "ApprovedBudgetForContract"]]
    df = pd.merge(df_scrape, df_kaggle, left_on='Contract ID', right_on='ContractId', how='left')
    print()
    # --- 1. Normalize Start Date and Completion Date ---
    def normalize_date(date_str):
        try:
            return datetime.strptime(str(date_str).strip(), "%m/%d/%Y").strftime("%m/%d/%Y")
        except ValueError:
            try:
                return datetime.strptime(str(date_str).strip(), "%m/%d/%y").strftime("%m/%d/%Y")
            except:
                return None  # keep NaT if invalid

    if "Start Date" in df.columns:
        df["Start Date"] = df["Start Date"].apply(normalize_date)

    if "Completion Date" in df.columns:
        df["Completion Date"] = df["Completion Date"].apply(normalize_date)

    # --- 2. Extract Latitude & Longitude from "Long Lat" ---
    if "Long Lat" in df.columns:
        df[["Latitude", "Longitude"]] = df["Long Lat"].str.extract(r"\(([-\d\.]+)\s*,\s*([-\d\.]+)\)")
        df.drop(columns=["Long Lat"], inplace=True)

    # --- 3. Remove rows where "Cost" is not numerical ---
    if "Cost" in df.columns:
        df = df[pd.to_numeric(df["Cost"].str.replace(",", ""), errors="coerce").notna()]

    # --- 4. Remove Report column ---
    if "Report" in df.columns:
        df.drop(columns=["Report"], inplace=True)

    # --- 5. Rename Municipality column ---
    def fix_encoding_issues(text):
        if not isinstance(text, str):
            return text
        try:
            # Decode Latin-1 → Encode UTF-8 properly
            fixed = text.encode("utf-8", errors="ignore").decode("latin1", errors="ignore")
        except Exception:
            fixed = text
        return fixed.title().strip()

    df["Municipality"] = (
        df["Municipality"]
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.title()
        .str.strip()
    )
    df["Municipality"] = (
        df["Municipality"]
        .apply(fix_encoding_issues)
        .apply(lambda x: re.sub(r"^City\s+Of\s+(.+)", r"\1 City", x) if isinstance(x, str) else x)
    )
    # --- 6. Replace Province column if HUC or ICC ---
    huc_icc_cities = [
        "Manila City", "Quezon City", "Caloocan City", "Las Piñas City", "Makati City",
        "Malabon City", "Mandaluyong City", "Marikina City", "Muntinlupa City", "Navotas City",
        "Parañaque City", "Pasay City", "Pasig City", "Taguig City", "Valenzuela City",
        "Baguio City", "Angeles City", "Olongapo City", "Lucena City", "Puerto Princesa City",
        "Iloilo City", "Bacolod City", "Cebu City", "Lapu-Lapu City", "Mandaue City", "Tacloban City",
        "Zamboanga City", "Iligan City", "Cagayan de Oro City", "Davao City", "General Santos City",
        "Butuan City", "Cotabato City", "Dagupan City", "Naga City", "Ormoc City", "Santiago City"
    ]

    df["Province"] = df.apply(
    lambda row: row["Municipality"] if row["Municipality"] in huc_icc_cities else row["Province"],
    axis=1
    )

    df["Province"] = df["Province"].str.title()

    # --- 7. Create Contractor Table ---
    contractor_table = pd.DataFrame()
    if "Contractor" in df.columns and "Contract ID" in df.columns:

        def clean_contractor_name(name):
            """
            Remove (FORMERLY: ...) fragments and strip spaces.
            Keep other parts (like INC.) intact.
            """
            if pd.isna(name):
                return ""
            s = str(name)

            # Remove patterns like "(FORMERLY: ...)" with optional whitespace
            s = re.sub(r"\(\s*FORMERLY\s*:.*?\)", "", s, flags=re.IGNORECASE)
            s = re.sub(r"\(\s*FORMERLY \s*:.*?\)", "", s, flags=re.IGNORECASE)
            s = re.sub(r"\(\s*FORM\s*:.*?\)", "", s, flags=re.IGNORECASE)
            s = re.sub(r"\(\s*FOR\s*:.*?\)", "", s, flags=re.IGNORECASE)
            s = re.sub(
                r"\(\s*(FORMERLY|FOR)\s*[:\.\)]?[^)]*\)",
                "",
                s,
                flags=re.IGNORECASE
            )

            # Also catch unclosed variants like '(FORMERLY ' or '(FOR:'
            s = re.sub(
                r"\(\s*(FORMERLY|FOR)[^)]*$",
                "",
                s,
                flags=re.IGNORECASE
            )

            # Collapse multiple spaces and strip
            s = re.sub(r"\s+", " ", s).strip()

            # Remove trailing punctuation leftover (like trailing '/' or ','), but keep internal punctuation like 'INC.'
            s = s.rstrip(" ,;/")
            return s

        for _, row in df.iterrows():
            contractors_raw = str(row["Contractor"])

            # Split only on slash ("/") with optional surrounding whitespace.
            # This prevents splitting on commas inside names like "ACME, INC."
            contractors = [c.strip() for c in re.split(r"\s*/\s*", contractors_raw) if c.strip()]

            # Clean each contractor name (remove formerly notes, trim)
            cleaned_contractors = [clean_contractor_name(c) for c in contractors]

            contract_type = "solo" if len(cleaned_contractors) == 1 else "multiple"

            # Append each contractor as a separate row, keeping same Contract ID
            for contractor in cleaned_contractors:
                contractor_table = pd.concat([
                    contractor_table,
                    pd.DataFrame({
                        "Contract ID": [row["Contract ID"]],
                        "Contractor": [contractor],
                        "Contract Type": [contract_type]
                    })
                ], ignore_index=True)

    # --- Save cleaned project table ---
    base, ext = os.path.splitext(input_file)
    output_file = f"{base}_cleaned{ext}"
    df.to_csv(output_file, index=False)

    print(f"Cleaned file saved as: {output_file}")

    # --- Save contractor table ---
    if not contractor_table.empty:
        contractor_file = f"{base}_contractors{ext}"
        contractor_table.to_csv(contractor_file, index=False)
        print(f"Contractor table saved as: {contractor_file}")
    else:
        print("No contractor data found to export.")


# Example usage:
if __name__ == "__main__":
    clean_csv("flood-control-data.csv")
