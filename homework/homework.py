"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import pandas as pd
    import zipfile
    from pathlib import Path

    input_dir = Path("files/input")
    output_dir = Path("files/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    dfs = []
    for zip_file in sorted(input_dir.glob("*.zip")):
        with zipfile.ZipFile(zip_file, "r") as z:
            dfs.append(pd.read_csv(z.open(z.namelist()[0])))

    df = pd.concat(dfs, ignore_index=True)

    client_df = pd.DataFrame(
        {
            "client_id": df["client_id"],
            "age": df["age"],
            "job": df["job"]
            .str.replace(".", "", regex=False)
            .str.replace("-", "_", regex=False),
            "marital": df["marital"],
            "education": df["education"]
            .str.replace(".", "_", regex=False)
            .replace("unknown", pd.NA),
            "credit_default": (df["credit_default"] == "yes").astype(int),
            "mortgage": (df["mortgage"] == "yes").astype(int),
        }
    )

    campaign_df = pd.DataFrame(
        {
            "client_id": df["client_id"],
            "number_contacts": df["number_contacts"],
            "contact_duration": df["contact_duration"],
            "previous_campaign_contacts": df["previous_campaign_contacts"],
            "previous_outcome": (df["previous_outcome"] == "success").astype(int),
            "campaign_outcome": (df["campaign_outcome"] == "yes").astype(int),
            "last_contact_date": pd.to_datetime(
                df["day"].astype(str) + "-" + df["month"].astype(str) + "-2022",
                format="%d-%b-%Y",
            ).dt.strftime("%Y-%m-%d"),
        }
    )

    economics_df = pd.DataFrame(
        {
            "client_id": df["client_id"],
            "cons_price_idx": df["cons_price_idx"],
            "euribor_three_months": df["euribor_three_months"],
        }
    )

    client_df.to_csv(output_dir / "client.csv", index=False)
    campaign_df.to_csv(output_dir / "campaign.csv", index=False)
    economics_df.to_csv(output_dir / "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
