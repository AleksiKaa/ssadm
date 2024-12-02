import csv, os
from pathlib import Path


def main():

    # All fields in CSV
    field_names = [
        "ID",
        "SecType",
        "Date",
        "Time",
        "Ask",
        "Ask volume",
        "Bid",
        "Bid volume",
        "Ask time",
        "Day's high ask",
        "Close",
        "Currency",
        "Day's high ask time",
        "Day's high",
        "ISIN",
        "Auction price",
        "Day's low ask",
        "Day's low",
        "Day's low ask time",
        "Open",
        "Nominal value",
        "Last",
        "Last volume",
        "Trading time",
        "Total volume",
        "Mid price",
        "Trading date",
        "Profit",
        "Current price",
        "Related indices",
        "Day high bid time",
        "Day low bid time",
        "Open Time",
        "Last trade time",
        "Close Time",
        "Day high Time",
        "Day low Time",
        "Bid time",
        "Auction Time",
    ]

    # Fields relevant to the assignment
    fields_of_interest = ["ID", "SecType", "Last", "Trading time", "Trading date"]

    data_path = Path(f"{os.getcwd()}/test")
    write_path = Path(f"{os.getcwd()}/test_clean")

    # Loop over files in directory
    i = 0
    for f in data_path.glob("*.csv"):

        # Create a file for processed data
        file_name = f.name
        write_f = write_path / file_name
        write_f.touch()

        print(f"Reading file {file_name}")
        with f.open("r") as csv_file, write_f.open("w") as dest_file:
            # Skip header
            for _ in range(13):
                csv_file.readline()

            # Write header
            dest_file.write(",".join([f'"{c}"' for c in fields_of_interest]))
            dest_file.write("\n")

            reader = csv.DictReader(csv_file, delimiter=",", fieldnames=field_names)
            writer = csv.DictWriter(
                dest_file, fieldnames=fields_of_interest, dialect="unix"
            )

            # Write relevant fields to new file
            for row in reader:
                new_row = {field: row[field] for field in fields_of_interest}
                writer.writerow(new_row)

                i += 1
                if i % 1e6 == 0:
                    print(f"{i // 1e6} million lines processed")


if __name__ == "__main__":
    main()
