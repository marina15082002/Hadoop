input_file = "./titles.csv"
output_file = "./filtered_titles.csv"

with open(input_file, "r", encoding="utf-8") as file:
    with open(output_file, "w", encoding="utf-8") as output:
        previous_line = ""  # Variable pour stocker la ligne précédente

        for line in file:
            if line.startswith("tm") or line.startswith("ts"):
                if previous_line:
                    output.write(previous_line + "\n")

                previous_line = line.strip()
            else:
                if line.strip():
                    previous_line += line.strip()

        if previous_line:
            output.write(previous_line + "\n")
