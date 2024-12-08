import csv

def process_csv(input_file, output_file):
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Read header
        headers = next(reader)
        writer.writerow(headers)
        
        for row in reader:
            if row[0].endswith('.0'):
                row[0] = str(int(float(row[0])))
            writer.writerow(row)

if __name__ == "__main__":
    input_file = 'contests_cleaned.csv'
    output_file = 'contests_cleaned2.csv'
    process_csv(input_file, output_file)