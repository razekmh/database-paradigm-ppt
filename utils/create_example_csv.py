import csv, random
random.seed(10)

with open('data/last_name.csv', newline='') as f:
    reader = csv.reader(f)
    last_name_list = [row[0] for row in reader]
with open('data/first_name.csv', newline='') as f:
    reader = csv.reader(f)
    first_name_list = [row[0] for row in reader]

with open('data/example_csv', 'w') as f:
    writer = csv.writer(f)
    row = ['first_name','last_name']
    writer.writerow(row)
    for i in range(50000000):
        row = [random.choice(first_name_list), random.choice(last_name_list)]
        writer.writerow(row)
