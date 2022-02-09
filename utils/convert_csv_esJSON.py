import json
import pandas as pd
in_file = 'data/example_csv'
n = 0
chunksize = 10 ** 6
with pd.read_csv(in_file, chunksize=chunksize) as reader:
        for chunk in reader:
            with open('data/outfile.json', 'w+') as outfile:
                for row in chunk.to_dict('records'):
                    n += 1
                    index_line = '{"index": {"_index": "names", "_type": "_doc", "_id": "' + str(n) + '"}}\n'
                    outfile.write(index_line)
                    json.dump(row, outfile)
                    outfile.write('\n')
                    
