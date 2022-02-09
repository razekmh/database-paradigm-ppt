import json
import pandas as pd
in_file = '~/Desktop/codebase/database-paradigm-ppt/utils/example_csv'
n = 0
df = pd.read_csv(in_file)
with open('outfile.json', 'w+') as outfile:
    for row in df.to_dict('records'):
        n += 1
        index_line = '{"index": {"_index": "names", "_type": "_doc", "_id": "' + str(n) + '"}}\n'
        outfile.write(index_line)
        json.dump(row, outfile)
        outfile.write('\n')