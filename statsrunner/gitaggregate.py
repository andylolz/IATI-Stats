import json, os, sys
from collections import defaultdict
import decimal

dated = len(sys.argv) > 1 and sys.argv[1] == 'dated'
git_out_dir = os.path.join('gitout','gitaggregate-dated' if dated else 'gitaggregate')
if dated:
    gitdates = json.load(open('gitdate.json'))

total = defaultdict(dict)
for fname in os.listdir(git_out_dir):
    if fname.endswith('.json'):
        with open(os.path.join(git_out_dir, fname)) as fp:
            total[fname[:-5]] = json.load(fp, parse_float=decimal.Decimal)

for commit in os.listdir(os.path.join('gitout', 'commits')):
    for fname in os.listdir(os.path.join('gitout', 'commits', commit, 'aggregated')):
        if fname.endswith('.json'):
            with open(os.path.join('gitout', 'commits', commit, 'aggregated', fname)) as fp:
                k = fname[:-5]
                if not commit in total[k]:
                    v = json.load(fp, parse_float=decimal.Decimal)
                    if dated:
                        if commit in gitdates:
                            total[k][gitdates[commit]] = v
                    else:
                        total[k][commit] = v

try:
    os.makedirs(git_out_dir)
except OSError:
    pass

for k,v in total.items():
    with open(os.path.join(git_out_dir, k+'.json'), 'w') as fp:
        json.dump(v, fp, sort_keys=True, indent=2)