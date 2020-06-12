export EP='--endpoint https://commons.bmeg.io'
export PP='--program /programs/ohsu --project CCG_TMP_AWG'


python3 load/gen3_loader.py $EP $PP --path output/gdan-tmp/project.json
python3 load/gen3_loader.py $EP $PP --path output/gdan-tmp/experiment.json
python3 load/gen3_loader.py $EP $PP --path output/gdan-tmp/case.json
