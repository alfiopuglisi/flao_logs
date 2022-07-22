
This script must be run on sxadsec or dxadsec. It will analyze one day at a time. Change the timestamp and the output file in the example.


usage: ./analyse_uao.pys [-h] [--html] [--outdir OUTDIR] [--verbose] day side logdir


Example (text output):

./analyse_uao.py 20220511 L /local/aolog

Example (html output):

./analyse_uao.py --html 20220511 L /local/aolog
