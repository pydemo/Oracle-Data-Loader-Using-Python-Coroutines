"""
Usage:
	time python load_data.py  -c load_test.py
	time /Bic/home/bicadmin/py36/Python-3.6.0b4/python sa_load.py  -c chunked_load_test.py

"""

import os, sys, re, atexit
import time
import io
from dateutil import parser
from pprint import pprint
from  include.init_job import init
TIMESTAMP, JOB_NAME, TRADE_DATE, HOME = init()
e=sys.exit
#print TIMESTAMP, JOB_NAME, TRADE_DATE, HOME 
dt = parser.parse(TRADE_DATE)
#print dt #datetime.datetime(2016, 10, 26, 0, 0)
FORMAT_0 = dt.strftime('%m%d%Y') #'10262016'
FORMAT_1 = dt.strftime('%Y%m%d') #'20161013'
FORMAT_2 = dt.strftime('%d-%b-%Y') #'26-Oct-2016'
#20161031 31-Oct-2016
dbs_key='DATABASES'

fn = '/Bic/scripts/oats/fix/missing_oats/pycopy/MatchIt_20161110.dat'
statinfo = os.stat(fn)
fsize= statinfo.st_size
chunk=int(fsize/5)
print (fsize)
import os,sys
fh = io.open(fn, "rb")

#l=f.readline()
cuts=[]
curpos=0
for i in range(int(fsize/chunk)):
	print(i)
	fh.seek(chunk, os.SEEK_CUR)
	
	pos = fh.tell()
	l=fh.readline()
	partial_line=fh.tell()-pos
	print(pos, fh.tell(), fh.tell()-pos)
	new_chunk_size=fh.tell() - curpos
	cuts.append([curpos,new_chunk_size])
	#print(cuts)
	curpos=fh.tell()
	#time.sleep(2)
#
fh.close()

end_of_last_chunk=sum(cuts[-1])

if end_of_last_chunk<fsize:
	print(end_of_last_chunk,fsize)
	cuts.append([end_of_last_chunk,fsize-end_of_last_chunk])
print(cuts)
#e(0)
tconf=\
{
	'servers':{'prod': ['bicadmin','jc1lbiorc1p'],'staging':['bicadmin','jc1lbiorc7']},
	dbs_key:{'prod': ['abuzunov','ORADB1P'],'UAT':['oats','ORADB1S']},
	'ctl_files':[
		#'/Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/cl.ctl',
		],
	'data_files':[																					#skip
	#['staging','UAT', '/Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/MatchIt_20161107.dat',		0],
	]
}
for i,c in enumerate(cuts):
	print (c)
	tconf['ctl_files'].append('/Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/all_ctl%d.ctl' % i)
	tconf['data_files'].append(['staging','UAT', fn,		0,0, c])
pprint(tconf)	
print(len(cuts))
#e(0)
log.info(__file__,extra=d)

def modify_ctls():
	
	for ctl_file in tconf['ctl_files']:
		#remove nonzero errors, and add DIRECT=TRUE
		with open(ctl_file, 'r') as content_file:
			content = content_file.read()
			content = content.replace('SILENT = FEEDBACK', 'DIRECT=TRUE')
			content = re.sub(r'ERRORS ?= ?(\d+)', 'ERRORS = 0', content)
		with open(ctl_file, 'w') as content_file: 
			content_file.write(content)
			#e(0)
			
def validate():
	assert len(tconf['ctl_files'])==len(tconf['data_files']), 'Number od data files (%d) does not match number of ctl files(%d).' % (len(tconf['data_files']),len(tconf['ctl_files']))
def cleanup():
	
	log.info('cleanup', extra=d)
	modify_ctls()
atexit.register(validate)
