#!/usr/bin/env python3.5
import asyncio
import io
import functools
import signal
import traceback
asyncio.PYTHONASYNCIODEBUG=1
import os, sys
from asyncio.subprocess import PIPE, STDOUT

from optparse import OptionParser
from pprint import pprint

from include.utils import import_module
import builtins

import  include.init_job as  init
TIMESTAMP, JOB_NAME, TRADE_DATE, HOME = init.init()

dr,latest_dir, ts_dir ,d, log= init.dr, init.latest_dir, init.ts_dir, init.d, init.log
config_home = os.path.join(HOME,'config')
e=sys.exit
S_SUCCESS=0	


job_status={}
def makedir(node, dir):
	global config
	status =0
	if is_localhost(node):
		if not os.path.isdir(dir):
			status= os.makedirs(dir)
	else:
		s=config.tconf['servers']
		assert s.has_key(node)
		creds = '@'.join(s[node])	
		cfg=["ssh",  creds,"'test -p %s'" % path ]
		status= os.system(' '.join(cfg))
	return status
def get_dbpassword(db):
	global config,job_status
	env_key='RSBMT_%s_%s' %  (config.dbs_key,db)
	assert env_key in os.environ.keys(), 'Password is not set. Use "export %s=<db password>".'	 % env_key
	return os.environ[env_key]
async def load_data(i,t):
	
	global config
	dbs= config.tconf['DATABASES']
	ctls=config.tconf['ctl_files']
	logdir=os.path.join(ts_dir,'sqlloader')
	max_chunk=200*1024*1024
	min_write_buffer_size= 2*1024*1024
	if 1:
	#for i,t in enumerate(config.tconf['data_files']):
		d['file_id']=i
		host, to_db, data_file, skip_rows, load_rows, cut=  t
		makedir(host, logdir)
		start, payload = cut
		assert payload>0, 'Zero payload.'
		print(cut)
		curr_pos=start
		
		
		print (i, payload, max_chunk, payload/max_chunk)

		
		#e(0)
		#userid='@'.join(dbs[to_db]) 
		userid='/testpwd@'.join(dbs[to_db]) 
		ctlfile=ctls[i]
		fn=os.path.basename(data_file)
		logfn = os.path.join(logdir,'load_data.%s.%d' % (fn,i))
		pwd=get_dbpassword(to_db)	
		
		load=''
		if load_rows:
			load = 'LOAD=%s' % load_rows
		
		if 1: #scan
			#io.BytesIO
			fh = io.open(data_file, "rb")
			#fh = Wrapped(io.open("test0.dat", "rb"))
			#logged = Wrapped(open(filename)) 
			#f.seek(2000000, os.SEEK_CUR)
			fh.seek(start)
			#l=fh.readline()
			#bio = io.BytesIO(f.read(100000))
			print (fh)		
			#f.seek(100000000, os.SEEK_CUR)
		
		loadCMD=('sqlldr SKIP=%s %s data=\\"-\\" userid=%s  control=%s LOG=%s.log BAD=%s.bad DISCARD=/dev/null'  % (skip_rows,load,userid, ctlfile,logfn,logfn))
		print(loadCMD)
		#e(0)	
		try:
			p = await asyncio.create_subprocess_shell(loadCMD,stdin=PIPE, stdout=PIPE, stderr=STDOUT)
			if payload>max_chunk:
				for j in range(int(payload/max_chunk)):	
					print(i, 'max chunk #%d' % j, max_chunk, 'buffer: %s' % p.stdin._transport.get_write_buffer_size(), end='')
					p.stdin.write(fh.read(max_chunk))	
					print ('-> %s' % p.stdin._transport.get_write_buffer_size())	
					while p.stdin._transport.get_write_buffer_size()>min_write_buffer_size:
						#print('%d:%d: sleep 0.2' % (i,j))
						await asyncio.sleep(1)
					#print (p.stdin.get_write_buffer_size())
					#p.stdin.flush()
					#pprint(dir(p.stdin._transport))
					#print ('%d. after:\t' % j,p.stdin._transport.get_write_buffer_size(), p.stdin._transport.get_write_buffer_limits())
					#e(0)
				print('tail',payload%max_chunk)
				await asyncio.sleep(10)
				p.stdin.write(fh.read(payload%max_chunk))
			else:
				print('small payload',payload)
				p.stdin.write(fh.read(payload))
				await asyncio.sleep(10)
				
			
			p.stdin.close()
			fh.close()
			
		except:
			
			print ('ERROR:',traceback.format_exc())
		print ('after', ctlfile)
		return (await p.communicate())[0].splitlines()
		
		if 0:
			p = Popen(loadCMD, stdin=PIPE, stdout=PIPE, stderr=STDOUT, shell=False, env=os.environ)
			#print(pwd.encode())
			#e()
			output, err =  p.communicate()	
			logical_cnt=-1

			status=p.wait()
			d['status']=status		
			if output:
				out= [x for x in output.splitlines()]
				last_line=out[-1]
				
				#'OUT: Load completed - logical record count 487301.'
				print(last_line)
				logical_cnt=last_line.split(b' ')[-1].strip(b'.')
				d['logical_count']=logical_cnt
				log.info(last_line, extra=d)
			if err:	
				pprint( ['ERROR: %s' % x for x in err.splitlines()])		
			#print logfn
			#print status
			job_status['sqlloader'].append({'logical_count':logical_cnt,'info':[userid, data_file, ctlfile, '%s.log' % logfn, status, err, output]})

			log.info('Load complete', extra=d)
			assert status in [0,2], 'Load faled.'
		 
async def get_lines(shell_command):
	p = await asyncio.create_subprocess_shell(shell_command,
			stdin=PIPE, stdout=PIPE, stderr=STDOUT)
	return (await p.communicate())[0].splitlines()

async def main():
	# get commands output concurrently
	coros=[]
	for i,t in enumerate(config.tconf['data_files']):
		print (i, t)
		coros.append(load_data(i,t))
	if 0:
		#coros = [get_lines('"{e}" -c "print({i:d}); import time; time.sleep(1)"'.format(i=i, e=sys.executable)) for i in reversed(range(5))]
		coros = [get_lines('"{e}" -c "print({i:d}); import time; time.sleep(1)"'.format(i=i, e=sys.executable)) for i in reversed(range(5))]
	for f in asyncio.as_completed(coros): # print in the order they finish
		pprint (await f)
	
def ask_exit(signame):
    print("got signal %s: exit" % signame)
    loop.stop()	

if 1:
	parser = OptionParser()
	parser.add_option("-c", "--load_config", dest="load_config", type=str, default='tconfig.py')
				  
	opt= parser.parse_args()[0]
	config_file = os.path.join(config_home,opt.load_config)
	assert os.path.isfile(config_file), '<load_config>.py does not exists.'
		
	builtins.log=log
	builtins.d=d
	config=import_module(config_file)
	job_status['sqlloader']=[]
	#config.cleanup()
	#e(0)
	dbs_key=config.dbs_key
	builtins.config = config
	builtins.opt = opt
	builtins.parent = __file__
	builtins.job_status=job_status
	from include.transfer_utils import is_localhost, save_status
if __name__ == "__main__":	
	#asyncio.AbstractEventLoop.set_debug(enabled=True)
	if sys.platform.startswith('win'):
		loop = asyncio.ProactorEventLoop() # for subprocess' pipes on Windows
		asyncio.set_event_loop(loop)
	else:
		loop = asyncio.get_event_loop()
	for signame in ('SIGINT', 'SIGTERM'):
		loop.add_signal_handler(getattr(signal, signame), functools.partial(ask_exit, signame))		
	try:
		if 0:
			tasks = asyncio.gather(*[load_data(i,t) for i,t in enumerate(config.tconf['data_files'])])
			loop.run_until_complete(tasks)
		loop.run_until_complete(main())
		loop.close()
		#print (tasks) 
	except:
		#print (tasks)
		print (traceback.format_exc())
