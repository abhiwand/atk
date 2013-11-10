from progress import Progress
from IPython.display import display
from threading import Thread
import time
import json
from os import listdir
from os.path import isfile, join

def test(bar, wait):
	for i in range(1,101):
	    time.sleep(wait)
	    bar.update(i)

def parse_status(data):
	dict = json.loads(data)
	return dict.setdefault('name', 'unknown'), int(dict.setdefault('progress', '0'))

def get_status_files(path):
	return [join(path,f) for f in listdir(path) if isfile(join(path,f)) and f.endswith(".status")]

def get_statuses(path = '.'):
	return [parse_status(open(f).read()) for f in get_status_files(path)]


_progress = {}
_watchers = {}

def show_upload_progress(path):
	exists = set()
	for (name, pct) in get_statuses(path):
		exists.add(name)
		if (not _progress.has_key(name)):
			p = Progress("Loading " + name + " into cluster")
			display(p)
			_progress[name] = p
		p = _progress[name]
		p.update(pct)
	for name in _progress.keys():
		if name not in exists:
			p = _progress[name]
			p.delete()
			del _progress[name]


def watcher(path, flag):
	while flag[0]:
		show_upload_progress(path)
		time.sleep(1)

def watch_changes(path = '.'):
	if _watchers.has_key(path):
		(t,flag) = _watchers[path]
		flag[0] = False
		t.join()
		_watchers.remove(path)
	flag = [True]
	t = Thread(target = watcher, args = (path,flag))
	t.start()
	_watchers[path] = (t, flag)




