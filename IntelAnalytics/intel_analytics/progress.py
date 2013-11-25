import uuid
import time
from IPython.display import HTML, Javascript, display
from threading import Thread

class Progress:
	def __init__(self, name, max_value = 100):
		self.name = name
		self.id = str(uuid.uuid4())
		self.max_value = max_value
		self.value = 0

	def _repr_html_(self):
		pb = HTML(
		"""
		<div class='status'>
			<span class="label span3">%s</span>
			<span class="span5 progress progress-striped">
		  		<div id="%s" class="bar" style= "width:0%%">&nbsp;</div>
			</span> 
		</div>
		""" % (self.name, self.id))
		display(pb)
		return pb


	def update(self, value):
		self.value = value;
		display(Javascript("$('div#%s').width('%i%%')" % 
					(self.id, (float(value) / float(self.max_value)) * 100)))

	def delete(self):
		display(Javascript("$('div#%s').parents('.status').remove()" %self.id))

	

