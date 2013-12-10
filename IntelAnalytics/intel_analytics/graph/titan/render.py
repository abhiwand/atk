##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
import uuid
import time
import json
from IPython.display import HTML, Javascript, display
from threading import Thread
from itertools import islice


__all__ = ['render_radial', 'vertex_to_json', 'edge_to_json', 'traverse', 'set_data', 'get_data', 'set_name',
            'get_name', 'set_format']

def set_data(js, **kwargs):
  """Updates the ['data'] dictionary on the JSON object."""
  js['data'].update(kwargs)

def set_name(js, name):
  """Sets the ['name'] property on the JSON object."""
  js['name'] = name
    
def get_name(js):
  """Gets the ['name'] property on the JSON object."""
  return js['name']
    
def get_data(js, name):
  """Gets the named value from the ['data'] dictionary on the JSON object."""
  return js['data'].get(name)

def set_format(js, **kwargs):
  """Sets the format in the ['data'] dictionary. Keyword arguments will appear
     in the data dictionary with '$' prepended."""
  for (k,v) in kwargs.items():
    js['data']['$' + k] = v
        

def _true(x):
    return True

def traverse(vertex, depth, vertex_filter = _true, edge_filter = _true):
    """This method traverses the out edges from the given vertex to the specified depth.
       Returns a generator that yields (vertex,edgelist) pairs.
       
       Generates a node for each vertex that is reachable within the *depth* steps.

       Required arguments:
       vertex        -- The vertex from which to start the traversal.
       depth         -- The number of 'hops', include the starting vertex in the count.
       vertex_filter -- The filter function - if you use it, this function will be used to filter
                        out vertices for which the function returns False.
       edge_filter   -- Filter out edges (and the vertices reachable from them) for which
			the function returns False.
    """
    todo = [(vertex, depth)]
    found = set()
    while todo:
	(v,d) = todo.pop(0)
	if v.eid in found or not vertex_filter(v):
	    continue
        found.add(v.eid)
	edges = [e for e in v.outE() or [] 
			if d > 0 and edge_filter(e) and vertex_filter(e.inV())]
	next = [(e.inV(), d - 1) for e in edges]
	todo.extend(next)
	yield (v,edges)

def edge_to_json(edge):
    """Converts a bulbs.Edge into JSON that can be consumed by the JavaScript InfoVis Toolkit."""
    return {'nodeTo':edge.inV().eid, 'data': {'label':edge.label()}} 



def vertex_to_json(vertex, edges, vertex_label = 'name', edge_formatter = edge_to_json):
    """Converts a bulbs.Vertex into JSON that can be consumed by the JavaScript InfoVis Toolkit.
       
       Generates a node with adjacencies for the given vertex.
    """
    node = {'id': vertex.eid, 
		'name':vertex.data().get(vertex_label) or str(vertex.eid), 
		'data': vertex.data(), 
		'adjacencies':map(edge_formatter, edges) }
    return node

def _traversal_to_json(vertex_edge_list,
			vertex_label = 'name', 
			vertex_formatter = vertex_to_json, 
			edge_formatter = edge_to_json):
    json = [vertex_formatter(v, es, 
			vertex_label = vertex_label, 
			edge_formatter = edge_formatter) 
		for (v,es) in vertex_edge_list]
    return json
 
def render_radial(vertex, depth = 1, 
			vertex_label = 'name', 
			vertex_filter = None, 
			edge_filter = None,
			vertex_formatter = vertex_to_json,
			edge_formatter = edge_to_json):
    """Renders the given bulbs.Vertex to an IPython notebook.
       
       Traverses the outer edges from the given vertex until the specified depth.
       
       Renders a node for each vertex that is reachable within the *depth* steps.

       Required arguments:
       vertex        -- The vertex from which to start the traversal.
       depth         -- The number of 'hops', include the starting vertex in the count.

       Keyword arguments:
       vertex_label     -- Which field from the vertex data() dictionary should be used for the vertex label.
       vertex_filter    -- The filter function - if you provide it, this function will be used to filter
                           out vertices for which the function returns False.
       edge_filter      -- Filters out edges (and the vertices reachable from them) for which
		   	   the function returns False.
       vertex_formatter -- Generates the JSON for each node. Defaults to vertex_to_json.
       edge_formatter   -- Generates the JSON for each edge. Defaults to edge_to_json.
       
    """
    height = min(1000, 200 + (depth * 100))
    edge_filter = edge_filter or _true
    vertex_filter = vertex_filter or _true
    nodes = traverse(vertex, depth, edge_filter = edge_filter, vertex_filter = vertex_filter)
    json = _traversal_to_json(nodes, vertex_formatter = vertex_formatter, edge_formatter = edge_formatter)
    return _render_json(json, height)

def _render_json(nodes,height):
	id = str(uuid.uuid4())
	pb = HTML(
		"""
		<div id="%s" class="container" style="width:100%%;height:%dpx;">
		</div>
		<div id="%s-log">
		</div>
		""" % (id, height, id))
	display(pb)
	display(Javascript("""if ($('#jit').length == 0) { 
			$("head").append(
				$("<script id = 'jit' src='files/Jit/jit.js'/>"));
	}
	"""))

	# most of the following JavaScript code from the the InfoVis Toolkit demo page for radial graphs:
        # http://philogb.github.io/jit/static/v20/Jit/Examples/RGraph/example1.html
	display(Javascript("""
		var labelType, useGradients, nativeTextSupport, animate;

(function() {
  var ua = navigator.userAgent,
      iStuff = ua.match(/iPhone/i) || ua.match(/iPad/i),
      typeOfCanvas = typeof HTMLCanvasElement,
      nativeCanvasSupport = (typeOfCanvas == 'object' || typeOfCanvas == 'function'),
      textSupport = nativeCanvasSupport 
        && (typeof document.createElement('canvas').getContext('2d').fillText == 'function');
  //I'm setting this based on the fact that ExCanvas provides text support for IE
  //and that as of today iPhone/iPad current text support is not good enough.
  labelType = (!nativeCanvasSupport || (textSupport && !iStuff))? 'Native' : 'HTML';
  nativeTextSupport = labelType == 'Native';
  useGradients = nativeCanvasSupport;
  animate = !(iStuff || !nativeCanvasSupport);
})();

(function() {
var Log = {
  elem: false,
  write: function(text){
    if (!this.elem) 
      this.elem = document.getElementById('%s-log');
    this.elem.innerHTML = text;
    this.elem.style.left = (500 - this.elem.offsetWidth / 2) + 'px';
  }
};


function init(){
        //init data
    var json = %s;
    //init RGraph
    var elementName = '%s';
    var rgraph = new $jit.RGraph({
        //Where to append the visualization
        injectInto: elementName,
        //Optional: create a background canvas that plots
        //concentric circles.
        background: {
          CanvasStyles: {
            strokeStyle: '#555'
          }
        },
        //Add navigation capabilities:
        //zooming by scrolling and panning.
        Navigation: {
          enable: true,
          panning: true,
          zooming: 10
        },
        //Set Node and Edge styles.
        Node: {
            overridable: true,
            color: '#ddeeff'
        },
        
        Edge: {
          overridable: true,
          color: '#C17878',
          lineWidth:1.5
        },

        Label: {
          overridable: true
        },
        
        onBeforeCompute: function(node){
            Log.write("centering " + node.name + "...");
            //Add the relation list in the right column.
            //This list is taken from the data property of each JSON node.
            //$jit.id('inner-details').innerHTML = node.data.relation;
        },
        
        //Add the name of the node in the correponding label
        //and a click handler to move the graph.
        //This method is called once, on label creation.
        onCreateLabel: function(domElement, node){
            domElement.innerHTML = node.name;
            domElement.onclick = function(){
                rgraph.onClick(node.id, {
                    onComplete: function() {
                        Log.write("done");
                    }
                });
            };
        },
    });
    //load JSON data
    rgraph.loadJSON(json);
    //trigger small animation
    rgraph.graph.eachNode(function(n) {
      var pos = n.getPos();
      pos.setc(-200, -200);
    });
    rgraph.compute('end');
    rgraph.fx.animate({
      modes:['polar'],
      duration: 2000
    });
    //end
    //append information about the root relations in the right column
    //$jit.id('inner-details').innerHTML = rgraph.graph.getNode(rgraph.root).data.relation;
}

function run() {
  if (typeof($jit) != 'undefined') {
    init();
  } else {
    setTimeout(run, 1000);
  }
}
run();

})();
		""" % (id,json.dumps(nodes),id)))

