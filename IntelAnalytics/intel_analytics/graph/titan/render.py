import uuid
import time
import json
from IPython.display import HTML, Javascript, display
from threading import Thread
from itertools import islice


__all__ = ['render_radial', 'vertex_to_json', 'edge_to_json']

def vertex_to_json(vertex, depth):
    """Convert a bulbs.Vertex into JSON that can be consumed by the JavaScript InfoVis Toolkit
       
       Generates a node for each vertex that is reachable within *depth* steps"""
    todo = [(vertex, depth)]
    found = set()
    done = [] # no point in doing this as a generator, since json.dumps can't handle them
    while todo:
	(v,d) = todo.pop(0)
	if v.eid in found:
	    continue
        found.add(v.eid)
	node = {'id': v.eid, 'name':v.data()['name'], 'data': v.data(), 'adjacencies':[] }
	if d > 0:
	    edges = v.outE() or []
	    node['adjacencies'][:] = map(edge_to_json, edges)
	    todo.extend([(next, depth - 1) for next in (v.outV() or [])])
        done.append(node)
    return done

def edge_to_json(edge):
    """Convert a bulbs.Edge into JSON that can be consumed by the JavaScript InfoVis Toolkit"""
    return {'nodeTo':edge.inV().eid, 'data': {'label':edge.label()}} 

def render_radial(vertex, depth):
    """Renders the given bulbs.Vertex to an IPython notebook"""
    height = min(1000, 200 + (depth * 100))
    return _render_json(vertex_to_json(vertex, depth),height)

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
  //and that as of today iPhone/iPad current text support is lame
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
            color: '#ddeeff'
        },
        
        Edge: {
          color: '#C17878',
          lineWidth:1.5
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
        //Change some label dom properties.
        //This method is called each time a label is plotted.
        onPlaceLabel: function(domElement, node){
            var style = domElement.style;
            style.display = '';
            style.cursor = 'pointer';

            if (node._depth <= 1) {
                style.fontSize = "0.8em";
                style.color = "#222";
            
            } else if(node._depth == 2){
                style.fontSize = "0.7em";
                style.color = "#494949";
            
            } else {
                style.display = 'none';
            }

            var left = parseInt(style.left);
            var w = domElement.offsetWidth;
            style.left = (left - w / 2) + 'px';
        }
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
init()
})();
		""" % (id,json.dumps(nodes),id)))

