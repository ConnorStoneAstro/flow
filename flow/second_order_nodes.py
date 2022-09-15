"""author: Connor Stone

summary:
  Nodes which encapsulate or act on other nodes or full flowcharts.

description:
  A "Chart" Node is a container for a whole flowchart (in this way
  flowcharts can be nested) and is the primary interface for building
  flowcharts. A "Pipe" Node is a wrapper for a flowchart (or node)
  which can iterate on the state and apply the flowchart (or node) in
  parallel, itteratively, or simply pass the state along. Similarly,
  other nodes in this module ought to have behaviour which acts on
  nodes or flowcharts.
"""

import pygraphviz as pgv
from multiprocessing import Pool
from copy import deepcopy
from .core import Node
from .first_order_nodes import Start, End
from .flow_exceptions import FlowExitChart, FlowExit
from datetime import datetime
from time import time
import traceback
import logging

class Chart(Node):
    """Main container for a flowchart.

    Stores all the nodes and links between them composing a
    flowchart. The run method for this object will iteratively apply
    each node in the flowchart and progress through the path from
    start to end. This is the main object that users should interact
    with when constructing a flowchart. Includes methods to add/link
    nodes, draw the flowchart, save/load the flowchart, and visualize
    it. This class inherits from the Node object and so can itself
    be a node in another larger flowchart. In this case it will be
    represented visually as a hexagon.

    Arguments
    -----------------
    structure: dict or list
      A dictonary that gives the structure of the flowchart. The keys
      in the dictionary are the name strings of nodes, the values can
      be either name strings or lists of name strings. The key will be
      linked forward to the value(s).

    node_kwargs: dict
      A dictionary of arguments to provide to nodes on creation.  For
      custom named nodes (e.g. sub-charts) will also provide the class
      name for the node.
    
    logfile: string
      The name of a log file to track the path through the flowchart.

    safe_mode: bool
      If safe_mode = True then errors will be caught and the Chart will
      simply proceed to the next node.
    """

    def __init__(self, structure, node_kwargs = {}, logfile=None, safe_mode=False, **kwargs):
        super().__init__(**kwargs)
        if isinstance(logfile, str):
            logging.basicConfig(
                filename=logfile, filemode="w", level=logging.INFO
            )
        self.nodes = {}
        self.state = None
        self.safe_mode = safe_mode
        self.structure_dict = {}
        self.current_node = "Start"
        self.build_chart(structure, node_kwargs)
        self.path = []
        self.benchmarks = []
        self.visual_kwargs['shape'] = "hexagon"

    def action(self, state):

        self.state = state
        self.path = []
        self.benchmarks = []
        
        for node in self:
            logging.info(f"{self.name}: {node.name} ({datetime.now()})")
            self.path.append(node.name)
            start = time()
            try:
                self.state = node(self.state)
            except FlowExitChart as e:
                if not "End" in self.structure_dict[node.name]:
                    self.link_nodes(node.name, "End")
                logging.info(f"{self.name}: {node.name} Ended Chart ({datetime.now()})")
                break
            except FlowExit as e:
                if not "End" in self.structure_dict[node.name]:
                    self.link_nodes(node.name, "End")
                if hasattr(node, "state") and node.state is not None:
                    self.state = node.state
                if self.owner is None:
                    logging.info(f"{self.name}: {node.name} Ended Flow ({datetime.now()})")
                    break
                else:
                    raise e
            except Exception as e:
                if not self.safe_mode:
                    logging.error(f"on step '{self.current_node}' got error: {str(e)}")
                    logging.error("with full trace: %s" % traceback.format_exc())
                    raise e
            finally:
                self.benchmarks.append(time() - start)
                    
        return self.state

    def add_node(self, node):
        """Add a new Node to the flowchart. This merely makes the flowchart
        aware of the Node, it will need to be linked in order to
        take part in the calculation.

        Arguments
        -----------------
        node: Node
          A Node object to be added to the flowchart.
        """
        if node.name in self.structure_dict:
            if self.safe_mode: return
            raise Flow_LinkError(f"{node.name} already in {self.name}")
        self.nodes[node.name] = node
        node.set_owner(self)
        self.structure_dict[node.name] = []

    def link_nodes(self, node1, node2):
        """Link two nodes in the flowchart. node1 will be linked forward to node2.

        Arguments
        -----------------
        node1: string
          A Node name in the flowchart which will be linked
          forward to node2.

        node2: string
          A Node name in the flowchart which will have node1 linked to it.
        """
        if self.nodes[node2].name in self.structure_dict[self.nodes[node1].name]:
            if self.safe_mode: return
            raise Flow_LinkError(f"{self.nodes[node2].name} already linked to {self.nodes[node1].name}")
        self.nodes[node1].link_forward(self.nodes[node2])
        self.structure_dict[node1].append(node2)

    def unlink_nodes(self, node1, node2):
        """Undo the operations of "link_nodes" and return to previous state.

        Arguments
        -----------------
        node1: string
          A Node name in the flowchart which was linked forward to node2.

        node2: string
          A Node name in the flowchart which did have node1 linked to it.
        """
        self.nodes[node1].unlink_forward(self.nodes[node2])
        self.structure_dict[node1].pop(self.structure_dict[node1].index(node2))

    def insert_node(self, node1, node2):
        """Insert node1 in the place of node2, and link to node2

        Arguments
        -----------------
        node1: string
          A Node name in the flowchart which will take the place of node2.

        node2: string
          A Node name in the flowchart which will now come after node1.
        """

        for reverse_node in list(self.nodes[node2].reverse):
            self.unlink_nodes(reverse_node.name, node2)
            self.link_nodes(reverse_node.name, node1)
        self.link_nodes(node1, node2)

    def build_chart(self, structure, node_kwargs = {}):
        """Compact way to build a chart.

        Through this function a user may supply all necessary
        information to construct a flowchart. A list of nodes can be
        added to the chart instead of adding them one at a time. Also
        a structure dictionary can be provided which gives all of the
        linkages between nodes. Essentially this function just
        condenses a number of "add_node" and "link_nodes" calls into a
        single operation. This function may be called multiple times,
        each call will add to the previous, not replace it.

        Arguments
        -----------------
        structure: dict or list
          A dictonary that gives the structure of the flowchart. The
          keys in the dictionary are the name strings of nodes, the
          values can be either name strings or lists of name
          strings. The key will be linked forward to the value(s).

        node_kwargs: dict
          A dictionary of arguments to provide to nodes on creation.
          For custom named nodes (e.g. sub-charts) will also provide
          the class name for the node.
        """
        AllNodes = Node.all_subclasses() #get_subclasses(Node)
        for inode, node in enumerate(structure):
            if ":" in node:
                node_name, node_type = node.split(":")
                node = node_name
                if not node in node_kwargs:
                    node_kwargs[node] = {}
                node_kwargs[node]["node_class"] = node_type
                structure[inode] = node
            if node in AllNodes:
                self.add_node(AllNodes[node](**node_kwargs.get(node,{})))
            elif isinstance(node, tuple) and node[0] in AllNodes:
                self.add_node(AllNodes[node[0]](**node_kwargs.get(node[0],{})))
            else:
                if "name" not in node_kwargs[node]:
                    node_kwargs[node]["name"] = node
                if isinstance(node_kwargs[node]["node_class"], str):
                    self.add_node(AllNodes[node_kwargs[node]["node_class"]](**node_kwargs.get(node,{})))
                else:
                    self.add_node(node_kwargs[node]["node_class"](**node_kwargs.get(node,{})))
                
        if "Start" not in self.structure_dict:
            self.add_node(Start(**node_kwargs.get("Start", {})))
        if "End" not in self.structure_dict:
            self.add_node(End(**node_kwargs.get("End", {})))
                
        if isinstance(structure, list):
            for n in range(len(structure)):
                if isinstance(structure[n],tuple):
                    for node2 in structure[n][1]:
                        self.link_nodes(structure[n][0], node2)
                elif n < len(structure)-1 and isinstance(structure[n], str) and isinstance(structure[n+1],str):
                    self.link_nodes(structure[n], structure[n+1])
                elif n < len(structure)-1 and isinstance(structure[n], str) and isinstance(structure[n+1],tuple):
                    self.link_nodes(structure[n], structure[n+1][0])
            if len(self.nodes["Start"].forward) == 0:
                if isinstance(structure[0],str):
                    self.link_nodes("Start", structure[0])
                else:
                    self.link_nodes("Start", structure[0][0])
            if len(self.nodes["End"].reverse) == 0:
                if isinstance(structure[-1],str):
                    self.link_nodes(structure[-1], "End")
                else:
                    self.link_nodes(structure[-1][0], "End")
        else:
            for node1 in structure:
                if isinstance(structure[node1], str):
                    self.link_nodes(node1, structure[node1])
                else:
                    for node2 in structure[node1]:
                        self.link_nodes(node1, node2)

    def draw(self, filename):
        """Visual representation of the flowchart.

        Creates a visual flowchart using pygraphviz. Every node will
        be drawn, including those that don't have links to other
        nodes, make sure to fully input the desired structure before
        running this method.

        Arguments
        -----------------
        filename: string
          path to save final graphical representation. Should end in
          .png, .jpg, etc.
        """
        visual, nodes = self._construct_chart_visual()
        visual.layout()
        visual.draw(filename)

    def _construct_chart_visual(self, visual = None):
        if not visual:
            visual = pgv.AGraph(strict=True, directed=True, splines="line", overlap=False)
        nodes = []
        for node in self.nodes.values():
            if isinstance(node, Chart):
                visual, subgraph = node._construct_chart_visual(visual)
                visual.subgraph(subgraph, name = node.name, label = node.name, style = 'dotted')
            else:
                visual.add_node(
                    node.name, **node.visual_kwargs
                )
                nodes.append(node.name)
        for node1, links in self.structure_dict.items():
            for node2 in links:
                visual.add_edge(node1, node2)
        return visual, nodes

    def __str__(self):
        visual, nodes = self._construct_chart_visual()
        return visual.string()

    def __iter__(self):
        self.current_node = "Start"
        return self

    def __next__(self):
        # try:
        self.current_node = self.nodes[self.current_node].next().name
        # except AttributeError:
        #     next_node = next(self.nodes[self.current_node])
        #     assert self.current_node in self.nodes
        #     self.link_nodes(self.current_node, next_node)
        #     self.current_node = next_node
        return self.nodes[self.current_node]


class Pipe(Node):
    """Basic object for running flowcharts on states.

    A pipe is initialized with a Chart object (or Node) and can then
    be called on a state, the pipe will make a copy of the flowchart
    to run on the state and will apply that copy. This way each state
    is processed with a fresh version of the class (otherwise some
    class variables could be altered). This is most important when
    running processes in parallel. There are three processing modes
    for a Pipe: parallelize, iterate, and pass.  The parallelize mode
    will apply the flowchart on each element of the state in parallel
    up to the specified number of cores. The iterate mode will do the
    same but in serial instead of parallel. The pass mode will simply
    pass on the state to the flowchart without any iteration. The
    reason for the three modes is to allow a single Pipe object to
    play many roles in an analysis task. One may wish to nest analysis
    tasks, in which case only the top level Pipe object should run in
    parallel, but later may wish to run an inner task
    independently. Finally the user may wish to streamline the final
    result and ignore the parallelization all together. Instead of
    creating new Pipes for each case, a single pipe will suffice with
    a changing process_mode value.

    Arguments
    -----------------
    name: string
      name of the node, should be unique in the flowchart. This is how
      other nodes (i.e. decision nodes) will identify the node.

    flowchart: Chart
      Instance of a Chart object which is to be called on a number of
      states.

    safe_mode: bool
      indicate how to handle errors. In safe mode, any error raised by
      an individual run will simply return None. However, the path and
      benchmarks for the chart will still be saved thus allowing one
      to diagnose where the error occured. When safe mode is off,
      errors will be raised out of the Pipe object.

    process_mode: string
      There are three processing modes for a Pipe: parallelize,
      iterate, and pass.  The parallelize mode will apply the
      flowchart on each element of the state in parallel up to the
      specified number of cores. The iterate mode will do the same but
      in serial instead of parallel. The pass mode will simply pass on
      the state to the flowchart without any iteration.

    cores: int
      number of processes to generate in parallelize mode.

    """

    def __init__(
            self, flowchart, safe_mode=True, process_mode="parallelize", cores=4, return_success = False, **kwargs
    ):

        super().__init__(**kwargs)
        self.update_flowchart(flowchart)
        self.safe_mode = safe_mode
        self.process_mode = process_mode
        self.cores = cores
        self.visual_kwargs['shape'] = "parallelogram"

    def update_flowchart(self, flowchart):

        self.flowchart = flowchart
        self.benchmarks = []
        self.paths = []

    def apply_chart(self, state):

        chart = deepcopy(self.flowchart)
        logging.info(f"PIPE:{self.name}({self.process_mode}): {chart.name} ({datetime.now()})")
        if self.safe_mode:
            try:
                res = chart(state)
            except Exception as e:
                logging.error(f"on step '{chart.current_node}' got error: {str(e)}")
                logging.error("with full trace: %s" % traceback.format_exc())
                res = None
        else:
            res = chart(state)
            
        if isinstance(chart, Chart):
            timing = chart.benchmarks
            path = chart.path
        else:
            timing = [chart.benchmark]
            path = [chart.name]
        if self.return_success:
            return (res is not None), timing, path
        else:
            return res, timing, path

    def _run(self, state):
        if self.process_mode == "parallelize":
            starttime = time()
            with Pool(self.cores) as pool:
                result = pool.map(self.apply_chart, state)
                for r in result:
                    self.benchmarks.append(r[1])
                    self.paths.append(r[2])
                logging.info(f"PIPE:Finished parallelize run in {time() - starttime} sec")
                return list(r[0] for r in result)
        elif self.process_mode == "iterate":
            result = map(self.apply_chart, state)
            ret = []
            for r in result:
                self.benchmarks.append(r[1])
                self.paths.append(r[2])
                ret.append(r[0])
            return ret
        elif self.process_mode == "pass":
            result = self.apply_chart(state)
            self.benchmarks.append(result[1])
            self.paths.append(result[2])
            return result[0]
        raise ValueError(
            "Unrecognized process_mode: '{self.process_mode}',"
            " should be one of: parallelize, iterate, pass."
        )
