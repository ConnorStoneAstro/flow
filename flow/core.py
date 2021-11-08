"""author: Connor Stone

summary:
  Main functionality for generating flowchart analysis pipelines.

description:
  This module contains classes allowing the user to encapsulate code
  in a framework which fundamentally represents code as a
  flowchart. This makes for more modular code which can be tested and
  updated easily. Conceptually every element of the flowchart is a
  "Node" and these nodes are linked together. There are a number
  of types of nodes, the most basic being "start" and "end" which
  are terminators for the flowchart and do nothing themselves. A
  "process" Node is the workhorse of the flowchart and should
  enclose most of your analysis code. A "decision" Node branches the
  flowchart into different sections to handle conditional elments of
  the analysis. A "chart" Node is a container for a whole flowchart
  (in this way flowcharts can be nested) and is the primary interface
  for building flowcharts.
"""

from time import time
from inspect import signature
from copy import deepcopy
from pickle import dumps, loads
from multiprocessing import Pool
import pygraphviz as pgv


class Node:
    """Base object for all nodes in the flowchart

    Core functionality for nodes in the flowchart. Includes methods
    for linking flowchart elements, a run method which operates on the
    state as it passes through the flowchart, and the specifications
    for the nodes visual appearance. A given node can be linked to any
    other node, when doing so it will store a pointer to that
    object. Nodes also retain a list of nodes which point to them,
    though this has no defaul functional purpose and is there only to
    support inheriting node functionality.

    Arguments
    -----------------
    name: string
      name of the node, should be unique in the flowchart. This is how
      other nodes (i.e. decision nodes) will identify the node.

    func: function
      function object of the form: func(state) returns state. This can
      be given on initialization to set the behavior of the node in
      the flowchart. This function should operate on the state and
      return the new updated state object.

      :default:
        None
    """

    def __init__(self, name, func=None):
        self.name = name
        self.forward = None
        self.reverse = set()
        self.status = "initialized"
        self.benchmark = -1
        self.colour = "black"
        self.shape = "plain"
        self.style = "solid"
        if not func is None:
            self.update_run(func)

    def update_run(self, func):
        """Method to update the function applied by the current Node. Do
        not directly alter the internal _run variable.

        Arguments
        -----------------
        func: function
          Function which takes the state as an argument and returns
          the updated state. Can also take the "self" object for the
          current Node to allow access to state variables of the
          Node.

        """
        self._run = func

    def link_forward(self, node):
        """Link to the next Node in the flow chart. Stores a reference to
        the next Node object, also calls the "link_reverse" function
        of the forward Node so that it knows what has linked to
        it. This is the primary function for building the
        flowchart. Though it should mostly be used internally by the
        chart object, users may wish to make use of it.

        Arguments
        -----------------
        node: Node
          A Node object to be linked as the next step in the
          flowchart.

        """
        self.forward = node
        node.link_reverse(self)

    def unlink_forward(self, node):
        """Undo the operations of the "link_forward" method. Returns the
        Node to its initial state where it is no longer linked
        forward to any object.

        Arguments
        -----------------
        node: Node
          A Node object to be unlinked from this step in the
          flowchart.

        """
        self.forward = None
        node.unlink_reverse(self)

    def link_reverse(self, node):
        """Store a reference to a Node which has linked to this
        Node. This function should only be used internally as it is
        expected that the "link_forward" method will call this
        method. Anyone constructing their own flowchart step type
        which inherits from Node should keep this behaviour in mind.

        Arguments
        -----------------
        node: Node
          A Node object which was linked to this step in the
          flowchart.

        """
        self.reverse.update([node])

    def unlink_reverse(self, node):
        """Undo the operations of the "link_reverse" method. Returns the
        Node to its initial state where it is no longer linked to
        any object.

        Arguments
        -----------------
        node: Node
          A Node object to be unlinked from this step in the
          flowchart.

        """
        self.reverse.remove(node)

    def _run(self, state):
        """Placeholder function for the behaviour of a Node. Simply
        passes the state on unaltered.

        Arguments
        -----------------
        state: object
          container for all information related to a flowchart
          analysis task.

        """
        return state

    def __call__(self, state):
        self.status = "running"
        start = time()
        sig = signature(self._run)
        if "self" in sig.parameters:
            res = self._run(self, state)
        else:
            res = self._run(state)
        self.benchmark = time() - start
        self.status = "complete"
        return res

    def __getitem__(self, key):
        if key == "next" and not self.forward is None:
            return self.forward
        else:
            raise StopIteration()

    def __str__(self):
        return self.name


class Start(Node):
    """Initialization point of a flowchart.

    This Node is the starting point of a flowchart. It has no
    functionality except to pass the state along to the next Node in
    the flowchart. Note that this class has no ability to link
    backwards as it is by requirement the first node. It's
    initialization takes no arguments.
    """

    def __init__(self):
        super().__init__("start")
        self.colour = "blue"
        self.shape = "box"
        self.style = "rounded"

    def link_reverse(self, node):
        """Override the default "link_reverse" method behavior. It now raises
        an error as nothing should link to start.

        """
        raise AttributeError("'start' object has no method 'link_reverse'")

    def unlink_reverse(self, node):
        """Override the default "unlink_reverse" method behaviour. It now
        raises an error as nothing should link to start.

        """
        raise AttributeError("'start' object has no method 'unlink_reverse'")


class End(Node):
    """Stopping point of a flowchart.

    This Node represents an endpoint of a flowchart. Whenever a
    process reaches this note it will end computation and return the
    state in it's current form. Note that this class has no ability to
    link forward as it is by requirement the last node in the
    flowchart. It's initialization takes no arguments.
    """

    def __init__(self):
        super().__init__("end")
        self.colour = "red"
        self.shape = "box"
        self.style = "rounded"

    def link_forward(self, node):
        """Override the default "link_forward" method behavior. It now raises
        an error as end should not link to anything.

        """
        raise AttributeError("'end' object has no method 'link_forward'")

    def unlink_forward(self, node):
        """Override the default "unlink_forward" method behavior. It now
        raises an error as end should not link to anything.

        """
        raise AttributeError("'end' object has no method 'unlink_forward'")

    def __getitem__(self, key):
        raise StopIteration()


class Process(Node):
    """Basic node for acting on the state.

    This node retains all default functionality of a Node. This is
    the workhorse of a flowchart that operates on a given state and
    returns the updated version. The visual appearance of this node is
    a box.

    Arguments
    -----------------
    name: string
      name of the node, should be unique in the flowchart. This is how
      other nodes (i.e. decision nodes) will identify the node.

    func: function
      function object of the form: func(state) returns state. This can
      be given on initialization to set the behavior of the node in
      the flowchart. This function should operate on the state and
      return the new updated state object.

      :default:
        None
    """

    def __init__(self, name, func=None):
        super().__init__(name, func)
        self.shape = "box"


class Decision(Node):
    """Node for switching flowchart path based on the state.

    Instead of acting on a state, this node's purpose is to determine
    which path to take through a flowchart. It retains a dictionary of
    options which it is allowed to point to, the output of this node
    is a choice of which path to take. This can be used to alter the
    behaviour of the flowchart on different types of states, or to
    create loops which iteratively perform some analysis. Visually
    represented as a diamond shape.

    Arguments
    -----------------
    name: string
      name of the node, should be unique in the flowchart. This is how
      other nodes (i.e. decision nodes) will identify the node.

    func: function
      function object of the form: func(state) returns state. This can
      be given on initialization to set the behavior of the node in
      the flowchart. This function should operate on the state and
      return the new updated state object.

      :default:
        None
    """

    def __init__(self, name, func=None):
        super().__init__(name, func)
        self.shape = "diamond"
        self.options = {}

    def link_forward(self, node):
        """Update the behaviour of the "link_forward" method. A decision
        object can be linked to many forward steps. The new behaviour
        stores these possibilities in an "options" dictionary. The
        first Node which is linked will be set as the forward object
        until a decision is made at runtime.

        Arguments
        -----------------
        node: Node
          A Node object to be linked as a possible next step in the
          flowchart.
        """
        if self.forward is None:
            self.forward = node
        self.options[node.name] = node
        node.link_reverse(self)

    def unlink_forward(self, node):
        """Update the behaviour of the "unlink_forward" method. A decision
        object can be linked to many forward steps. The new behaviour
        removes the given state from the options dictionary. If
        necessary, a new object will be set as the forward object
        until a decision is made at runtime.

        Arguments
        -----------------
        node: Node
          A Node object to be unlinked as a next step in the
          flowchart.
        """
        del self.options[node.name]
        if len(self.options) > 0:
            self.forward = self.options[self.options.keys()[0]]
        else:
            self.forward = None
        node.unlink_reverse(self)

    def __call__(self, state):
        self.status = "running"
        start = time()
        sig = signature(self._run)
        if "self" in sig.parameters:
            res = self._run(self, state)
        else:
            res = self._run(state)

        assert res in self.options, (
            "node must be linked before it can be selected. Chosen %s not in options: %s"
            % (res, str(self.options.keys()))
        )

        self.forward = self.options[res]
        self.benchmark = time() - start
        self.status = "complete"
        return state


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
    name: string
      name of the node, should be unique in the flowchart. This is how
      other nodes (i.e. decision nodes) will identify the node.

    filename: string
      path to a file containing a saved flowchart. This will be loaded
      and used to initialize the current flowchart. Note that the user
      can still set the name of the flowchart to whatever they like.

      :default:
        None
    """

    def __init__(self, name=None, filename=None):
        if not filename is None:
            res = self.load(filename)
            super().__init__(res["name"])
            self.__dict__.update(res)
            return

        super().__init__(name)
        self.nodes = {}
        self.structure_dict = {}
        self._linear_mode = False
        self._linear_mode_link = "start"
        self.add_node(Start())
        self.add_node(End())
        self.path = []
        self.benchmark = []
        self.istidy = False
        self.shape = "hexagon"

    def linear_mode(self, mode):
        """Activate a mode where new nodes are automatically added to the end
        of the flowchart. This way a simple chart without a complex
        decision structure can be constructed with minimal redundant
        linking.

        Arguments
        -----------------
        mode: bool
          If True, linear mode will be turned on. If False, it will be
          turned off

        """
        if mode and not self._linear_mode:
            self._linear_mode = True
            while not self.nodes[self._linear_mode_link].forward is None:
                prev = self._linear_mode_link
                self._linear_mode_link = self.nodes[self._linear_mode_link].forward.name
            if self._linear_mode_link == "end":
                self._linear_mode_link = prev
            else:
                self.link_nodes(self._linear_mode_link, "end")
        elif not mode and self._linear_mode:
            self._linear_mode = False

    def add_node(self, node):
        """Add a new Node to the flowchart. This merely makes the flowchart
        aware of the Node, it will need to be linked in order to
        take part in the calculation (unless linear mode is on).

        Arguments
        -----------------
        node: Node
          A Node object to be added to the flowchart.
        """
        self.nodes[node.name] = node
        self.structure_dict[node.name] = []
        self.istidy = False
        if self._linear_mode:
            self.unlink_nodes(self._linear_mode_link, "end")
            self.link_nodes(self._linear_mode_link, node.name)
            self.link_nodes(node.name, "end")
            self._linear_mode_link = node.name

    def add_process_node(self, name, func=None):
        """Utility wrapper to first create a process object then add it to
        the flowchart with the "add_node" method.

        Arguments
        -----------------
        name: string
          name of the node, should be unique in the flowchart. This is how
          other nodes (i.e. decision nodes) will identify the node.

        func: function
          function object of the form: func(state) returns state. This can
          be given on initialization to set the behavior of the node in
          the flowchart. This function should operate on the state and
          return the new updated state object.

        :default:
          None
        """
        newprocess = Process(name, func)
        self.add_node(newprocess)

    def add_decision_node(self, name, func=None):
        """Utility wrapper to first create a decision object then add it to
        the flowchart with the "add_node" method.

        Arguments
        -----------------
        name: string
          name of the node, should be unique in the flowchart. This is how
          other nodes (i.e. decision nodes) will identify the node.

        func: function
          function object of the form: func(state) returns state. This can
          be given on initialization to set the behavior of the node in
          the flowchart. This function should operate on the state and
          return the new updated state object.

        :default:
          None
        """
        newdecision = Decision(name, func)
        self.add_node(newdecision)

    def link_nodes(self, node1, node2):
        """Link two nodes in the flowchart. node1 will be linked forward to node2.

        Arguments
        -----------------
        node1: Node
          A Node object in the flowchart which will be linked
          forward to node2.

        node2: Node
          A Node object in the flowchart which will have node1 linked to it.
        """
        self.nodes[node1].link_forward(self.nodes[node2])
        self.structure_dict[node1].append(node2)
        self.istidy = False

    def unlink_nodes(self, node1, node2):
        """Undo the operations of "link_nodes" and return to previous state.

        Arguments
        -----------------
        node1: Node
          A Node object in the flowchart which was linked forward to node2.

        node2: Node
          A Node object in the flowchart which did have node1 linked to it.
        """
        self.nodes[node1].unlink_forward(self.nodes[node2])
        self.structure_dict[node1].pop(self.structure_dict[node1].index(node2))
        self.istidy = False

    def build_chart(self, nodes=[], structure={}):
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
        nodes: list
          A list of Node objects to add to the flowchart. These will
          be added one at a time in the order provided, thus if
          "linear mode" is on then each one will be appended to the
          end of the flowchart.

        structure: dict
          A dictonary that gives the structure of the flowchart. The
          keys in the dictionary are the name strings of nodes, the
          values can be either name strings or lists of name
          strings. The key will be linked forward to the value(s).
        """
        for node in nodes:
            self.add_node(node)

        if isinstance(structure, list):
            for node1, node2 in zip(structure[:-1], structure[1:]):
                self.link_nodes(node1, node2)
            if not structure[0] == "start":
                self.link_nodes("start", structure[0])
            if not structure[-1] == "end":
                self.link_nodes(structure[-1], "end")
        else:
            for node1 in structure.keys():
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
        visual = self._construct_chart_visual()
        visual.layout()
        visual.draw(filename)

    def save(self, filename):
        """Save the flowchart to file.

        Applies pickling to the core information in the flowchart and
        saves to a given file location. Some python objects cannot be
        pickled and so cannot be saved this way. The user may need to
        write a specialized save function for such structures.

        Arguments
        -----------------
        filename: string
          path to save current flowchart to.
        """
        with open(filename, "wb") as flowchart_file:
            flowchart_file.write(dumps(self.__dict__))

    def load(self, filename):
        """Loads the flowchart representation.

        Reads a pickle file as created by "save" to reconstruct a
        saved flowchart. This function should generally not be
        accessed by the user, instead provide a filename when
        initializing the flowchart and the loading will be handled
        properly. In case you wish to use load directly, it returns a
        dictionary of all the class structures/methods/variables.

        Arguments
        -----------------
        filename: string
          path to load flowchart from.
        """
        with open(filename, "rb") as flowchart_file:
            res = loads(flowchart_file.read())
        return res

    def _tidy_ends(self):
        for name, node in self.nodes.items():
            if name == "end":
                continue
            if node.forward is None:
                RuntimeWarning("%s is undirected, linking to 'end' node" % name)
                self.link_nodes(name, "end")
        self.istidy = True

    def _run(self, state):
        current = "start"
        assert (
            not self.nodes["start"].forward is None
        ), "chart has no structure! start must be linked to a node"
        if not self.istidy:
            self._tidy_ends()
        while True:
            state.update(self.nodes[current](state))
            self.benchmark.append(self.nodes[current].benchmark)
            self.path.append(current)
            try:
                current = self.nodes[current]["next"].name
            except StopIteration:
                break
        return state

    def _construct_chart_visual(self):
        if not self.istidy:
            self._tidy_ends()
        visual = pgv.AGraph(strict=True, directed=True)
        for node in self.nodes.values():
            visual.add_node(
                node.name, color=node.colour, shape=node.shape, style=node.style
            )
        for node1, links in self.structure_dict.items():
            for node2 in links:
                visual.add_edge(node1, node2)
        return visual

    def __str__(self):
        visual = self._construct_chart_visual()
        return visual.string()


class Pipe:
    """Basic object for running flowcharts on states.

    A pipe is initialized with a chart object and can then be called
    on a state, the pipe will make a copy of the flowchart to run on
    the state and will apply that copy. This way each state is
    processed with a fresh version of the class (otherwise some class
    variables could be altered). This is most important when running
    processes in parallel. The pipe object can also be provided a list
    of state objects, in which case the pipe will process all of the
    states in parallel.

    Arguments
    -----------------
    flowchart: chart
      Instance of a chart object which is to be called on a number of
      states.
    """

    def __init__(self, flowchart):
        self.flowchart = flowchart

    def _run(self, state):
        chart = deepcopy(self.flowchart)
        return chart(state)

    def __call__(self, state):

        if isinstance(state, list):
            with Pool(4) as pool:
                return pool.map(self._run, state)
        else:
            return self._run(state)


class State:
    """Dummy object to store state information

    This can be used as a state object to pass information through a
    flowchart.
    """
