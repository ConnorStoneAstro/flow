from time import time
from inspect import signature
import pygraphviz as pgv
from copy import deepcopy
from pickle import dumps, loads
from multiprocessing import Pool


class symbol(object):
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

    def _run(self, state):
        return state

    def update_run(self, func):
        self._run = func

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

    def link_forward(self, S):
        self.forward = S
        S.link_reverse(self)

    def unlink_forward(self, S):
        self.forward = None
        S.unlink_reverse(self)

    def link_reverse(self, S):
        self.reverse.update([S])

    def unlink_reverse(self, S):
        self.reverse.remove(S)

    def __getitem__(self, key):
        if key == "next" and not self.forward is None:
            return self.forward
        else:
            raise StopIteration()

    def __str__(self):
        return self.name


class start(symbol):
    """Initialization point of a flowchart.

    This symbol is the starting point of a flowchart. It has no
    functionality except to pass the state along to the next symbol in
    the flowchart. Note that this class has no ability to link
    backwards as it is by requirement the first node. It's
    initialization takes no arguments.
    """

    def __init__(self):
        super().__init__("start")
        self.colour = "blue"
        self.shape = "box"
        self.style = "rounded"

    def link_reverse(self, S):
        raise AttributeError("'start' object has no method 'link_reverse'")

    def unlink_reverse(self, S):
        raise AttributeError("'start' object has no method 'unlink_reverse'")


class end(symbol):
    """Stopping point of a flowchart.

    This symbol represents an endpoint of a flowchart. Whenever a
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

    def link_forward(self, S):
        raise AttributeError("'end' object has no method 'link_forward'")

    def unlink_forward(self, S):
        raise AttributeError("'end' object has no method 'unlink_forward'")

    def __getitem__(self, key):
        raise StopIteration()


class process(symbol):
    """Basic node for acting on the state.

    This node retains all default functionality of a symbol. This is
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


class decision(symbol):
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

    def link_forward(self, S):
        if self.forward is None:
            self.forward = S
        self.options[S.name] = S
        S.link_reverse(self)

    def unlink_forward(self, S):
        del self.options[S.name]
        if len(self.options) > 0:
            self.forward = self.options[self.options.keys()[0]]
        else:
            self.forward = None
        S.unlink_reverse(self)

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


class chart(symbol):
    """Main container for a flowchart.

    Stores all the nodes and links between them composing a
    flowchart. The run method for this object will iteratively apply
    each node in the flowchart and progress through the path from
    start to end. This is the main object that users should interact
    with when constructing a flowchart. Includes methods to add/link
    nodes, draw the flowchart, save/load the flowchart, and visualize
    it. This class inherits from the symbol object and so can itself
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
        self.symbols = {}
        self.structure_dict = {}
        self._linear_mode = False
        self._linear_mode_link = "start"
        self.add_node(start())
        self.add_node(end())
        self.path = []
        self.benchmark = []
        self.istidy = False
        self.shape = "hexagon"

    def linear_mode(self, mode):
        if mode:
            self._linear_mode = True
            while not self.symbols[self._linear_mode_link].forward is None:
                prev = self._linear_mode_link
                self._linear_mode_link = self.symbols[
                    self._linear_mode_link
                ].forward.name
            if self._linear_mode_link == "end":
                self._linear_mode_link = prev
            else:
                self.link_nodes(self._linear_mode_link, "end")
        else:
            self._linear_mode = False

    def add_node(self, S):
        self.symbols[S.name] = S
        self.structure_dict[S.name] = []
        self.istidy = False
        if self._linear_mode:
            self.unlink_nodes(self._linear_mode_link, "end")
            self.link_nodes(self._linear_mode_link, S.name)
            self.link_nodes(S.name, "end")
            self._linear_mode_link = S.name

    def add_process_node(self, name, func=None):
        newprocess = process(name, func)
        self.add_node(newprocess)

    def add_decision_node(self, name, func=None):
        newdecision = decision(name, func)
        self.add_node(newdecision)

    def link_nodes(self, S1, S2):
        self.symbols[S1].link_forward(self.symbols[S2])
        self.structure_dict[S1].append(S2)
        self.istidy = False

    def unlink_nodes(self, S1, S2):
        self.symbols[S1].unlink_forward(self.symbols[S2])
        self.structure_dict[S1].pop(self.structure_dict[S1].index(S2))
        self.istidy = False

    def build_chart(self, symbols=[], structure={}):
        for S in symbols:
            self.add_node(S)

        if type(structure) == list:
            for S1, S2 in zip(structure[:-1], structure[1:]):
                self.link_nodes(S1, S2)
            if not structure[0] == "start":
                self.link_nodes("start", structure[0])
            if not structure[-1] == "end":
                self.link_nodes(structure[-1], "end")
        else:
            for S1 in structure.keys():
                if type(structure[S1]) == str:
                    self.link_nodes(S1, structure[S1])
                else:
                    for S2 in structure[S1]:
                        self.link_nodes(S1, S2)

    def draw(self, filename):
        visual = self._construct_chart_visual()
        visual.layout()
        visual.draw(filename)

    def save(self, filename):
        print(self.__dict__)
        with open(filename, "wb") as f:
            f.write(dumps(self.__dict__))

    def load(self, filename):
        with open(filename, "rb") as f:
            res = loads(f.read())
        return res

    def _tidy_ends(self):
        for S in self.symbols.keys():
            if S == "end":
                continue
            if self.symbols[S].forward is None:
                RuntimeWarning("%s is undirected, linking to 'end' node" % S)
                self.link_nodes(S, "end")
        self.istidy = True

    def _run(self, state):
        current = "start"
        assert (
            not self.symbols["start"].forward is None
        ), "chart has no structure! start must be linked to a node"
        if not self.istidy:
            self._tidy_ends()
        while True:
            state.update(self.symbols[current](state))
            self.benchmark.append(self.symbols[current].benchmark)
            self.path.append(current)
            try:
                current = self.symbols[current]["next"].name
            except StopIteration:
                break
        return state

    def _construct_chart_visual(self):
        if not self.istidy:
            self._tidy_ends()
        visual = pgv.AGraph(strict=True, directed=True)
        for S in self.symbols.values():
            visual.add_node(S.name, color=S.colour, shape=S.shape, style=S.style)
        for S1 in self.structure_dict.keys():
            for S2 in self.structure_dict[S1]:
                visual.add_edge(S1, S2)
        return visual

    def __str__(self):
        visual = self._construct_chart_visual()
        return visual.string()


class pipe(object):
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
        C = deepcopy(self.flowchart)
        return C(state)

    def __call__(self, state):

        if type(state) == list:
            with Pool(4) as pool:
                return pool.map(self._run, state)
        else:
            return self._run(state)


class state(object):
    """Dummy object to store state information

    This can be used as a state object to pass information through a
    flowchart.
    """

    pass
