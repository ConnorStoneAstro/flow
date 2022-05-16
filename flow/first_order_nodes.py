"""author: Connor Stone

summary:
  Nodes with defined behaviour in a flowchart.

description:
  There are a number of types of nodes for performing different roles
  in a flowchart. The most basic being "start" and "end" which are
  terminators for the flowchart and do nothing themselves. A "process"
  Node is the workhorse of the flowchart and should enclose most of
  your analysis code. A "decision" Node branches the flowchart into
  different sections to handle conditional elments of the
  analysis. Nodes in this module ought to have straightforward
  behaviour and act as the nuclear elements of a flowchart from which
  the full flowchart is built.
"""

from time import time
from inspect import signature
from .core import Node


class Start(Node):
    """Initialization point of a flowchart.

    This Node is the starting point of a flowchart. It has no
    functionality except to pass the state along to the next Node in
    the flowchart. Note that this class has no ability to link
    backwards as it is by requirement the first node. It's
    initialization takes no arguments.
    """

    def __init__(self):
        super().__init__()
        self.visual_kwargs['color'] = "blue"
        self.visual_kwargs['shape'] = 'box'
        self.visual_kwargs['style'] = 'rounded'
        self.visual_kwargs['root'] = True
        self.visual_kwargs['pin'] = True

class End(Node):
    """Stopping point of a flowchart.

    This Node represents an endpoint of a flowchart. Whenever a
    process reaches this note it will end computation and return the
    state in it's current form. Note that this class has no ability to
    link forward as it is by requirement the last node in the
    flowchart. It's initialization takes no arguments.
    """

    def __init__(self):
        super().__init__()
        self.visual_kwargs['color'] = "red"
        self.visual_kwargs['shape'] = 'box'
        self.visual_kwargs['style'] = 'rounded'

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
        raise StopIteration


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.visual_kwargs['shape'] = "box"


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.visual_kwargs['shape'] = "diamond"

    def __call__(self, *state):
        start = time()
        res = self._run(*state)
        if isinstance(res,int) and res != 0:
            self.forward[0], self.forward[res] = self.forward[res], self.forward[0]
        elif isinstance(res, str) and res != self.forward[0].name:
            names = list(self.forward[i].name for i in range(len(self.forward)))
            index = names.index(res)
            self.forward[0], self.forward[index] = self.forward[index], self.forward[0]
        elif isinstance(res, Node) and res is not self.forward[0]:
            names = list(self.forward[i].name for i in range(len(self.forward)))
            index = names.index(res.name)
            self.forward[0], self.forward[index] = self.forward[index], self.forward[0]            
        self.benchmark = time() - start
        return state
