"""author: Connor Stone

summary:
  core objects used to construct flowchart analysis pipelines.

description:
  This module contains classes allowing the user to encapsulate code
  in a framework which fundamentally represents code as a
  flowchart. This makes for more modular code which can be tested and
  updated easily. Conceptually every element of the flowchart is a
  "Node" and these nodes are linked together. 
"""

from time import time
from inspect import signature
from .flow_exceptions import FlowExitChart, FlowExit

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

    """

    node_state = {}
    
    def __init__(self, **kwargs):
        if "name" in kwargs: self.name = kwargs["name"]
        else: self.name = self.__class__.__name__
        
        self.owner = None
        self.forward = []
        self.reverse = set()
        self.benchmark = -1
        self.visual_kwargs = {'color': "black",
                              'shape': 'plain',
                              'style': 'solid'}
        if "action" in kwargs:
            self.action = kwargs["action"]

    def action(self, state):
        """Placeholder function which defines the primary behaviour of the
        node.

        Arguments
        -----------------
        state: object
          container for all information related to a flowchart
          analysis task.

        """
        return state

    def set_owner(self, node):
        """Pointer to the object which contains this node.

        Arguments
        -----------------
        node: Node
          A Node object which can hold nodes, for example a Chart
          object.

        """
        self.owner = node

    @classmethod
    def all_subclasses(cls):
        subclasses = {}
        for subclass in cls.__subclasses__():
            subclasses[subclass.__name__] = subclass
            subclasses.update(subclass.all_subclasses())
        return subclasses
        
    
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
        self.forward.append(node)
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
        self.forward.pop(self.forward.index(node))
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

    def exit_chart(self, msg = ""):
        raise FlowExitChart(msg)
    def exit_flow(self, msg = ""):
        raise FlowExit(msg)

    def _run(self, state):
        """Wrapper function for node specific action function.

        Arguments
        -----------------
        state: object
          container for all information related to a flowchart
          analysis task.

        """
        return self.action(state)

    def __call__(self, state):
        start = time()
        res = self._run(state)
        self.benchmark = time() - start
        return res

    def next(self):
        return self.forward[0]

    def __str__(self):
        return self.name

