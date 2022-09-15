

class FlowException(Exception):
    """
    Base exception for flow related errors
    """

class FlowLinkError(FlowException):
    """
    Error while linking nodes in a flowchart
    """

class FlowExitChart(FlowException):
    """
    Raises a call to end the current chart object's itteration and
    proceed immediately to whatever is next.
    """

class FlowExit(FlowException):
    """
    Raises a call to end the flowchart immediately. 
    """
