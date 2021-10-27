from time import time
from inspect import signature
import pygraphviz as pgv
from copy import deepcopy
from pickle import dumps, loads
from multiprocessing import Pool

class symbol(object):

    def __init__(self, name, func = None):
        self.name = name
        self.forward = None
        self.reverse = set()
        self.status = 'initialized'
        self.benchmark = -1
        self.colour = 'black'
        self.shape = 'plain'
        self.style = 'solid'
        if not func is None:
            self.update_run(func)
        
    def _run(self, state):
        return state

    def update_run(self, func):
        self._run = func

    def __call__(self, state):
        self.status = 'running'
        start = time()
        sig = signature(self._run)
        if 'self' in sig.parameters:            
            res = self._run(self,state)
        else:
            res = self._run(state)
        self.benchmark = time() - start
        self.status = 'complete'
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
    
    def __getitem__(self,key):
        if key == 'next' and not self.forward is None:
            return self.forward
        else:
            raise StopIteration()

    def __str__(self):
        return self.name
        
class start(symbol):
    def __init__(self):
        super().__init__('start')
        self.colour = 'blue'
        self.shape = 'box'
        self.style = 'rounded'
        
    def link_reverse(self, S):
        raise AttributeError('\'start\' object has no method \'link_reverse\'')

    def unlink_reverse(self, S):
        raise AttributeError('\'start\' object has no method \'unlink_reverse\'')

class end(symbol):
    def __init__(self):
        super().__init__('end')
        self.colour = 'red'
        self.shape = 'box'
        self.style = 'rounded'
        
    def link_forward(self, S):
        raise AttributeError('\'end\' object has no method \'link_forward\'')
    
    def unlink_forward(self, S):
        raise AttributeError('\'end\' object has no method \'unlink_forward\'')

    def __getitem__(self, key):
        raise StopIteration()

class process(symbol):
    def __init__(self, name, func = None):
        super().__init__(name, func)
        self.shape = 'box'

class decision(symbol):
    def __init__(self, name, func = None):
        super().__init__(name, func)
        self.shape = 'diamond'
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
        S.unlink_reverse(self)

    def __call__(self, state):
        self.status = 'running'
        start = time()
        sig = signature(self._run)
        if 'self' in sig.parameters:            
            res = self._run(self,state)
        else:
            res = self._run(state)
        self.forward = self.options[res]
        self.benchmark = time() - start
        self.status = 'complete'
        return state

class chart(symbol):

    def __init__(self, name = None, filename = None):
        if not filename is None:
            res = self.load(filename)
            super().__init__(res['name'])
            self.__dict__.update(res)
            return 
        
        super().__init__(name)
        self.symbols = {}
        self.structure_dict = {}
        self._linear_mode = False
        self._linear_mode_link = 'start'
        self.add_node(start())
        self.add_node(end())        
        self.path = []
        self.benchmark = []
        self.istidy = False
        self.shape = 'hexagon'

    def linear_mode(self, mode):
        if mode:
            self._linear_mode = True
            while not self.symbols[self._linear_mode_link].forward is None:
                prev = self._linear_mode_link
                self._linear_mode_link = self.symbols[self._linear_mode_link].forward.name
            if self._linear_mode_link == 'end':
                self._linear_mode_link = prev
            else:
                self.link_nodes(self._linear_mode_link, 'end')
        else:
            self._linear_mode = False
        
    def add_node(self, S):
        self.symbols[S.name] = S
        self.structure_dict[S.name] = []
        self.istidy = False
        if self._linear_mode:
            self.unlink_nodes(self._linear_mode_link, 'end')
            self.link_nodes(self._linear_mode_link, S.name)
            self.link_nodes(S.name, 'end')
            self._linear_mode_link = S.name

    def add_process_node(self, name, func = None):
        newprocess = process(name, func)
        self.add_node(newprocess)

    def add_decision_node(self, name, func = None):
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

    def build_chart(self, symbols = [], structure = {}):
        for S in symbols:
            self.add_node(S)

        if type(structure) == list:
            for S1, S2 in zip(structure[:-1], structure[1:]):
                self.link_nodes(S1,S2)
            if not structure[0] == 'start':
                self.link_nodes('start', structure[0])
            if not structure[-1] == 'end':
                self.link_nodes(structure[-1], 'end')
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
        with open(filename, 'wb') as f:
            f.write(dumps(self.__dict__))
            
    def load(self, filename):
        with open(filename, 'rb') as f:
            res = loads(f.read())
        return res
    
    def _tidy_ends(self):
        for S in self.symbols.keys():
            if S == 'end':
                continue
            if self.symbols[S].forward is None:
                RuntimeWarning('%s is undirected, linking to \'end\' node' % S)
                self.link_nodes(S, 'end')
        self.istidy = True

    def _run(self, state):
        current = 'start'
        assert not self.symbols['start'].forward is None, 'chart has no structure! start must be linked to a node'
        if not self.istidy:
            self._tidy_ends()
        while True:
            state.update(self.symbols[current](state))
            self.benchmark.append(self.symbols[current].benchmark)
            self.path.append(current)
            try:
                current = self.symbols[current]['next'].name
            except StopIteration:
                break
        return state
        
    def _construct_chart_visual(self):
        if not self.istidy:
            self._tidy_ends()
        visual = pgv.AGraph(strict = True, directed = True)
        for S in self.symbols.values():
            visual.add_node(S.name, color = S.colour, shape = S.shape, style = S.style)
        for S1 in self.structure_dict.keys():
            for S2 in self.structure_dict[S1]:
                visual.add_edge(S1, S2)        
        return visual
    
    def __str__(self):
        visual = self._construct_chart_visual()
        return visual.string()

class pipe(object):

    def __init__(self, chart):
        self.chart = chart

    def _run(self, state):
        C = deepcopy(self.chart)
        return C(state)
        
    def __call__(self, state):
        
        if type(state) == dict:
            return self._run(state)
        else:
            with Pool(4) as pool:
                return pool.map(self._run, state)

class state(object):
    pass
