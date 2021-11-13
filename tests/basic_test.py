from context import flow
from copy import deepcopy

def proc1(state):
    print('in process 1, message: ', state['first'])
    state.update({'second': 'second was added'})
    return state
    
def proc2(state):
    print('in process 2, message: ', state['second'])
    state.update({'third': 'third was added'})
    return state
    
def proc3(state):
    print('in process 3, message: ', state['third'])
    if 'choose' in state:
        state.update({'choose': 'P4'})
    else:
        state.update({'choose': 'P2'})
    return state

def dec1(state):
    print('in decision 1, choosing: ', state['choose'])
    return state['choose']

def proc4(state):
    print('in process 4. current state: ', state)
    return state


# Test build chart construction, explicit
######################################################################
print('test 1')
methods = [flow.Process('P1', proc1), flow.Process('P2', proc2), flow.Process('P3', proc3), flow.Decision('D1', dec1), flow.Process('P4', proc4)]

C1 = flow.Chart('C1')
C1.build_chart(methods, {'start': 'P1', 'P1':'P2', 'P2': 'P3', 'P3': 'D1', 'D1':['P2','P4'], 'P4': 'end'})
print(C1)
C1.draw('basic_test.png')

CC1 = deepcopy(C1)

res = CC1({'first': 'first message'})
print(res)
print('benchmarks: ', CC1.benchmark, CC1.benchmarks, sum(CC1.benchmarks))
# Test nesting charts
######################################################################
print('test 2')

def newbeginning(state):
    state.update({'first': 'different first message'})
    return state

C2 = flow.Chart('C2')

C2.add_node(flow.Process('Pnew', newbeginning))
C2.add_node(C1)
C2.link_nodes('Pnew', 'C1')
C2.link_nodes('start', 'Pnew')
C2.link_nodes('C1', 'end')

C2.draw('basic_test_meta.png')

print(C2({'first': 'this message will be erased'}))
print('benchmarks: ', C2.benchmark, C2.benchmarks, sum(C2.benchmarks))


# test linear mode linking
######################################################################
print('test 3')

C3 = flow.Chart('C3')
C3.linear_mode(True)
C3.add_process_node('P1', proc1)
C3.add_process_node('P2', proc2)
C3.add_process_node('P3', proc3)
C3.add_decision_node('D1', dec1)
C3.add_process_node('P4', proc4)
C3.linear_mode(False)
C3.link_nodes('D1', 'P2')

C3.draw('basic_test_linear.png')

print(C3({'first': 'first message from linear mode chart'}))
print('benchmarks: ', C3.benchmark, C3.benchmarks, sum(C3.benchmarks))
