# Girvan-Newman-Algorithm
Python implementation of GN algorithm, no external package is needed

The code uses the example graph: 
adjacent_dict = {'a':['b','c'],
                 'b':['a','c','d'],
                 'c':['a','b'],
                 'd':['b','e','f','g'],
                 'e':['d','f'],
                 'f':['d','e','g'],
                 'g':['d','f']
}
and the vertices list: 
vertices_list = ['a','b','c','d','e','f','g']


The final result of GN algorithm is shown in the dict below.
Key is the edge, value is the betweenness of the edge.

{('d', 'e'): 4.5,
 ('d', 'f'): 4.0,
 ('d', 'g'): 4.5,
 ('b', 'd'): 12.0,
 ('a', 'b'): 5.0,
 ('a', 'c'): 1.0,
 ('b', 'c'): 5.0,
 ('f', 'g'): 1.5,
 ('e', 'f'): 1.5}
