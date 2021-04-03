# Community Detection - Python implementation, no external packages are needed.
## Part I: Girvan Newman algorithm. 

The code uses the example graph: 
```
adjacent_dict = {'a':['b','c'], 
                 'b':['a','c','d'],
                 'c':['a','b'],
                 'd':['b','e','f','g'],
                 'e':['d','f'],
                 'f':['d','e','g'],
                 'g':['d','f']
} 
```

and the vertices list: 

```vertices_list = ['a','b','c','d','e','f','g']```


The final result of GN algorithm is shown in the dict below.
Key is the edge, value is the betweenness of the edge.

```
{('d', 'e'): 4.5,
 ('d', 'f'): 4.0,
 ('d', 'g'): 4.5,
 ('b', 'd'): 12.0,
 ('a', 'b'): 5.0,
 ('a', 'c'): 1.0,
 ('b', 'c'): 5.0,
 ('f', 'g'): 1.5,
 ('e', 'f'): 1.5}
 ```
 ## Part II: Community detection using Modularity Q
 The code uses the example graph: 
```
complete_graph = {'a':['b','c'], 
                  'b':['a','c','d'],
                  'c':['a','b'],
                  'd':['b','e','f','g'],
                  'e':['d','f'],
                  'f':['d','e','g'],
                  'g':['d','f']
} 
```

and the vertices list: 

```vertices_list = ['a','b','c','d','e','f','g']```

1. Function call:  best_community, best_q = community_detection(complete_graph, vertices_list)
2. Parameters: 
   1. complete_graph: dict representation of the graph
   2. vertices_list: list representation of all vertices of the graph
3. Return: list representation of the best communities and corresponding Q score
   1. best communities: ```[['a', 'b', 'c'], ['d', 'e', 'f', 'g']]```
   2. Q score: ```0.3641975308641976```

