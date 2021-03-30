from collections import defaultdict

# data preparation
adjacent_dict = {'a':['b','c'],
                 'b':['a','c','d'],
                 'c':['a','b'],
                 'd':['b','e','f','g'],
                 'e':['d','f'],
                 'f':['d','e','g'],
                 'g':['d','f']
                }
vertices_list = ['a','b','c','d','e','f','g']


# algorithm starts here
total_edge_credit_dict = defaultdict(int)
for root in vertices_list:
    # bfs to explore the graph and build the tree
    visited = {root}
    same_level_visited = set() 
    queue = [root]
    same_level_queue = list()
    bfs_result = list() 
    same_level_dict = dict()
    parents = set()

    while queue:
        node = queue.pop(0)
        neighbors = adjacent_dict[node]
        for neighbor in neighbors:
            if neighbor not in visited:
                if node in same_level_dict:
                    same_level_dict[node][0].append(neighbor)
                else:
                    same_level_dict[node] = ([neighbor], [])
                same_level_visited.add(neighbor)
                if neighbor not in set(same_level_queue):
                    same_level_queue.append(neighbor)
            elif neighbor in parents:
                if node in same_level_dict:
                    same_level_dict[node][1].append(neighbor)
                else:
                    same_level_dict[node] = ([], [neighbor])
        if len(queue) == 0:
            bfs_result.append(same_level_dict)
            parents = set(same_level_dict.keys())
            queue = same_level_queue.copy()
            visited = visited.union(same_level_visited)
            same_level_dict = dict()
            same_level_queue = list()
            same_level_visited = set()      

    # bfs tree is built, proceed to GN algorithm----------------------------------------------------

    # assigning node weights to each node from top to bottom
    # initialize all node credits to one
    node_credit_dict = dict()
    node_weight_dict = defaultdict(int)
    for level in bfs_result:
        for node in level:
            if len(level[node][1])==0: # this is the root node
                node_weight_dict[node] += 1
                node_credit_dict[node] = 1 # initialize all node credits to 1
            else:# this node is a child node, not root
                for parent in level[node][1]: # node weight equals to the sum of its parents weight
                    node_weight_dict[node] += node_weight_dict[parent]
                node_credit_dict[node] = 1 # initialize all node credits to 1

    # girvan-newman algorithm, bottom-up approach
    edge_credit_dict = defaultdict(int)
    for level in reversed(bfs_result):
        for node in level:
            if len(level[node][0])!=0: # this is not a leaf node, then first update node credit
                # update node credit
                for child in level[node][0]:
                    edge = tuple(sorted((node, child)))
                    node_credit_dict[node] += edge_credit_dict[edge]
            # compute and update edge credit
            for parent in level[node][1]:
                edge = tuple(sorted((node, parent)))
                ratio = node_weight_dict[parent] / node_weight_dict[node]
                edge_credit_dict[edge] += ratio * node_credit_dict[node]                  

    for edge in edge_credit_dict:
        total_edge_credit_dict[edge] += edge_credit_dict[edge] / 2


print(total_edge_credit_dict)





