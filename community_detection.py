import time
from collections import defaultdict
import copy
import itertools

def betweenness(vertices_list, adjacent_dict):
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

    total_edge_credit_dict = dict(sorted(total_edge_credit_dict.items(), key=lambda item: (-item[1], item[0])))
    return total_edge_credit_dict

def remove_edge(adjacent_dict, edge):
    vertex1 = edge[0]
    vertex2 = edge[1]
    adjacent_dict[vertex1].remove(vertex2)
    adjacent_dict[vertex2].remove(vertex1)

def find_communities(adjacent_dict, vertices_list):
    found_communities = list() # list of communities, each community is represented as a list
    curr_community = list() # a single commuity
    found_vertices = list() # list of vertices that had been included in known communities
    queue = list() 
    
    for vertex in vertices_list:
        if vertex not in found_vertices: # if a vertex is already in a known community, then skip it
            queue.append(vertex)
            while queue:
                node = queue.pop(0)
                neighbor_list = adjacent_dict[node]
                # this node has no connections to other nodes, deg=0. Just a single node
                if len(neighbor_list)==0: 
                    found_vertices.append(node)
                    curr_community.append(node)
                else:
                    for neighbor in neighbor_list:
                        if neighbor not in found_vertices:
                            queue.append(neighbor)
                            found_vertices.append(neighbor)
                            curr_community.append(neighbor)
            # current queue is empty, which means all nodes in this community have been found
            found_communities.append(sorted(curr_community))
            curr_community = list()
    return found_communities
            
def calculateModularity(partitioned_graph, communities, two_m, A_matrix):
    q = 0
    for community in communities:
        edges_pair = list(itertools.product(community, community))
        for edge in edges_pair:
            i = edge[0]
            j = edge[1]
            ki = len(complete_graph[i])
            kj = len(complete_graph[j])
            q += A_matrix.get(tuple(sorted((i,j))), 0) - (ki*kj)/two_m
    return q / two_m

def findBestCommunities(complete_graph, vertices_list):
    partitioned_graph = copy.deepcopy(complete_graph)
    two_m = sum([len(complete_graph[node]) for node in complete_graph])
    A_matrix = dict()
    for node in complete_graph:
        for node2 in complete_graph[node]:
            A_matrix[tuple(sorted((node, node2)))] = 1
    
    betweenness_dict = betweenness(vertices_list, partitioned_graph)
    best_q = calculateModularity(partitioned_graph, [vertices_list], two_m, A_matrix)
    best_comm = find_communities(partitioned_graph, vertices_list)

    while len(betweenness_dict)!=0:
        edge_to_remove = list(betweenness_dict)[0] # the first key is the edge with highest betweenness
        remove_edge(partitioned_graph, edge_to_remove) # remove the edge with top betweenness
        betweenness_dict = betweenness(vertices_list, partitioned_graph) # re-compute the betweenness
        curr_comm = find_communities(partitioned_graph, vertices_list) # identity the new communities
        curr_q = calculateModularity(partitioned_graph, curr_comm, two_m, A_matrix) # calculate modularity for new communities
        if curr_q > best_q:
            best_q = curr_q
            best_comm = copy.deepcopy(curr_comm)
    return best_comm, best_q

complete_graph = {'a':['b','c'],
                  'b':['a','c','d'],
                  'c':['a','b'],
                  'd':['b','e','f','g'],
                  'e':['d','f'],
                  'f':['d','e','g'],
                  'g':['d','f']
                 }
vertices_list = ['a','b','c','d','e','f','g']

best_comm, best_q = findBestCommunities(complete_graph, vertices_list)
print('The best partition of the network is\n{}, the modularity q of this partition is {}'.format(best_comm, best_q))

