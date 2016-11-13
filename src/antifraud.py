import sys
from collections import deque
"""
Author : Muthu
Description: Implementation of finding antifraud transactions for digital wallnt frim - challenge
from Insight Data Science
"""


class Graph(object):
  """ Graph class as a data structure to hold tthe data
  forming edges and vertices
  """
  def __init__(self, graph_dict=None):
    if graph_dict is None:
      graph_dict = {}
    self.__graph_dict = graph_dict

  def vertices(self):
    return list(self.__graph_dict.keys())

  def edges(self):
    return self.__generate_edges()

  def __generate_edges(self):
    edges = []
    for vertex in self.__graph_dict:
      for neighbbor in self.__graph_dict[vertex]:
        if {vertex, neighbbor} not in edges:
          edges.append({vertex, neighbbor})
    return edges

  def add_vertex(self, vertex):
    if vertex not in self.__graph_dict:
      self.__graph_dict[vertex] = []

  def is_vertex(self, vertex):
    if vertex in self.__graph_dict.keys():
      return True
    return False

  def get_neighbors(self, vertex):
    if vertex in self.__graph_dict.keys():
      return list(self.__graph_dict[vertex])
    return []

  def add_edge(self, edge):
    edge = set(edge)
    (vertex1, vertex2) = tuple(edge)
    # undirected so add it to both vertices
    if vertex1 in self.__graph_dict:
      self.__graph_dict[vertex1].append(vertex2)
    else:
      self.__graph_dict[vertex1] = [vertex2]

    if vertex2 in self.__graph_dict:
      self.__graph_dict[vertex2].append(vertex1)
    else:
      self.__graph_dict[vertex2] = [vertex1]

  """ Returns the distance between 2 nodes """
  def find_path_distance(self, touch, path_forward, path_backward, source, target):
    distance = 0
    root = touch
    while root != source:
      distance += 1
      root = path_forward[root]
    root = touch
    while root != target:
      distance += 1
      root = path_backward[root]
    return distance

  """ Bidirectional bfs for efficient path finding between 2 nodes """
  def bidirectional_bfs(self, vertex1, vertex2):
    if self.is_vertex(vertex1) and self.is_vertex(vertex2):
      queue_forward, visited_forward, path_forward = deque([vertex1]), set([str(vertex1)]), {str(vertex1): 0}
      queue_backward, visited_backward, path_backward = deque([vertex2]), set([str(vertex2)]), {str(vertex2): 0}

      while queue_forward and queue_backward:
        if queue_forward:
          x1 = queue_forward.popleft()
          if x1 == vertex2 or x1 in queue_backward or x1 in visited_backward:
            return self.find_path_distance(x1, path_forward, path_backward, vertex1, vertex2)
          for neighbor in self.__graph_dict[x1]:
            if neighbor not in visited_forward:
              path_forward[str(neighbor)] = x1
              visited_forward.add(str(neighbor))
              queue_forward.extend([str(neighbor)])

        if queue_backward:
          x1 = queue_backward.popleft()
          if x1 == vertex1 or x1 in queue_forward or x1 in visited_forward:
            return self.find_path_distance(x1, path_forward, path_backward, vertex1, vertex2)
          for neighbor in self.__graph_dict[x1]:
            if neighbor not in visited_backward:
              path_backward[str(neighbor)] = x1
              visited_backward.add(str(neighbor))
              queue_backward.extend([str(neighbor)])
    return -1


class AntiFraud(object):
  """ AntiFraud class - Loads and runs transaction check on given transaction data
  Feature1 - Friends are trusted
  Feature2 - friends of friends are trusted
  Feature3 - friends upto 4th degree connections are trusted
  """
  def __init__(self, distance):
    self.__maxDistance = distance
    self.__network = Graph()

  """ Builds graph from the text file """
  def build_graph_from_txt(self, input_path):
    with open(input_path) as file:
      next(file)
      for line in file:
        content = line.split(',')
        vertex1, vertex2 = content[1].strip(), content[2].strip()
        self.__network.add_edge([str(vertex1), str(vertex2)])
    return

  """ Write to all the output files """
  def __write_output(self, output1_file, feature1_op, output2_file, feature2_op, output3_file, feature3_op):
    output1_file.write('\n'.join(feature1_op))
    output1_file.write('\n')
    output2_file.write('\n'.join(feature2_op))
    output2_file.write('\n')
    output3_file.write('\n'.join(feature3_op))
    output3_file.write('\n')

  def build_features(self, input_path, feature1_op_path, feature2_op_path, feature3_op_path):
    feature1, feature2, feature3 = [], [], []
    count = 0
    with open(feature1_op_path, 'w') as output1_file:
      with open(feature2_op_path, 'w') as output2_file:
        with open(feature3_op_path, 'w') as output3_file:
          with open(input_path) as file:
            next(file)
            for line in file:
              content = line.split(',')
              count += 1
              try:
                vertex1, vertex2 = content[1].strip(), content[2].strip()
              except Exception:
                print 'ignoring data as invalid', content
                feature1.append('unverified')
                feature2.append('unverified')
                feature3.append('unverified')
              else:
                # get feature 1
                result = self.__build_feature1(vertex1, vertex2)
                feature1.append(result)
                path_distance = self.__network.bidirectional_bfs(str(vertex1), str(vertex2))
                # get feature 2
                result = self.__build_feature_with_distance(vertex1, vertex2, 2, path_distance)
                feature2.append(result)
                # get feature 3
                result = self.__build_feature_with_distance(vertex1, vertex2, self.__maxDistance, path_distance)
                feature3.append(result)
                # write on batch of 20,000
              if count % 20000 == 0:
                print "writing.. Progress: ", count, ' trasactions'
                self.__write_output(output1_file, feature1, output2_file, feature2, output3_file, feature3)
                feature1, feature2, feature3 = [], [], []

            # write final remaining features to output
            self.__write_output(output1_file, feature1, output2_file, feature2, output3_file, feature3)
            print "writing.. Progress: ", count, ' trasactions'

  def __build_feature_with_distance(self, vertex1, vertex2, distance, path_distance):
    if path_distance <= distance and path_distance >= 0:
      return "trusted"
    else:
      return "unverified"

  def __build_feature1(self, vertex1, vertex2):
    if self.__feature1(vertex1, vertex2):
      return "trusted"
    else:
      return "unverified"

  def __feature1(self, vertex1, vertex2):
    neighbors = self.__network.get_neighbors(vertex1)
    if vertex2 in neighbors:
      return True
    neighbors = self.__network.get_neighbors(vertex2)
    if vertex1 in neighbors:
      return True
    return False

if __name__ == "__main__":
  try:
    batch_input = sys.argv[1]
    stream_input = sys.argv[2]
    feature1_op, feature2_op, feature3_op = sys.argv[3], sys.argv[4], sys.argv[5]
  except Exception as e:
    print 'Usage Error'
    print "Usage: python antifraud.py <batch_ip> <stream_ip> <feature1_op> <feature2_op> <feature3_op>"
    raise 'usage error'
  else:
    try:
        # max distance allowed for feature 3 to be trusted as argument
      fraud = AntiFraud(4)
      print 'Building the network...'
      fraud.build_graph_from_txt(batch_input)
      print 'Done. Started predicting fraud transactions...'
      fraud.build_features(stream_input, feature1_op, feature2_op, feature3_op)
      print 'done..'
    except Exception as e:
      print 'Program error, contact developer'
      raise e
