
from random import random
import sys
# TODO:
# Input: vec_a, vec_b are two array of double of same length
# Output: p value

# Implement kstest using scipy.stats.kstest
from scipy.stats import ks_2samp
def kstest(vec_a, vec_b):
  _, p = ks_2samp(vec_a, vec_b)
  return p


class Node:
  def __init__(self, id, tmin_vec, tmax_vec):
    self.id = id
    self.tmin_vec = tmin_vec
    self.tmax_vec = tmax_vec

  def get_sibling_id(self):
    parent_id = self.id[:-1]
    last_bit = self.id[-1]
    if last_bit == '0':
      return parent_id + '1'
    else:
      return parent_id + '0'

def get_all_left_nodes_id(nodes_dict):
  left_nodes = []
  for nid in nodes_dict:
    if len(nid) > 0:
      last_bit = nid[-1]
      if last_bit == '0':
        left_nodes.append(nid)
  return left_nodes

def read_nodes_from_csv(in_file):
  nodes_dict = {}
  with open(in_file, 'r') as f:
    for line in f:
      items = line.strip().split(',')

      node_id = items[0]
      len_tmin_vec = int(items[1])
      len_tmax_vec = int(items[2])

      tmin_vec, tmax_vec = [], []

      for i in range(len_tmin_vec):
        tmin_vec.append(float(items[i + 3]))

      for i in range(len_tmax_vec):
        tmax_vec.append(float(items[i + 3 + len_tmin_vec]))

      nodes_dict[node_id] = Node(node_id, tmin_vec, tmax_vec)

    return nodes_dict

def compute_node_pairs_should_merge(nodes, thres=0.05):
  tmin_pairs, tmax_pairs = [], []

  left_nodes = get_all_left_nodes_id(nodes)

  for left_node_id in left_nodes:
    left_node = nodes[left_node_id]
    right_node_id = left_node.get_sibling_id()
    right_node = nodes[right_node_id]

    # base on kstest on tmin_vec
    a = left_node.tmin_vec
    b = right_node.tmin_vec
    p = kstest(a, b)
    if p > thres:
      tmin_pairs.append((left_node.id, right_node.id))

    # base on kstest on tmax_vec
    a = left_node.tmax_vec
    b = right_node.tmax_vec
    p = kstest(a, b)
    if p > thres:
      tmax_pairs.append((left_node.id, right_node.id))

  return tmin_pairs, tmax_pairs

def output_node_pairs_should_merge(pairs, f):
  for p in pairs:
    f.write(p[0] + ',' + p[1] + '\n')

if __name__ == '__main__':
  in_file = '/home/GL/Desktop/node-daily-avg-tminmax.csv'
  out_file_1 = 'data/node-pairs-should-merge-using-kstest-tmin.csv'
  out_file_2 = 'data/node-pairs-should-merge-using-kstest-tmax.csv'

  nodes = read_nodes_from_csv(in_file)
  tmin_pairs, tmax_pairs = compute_node_pairs_should_merge(nodes)

  # output_node_pairs_should_merge(tmin_pairs, sys.stdout)

  with open(out_file_1, 'w') as f1, open(out_file_2, 'w') as f2:
    output_node_pairs_should_merge(tmin_pairs, f1)
    output_node_pairs_should_merge(tmax_pairs, f2)
