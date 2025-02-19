"""This script takes as input a (prepared) csv file, then produces as output
    a new text file that contains the list of all the edges in the graph. The
    script can also export the edgelist in networkx format, so that the graph
    can be imported by other programs that use networkx.

    Author: Riccardo Cappuzzo
"""

import argparse
import ast
import math
import os.path as osp
import pickle
from collections import Counter

import networkx as nx
import numpy as np
import pandas as pd
# from tqdm import tqdm


class DynEdges:
    """Edgelist class. This class contains all the methods and attributes needed
    to build a graph for EmbDI. It also includes a number of functions for the
    assignment of weights to the edges.

    Returns:
        Edgelist: The completed EdgeList
    """
    def __init__(
        self,
        df,
        edgefile_path=None,
        prefixes=["3#__tn", "3$__tt", "5$__idx", "1$__cid"], ### tn: token as number,### tt: token as text, ### idx: id of row, ### cid: id of column
                                                             ### $ represent this value is a string, # represente number
                                                             ### at the beginning, the number (1-7) represent class of this type of value, definition of different classes is written at file README
        # info_file=None,
        smoothing_method="no",
    ):
        """Data structure used to represent dataframe df as a graph. The data
        structure contains a list of all nodes in the graph, built according to
        the parameters passed to the function.

        :param df: dataframe to convert into graph
        :param sim_list: optional, list of pairs of similar values
        :param smoothing_method: one of {no, smooth, inverse_smooth, log, inverse}
        :param flatten: if set to True, spread multi-word tokens over multiple nodes. If set to false, all unique cell
        values will be merged in a single node.
        """
        self._parse_smoothing_method(smoothing_method)
        self.edgelist = []
        self.prefixes = prefixes
        numeric_columns = []
        flatten=False

        for col in df.columns:
            try:
                df[col].dropna(axis=0).astype(float).astype(str)
                numeric_columns.append(col)
            except ValueError:
                pass

        intersection = set()

        frequencies = self.evaluate_frequencies(flatten, df, intersection)

        count_rows = 1

        if edgefile_path is not None:
            fp = open(edgefile_path, "a")
            ###### ??? this ligne is for ???
            # fp.write(",".join(prefixes) + "\n")
        else:
            fp = None

        # Iterate over all rows in the df
        for idx, df_row in df.iterrows(): # tqdm()
            rid = str(df_row['rid'])

            # Remove nans from the row
            row = df_row.dropna()
            # Create a node for the current row id.
            for col in df.columns:
                if col != "rid":
                    try:
                        og_value = row[col]
                        # if value is a list, separate and treat each one as a node
                        ######## coding coding ########
                        try:
                            #  ast.literal_eval() function to safely evaluate a string containing a Python literal (like a dictionary, list, tuple, string, number, etc.) and convert it into its corresponding Python object.
                            result = ast.literal_eval(og_value)
                            if isinstance(result, list):
                                for value in result:
                                    if isinstance(value, str):
                                        value = value.replace('"', r'\"')
                                        value = value.replace(',', '')
                                        value = value.replace(' ', '_')
                                
                                    self.draw(value, flatten, intersection, frequencies, rid, col, numeric_columns, fp)
                                
                        except (ValueError, SyntaxError):
                            value = og_value
                            if isinstance(value, str):
                                value = value.replace('"', r'\"')
                                value = value.replace(',', '')
                                value = value.replace(' ', '_')
                            self.draw(value, flatten, intersection, frequencies, rid, col, numeric_columns, fp)
                    except KeyError:
                        continue
            # print("\r# {:0.1f} - {:}/{:} tuples".format(count_rows / len(df) * 100, count_rows, len(df)), end="")
            # count_rows += 1

    @staticmethod
    def f_no_smoothing():
        """Uniform weights

        Returns:
            float: A uniform weight for all values, regardless of the frequency.
        """
        return 1.0

    @staticmethod
    def smooth_exp(x, eps=0.01, target=10, k=0.5):
        """Weight function that assigns weights based on a node's frequency.
        Nodes with degree 1 have the highest weight (1), nodes with higher
        degree are assigned a decreasing weight with minimum value k. The
        parameter "target" is the frequency beyond which each node will receive
        the lowest weight. The function is defined in such a way that
        f(target)~=k+eps

        Args:
            x (np.Array): The vector of frequencies.
            eps (float, optional): Internal parameter. Defaults to 0.01.
            target (int, optional): Frequency value beyond which all nodes are
            assigned the minimum weight. Defaults to 10.
            k (float, optional): Value of the minimum value. Defaults to 0.5.

        Returns:
            _type_: Weight vector.
        """
        t = (eps / (1 - k)) ** (1 / (1 - target))
        y = (1 - k) * t ** (-x + 1) + k
        return y

    @staticmethod
    def inverse_smooth(x, s):
        y = 1 / 2 * (-((1 + s) ** (1 - x)) + 2)
        return y

    @staticmethod
    def inverse_freq(freq):
        return 1 / freq

    @staticmethod
    def log_freq(freq, base=10):
        return 1 / (math.log(freq, base) + 1)

    def smooth_freq(self, freq, eps=0.01):
        if self.smoothing_method == "smooth":
            return self.smooth_exp(freq, eps, self.smoothing_target)
        if self.smoothing_method == "inverse_smooth":
            return self.inverse_smooth(freq, self.smoothing_k)
        elif self.smoothing_method == "log":
            return self.log_freq(freq, self.smoothing_target)
        elif self.smoothing_method == "inverse":
            return self.inverse_freq(freq)
        elif self.smoothing_method == "no":
            return self.f_no_smoothing()

    @staticmethod
    def convert_cell_value(original_value):
        """
        Convert cell values to strings. Round float values.
        :param original_value: The value to convert to str.
        :return: The converted value.
        """

        # If the cell is the empty string, or np.nan, return None.
        if original_value == "":
            return None
        if original_value != original_value:
            return None
        try:
            float_c = float(original_value)
            if math.isnan(float_c):
                return None
            cell_value = str(int(float_c))
        except ValueError:
            cell_value = str(original_value)
        except OverflowError:
            cell_value = str(original_value)
        return cell_value

    def _parse_smoothing_method(self, smoothing_method):
        """
        Convert the smoothing method supplied by the user into parameters that can be used by the edgelist.
        This function is also performing error checking on the input parameters.

        :param smoothing_method: One of "smooth", "inverse_smooth", "log", "piecewise", "no".
        """
        if smoothing_method.startswith("smooth"):
            smooth_split = smoothing_method.split(",")
            if len(smooth_split) == 3:
                self.smoothing_method, self.smoothing_k, self.smoothing_target = smooth_split
                self.smoothing_k = float(self.smoothing_k)
                self.smoothing_target = float(self.smoothing_target)
                if not 0 <= self.smoothing_k <= 1:
                    raise ValueError("Smoothing k must be in range [0,1], current k = {}".format(self.smoothing_k))
                if self.smoothing_target < 1:
                    raise ValueError("Smoothing target must be > 1, current target = {}".format(self.smoothing_target))
            elif len(smooth_split) == 1:
                self.smoothing_method = "smooth"
                self.smoothing_k = 0.2
                self.smoothing_target = 200
            else:
                raise ValueError("Unknown smoothing parameters.")
        if smoothing_method.startswith("inverse_smooth"):
            smooth_split = smoothing_method.split(",")
            if len(smooth_split) == 2:
                self.smoothing_method, self.smoothing_k = smooth_split
                self.smoothing_k = float(self.smoothing_k)
            elif len(smooth_split) == 1:
                self.smoothing_method = "inverse_smooth"
                self.smoothing_k = 0.1
            else:
                raise ValueError("Unknown smoothing parameters.")
        elif smoothing_method.startswith("log"):
            log_split = smoothing_method.split(",")
            if len(log_split) == 2:
                self.smoothing_method, self.smoothing_target = log_split
                self.smoothing_target = float(self.smoothing_target)
                if self.smoothing_target <= 1:
                    raise ValueError("Log base must be > 1, current base = {}".format(self.smoothing_target))
            elif len(log_split) == 1:
                self.smoothing_method = "log"
                self.smoothing_target = 10
            else:
                raise ValueError("Unknown smoothing parameters.")
        elif smoothing_method.startswith("piecewise"):
            piecewise_split = smoothing_method.split(",")
            if len(piecewise_split) == 2:
                self.smoothing_method, self.smoothing_target = piecewise_split
                self.smoothing_target = float(self.smoothing_target)
                self.smoothing_k = 10
            elif len(piecewise_split) == 3:
                self.smoothing_method, self.smoothing_target, self.smoothing_k = piecewise_split
                self.smoothing_target = float(self.smoothing_target)
                self.smoothing_k = float(self.smoothing_k)
            elif len(piecewise_split) == 1:
                self.smoothing_method = self.smoothing_method
                self.smoothing_target = 20
                self.smoothing_k = 10
            else:
                raise ValueError("Unknown smoothing parameters. ")
        else:
            self.smoothing_method = smoothing_method

    # @staticmethod
    # def find_intersection_flatten(df, info_file):
    #     print("Searching intersecting values. ")
    #     with open(info_file, "r") as fp:
    #         line = fp.readline()
    #         n_items = int(line.split(",")[1])
    #     df1 = df[:n_items]
    #     df2 = df[n_items:]
    #     #     Code to perform word-wise intersection
    #     # s1 = set([str(_) for word in df1.values.ravel().tolist() for _ in word.split('_')])
    #     # s2 = set([str(_) for word in df2.values.ravel().tolist() for _ in word.split('_')])
    #     print("Working on df1.")
    #     s1 = set([str(_) for _ in df1.values.ravel().tolist()])
    #     print("Working on df2.")
    #     s2 = set([str(_) for _ in df2.values.ravel().tolist()])

    #     intersection = s1.intersection(s2)

    #     return intersection

    @staticmethod
    def evaluate_frequencies(flatten, df, intersection):
        if flatten and intersection:
            split_values = []
            for val in df.values.ravel().tolist():
                if val not in intersection and isinstance(val, str):
                    split = val.split("_")
                else:
                    split = [str(val)]
                split_values += split
            frequencies = dict(Counter(split_values))
        elif flatten:
            split_values = []
            for val in df.values.ravel().tolist():
                if isinstance(val, str):
                    split = val.split("_")
                else:
                    split = [str(val)]
                split_values += split
            frequencies = dict(Counter(split_values))
        else:
            frequencies = dict(Counter([str(_) for _ in df.values.ravel().tolist() if _ == _]))
        # Remove null values if they somehow slipped in.
        frequencies.pop("", None)
        frequencies.pop(np.nan, None)

        return frequencies

    @staticmethod
    def prepare_split(cell_value, flatten, intersection):
        if flatten and intersection:
            if cell_value in intersection:
                valsplit = [cell_value]
            else:
                valsplit = cell_value.split("_")
        elif flatten:
            valsplit = cell_value.split("_")
        else:
            valsplit = [cell_value]
        return valsplit

    

    def draw(self, value, flatten, intersection, frequencies, rid, col, numeric_columns, fp):
        # Convert cell values to strings. Round float values.
        cell_value = self.convert_cell_value(value)
        if cell_value:
            # Tokenize cell_value depending on the chosen strategy.
            valsplit = self.prepare_split(cell_value, flatten, intersection)

            for split in valsplit:
                split = split.replace('"', r'\"')
                split = split.replace(',', '')
                try:
                    smoothed_f = self.smooth_freq(frequencies[split])
                except KeyError:
                    smoothed_f = 1

                ##### write for rows
                rid_node = rid
                if col in numeric_columns:
                    cell_node = "tn__" + split
                else:
                    cell_node = "tt__" + split

                weight_rid_to_cell = 1
                weight_cell_to_rid = smoothed_f
                self.edgelist.append([rid_node, cell_node, weight_rid_to_cell, weight_cell_to_rid])

                ##### write for columns
                cid_node = "cid__" + col
                if col in numeric_columns:
                    cell_node = "tn__" + split
                else:
                    cell_node = "tt__" + split
                
                weight_cell_to_cid = smoothed_f
                weight_cid_to_cell = 1
                self.edgelist.append([cell_node, cid_node, weight_cell_to_cid, weight_cid_to_cell])

                if fp is not None:
                    edgerow_rid_cell = '"{}","{}",{},{}\n'.format(
                        rid_node, cell_node, weight_rid_to_cell, weight_cell_to_rid
                    )
                    fp.write(edgerow_rid_cell)

                    edgerow_cid_cell = '"{}","{}",{},{}\n'.format(
                        cell_node, cid_node, weight_cell_to_cid, weight_cid_to_cell
                    )
                    fp.write(edgerow_cid_cell)

    def get_edgelist(self):
        """Return the list of edges.

        Returns:
            list: List that contains edges in format
                    (node_1, node_2, weight_12, weight_21)
        """
        return self.edgelist

    def get_prefixes(self):
        return self.prefixes

    def convert_to_dict(self):
        self.graph_dict = {}
        for edge in self.edgelist:
            if len(edge) == 4:
                node_1, node_2, weight_1, weight_2 = edge
            elif len(edge) == 3:
                node_1, node_2, weight_1 = edge
                weight_2 = 0
            else:
                raise ValueError(f"Edge {edge} contains errors.")

            if node_1 in self.graph_dict:
                self.graph_dict[node_1][node_2] = {"weight": weight_1}
            else:
                self.graph_dict[node_1] = {node_2: {"weight": weight_1}}

            if node_2 in self.graph_dict:
                self.graph_dict[node_2][node_1] = {"weight": weight_2}
            else:
                self.graph_dict[node_2] = {node_1: {"weight": weight_2}}

        return self.graph_dict

    def convert_to_numeric(self):
        i2n = {idx: node_name for idx, node_name in enumerate(self.graph_dict.keys())}
        n2i = {node_name: idx for idx, node_name in i2n.items()}

        numeric_dict = {}

        for node in self.graph_dict:
            adj = self.graph_dict[node]
            new_adj = [n2i[_] for _ in adj]
            numeric_dict[n2i[node]] = new_adj

        return numeric_dict


def dynedges_generation(df, edgefile_path, prefixes, smoothing_method):
    '''
    generate edges, prepare for graph
    :return : list of edges which can be used directly for constructing a Graph
    '''
    e = DynEdges(df, edgefile_path, prefixes, smoothing_method).edgelist
    return e