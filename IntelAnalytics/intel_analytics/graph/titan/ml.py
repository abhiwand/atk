##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
Titan-base Giraph Machine Learning.
"""
from intel_analytics.subproc import call
from intel_analytics.config import global_config, get_time_str
from intel_analytics.report import ProgressReportStrategy, find_progress,\
    MapReduceProgress
import matplotlib as mpl
mpl.use('Agg')
#matplotlib object-oriented api
import matplotlib.pyplot as plt
#from IPython.display import display
#import numpy as np
# MATLAB-like API
#from pylab import *
import re



class TitanGiraphMachineLearning(object): # TODO: >0.5, inherit MachineLearning
    """
    Titan-based Giraph Machine Learning instance for a graph.
    """

    def __init__(self, graph):
        self._graph = graph
        self._table_name = graph.titan_table_name
        #pagerank_graph"
        #"small_netflix_titan_graph"
        #"small_lda_titan_graph"
        #"ivy_titan4_bi"
        #"small_apl_titan_graph"

    def plot_progress_curve(self,
                            data_x,
                            data_y,
                            curve_title,
                            curve_ylabel):
        """
        Plot progress curve for algorithms
        """
        fig, axes = plt.subplots()
        axes.plot(data_x, data_y, 'b')

        axes.set_title(curve_title, fontsize=14)
        axes.set_xlabel("Number of SuperStep", fontsize=12)
        axes.set_ylabel(curve_ylabel, fontsize=12)
        axes.grid(True, linestyle='-', color='0.75')

    def plot_learning_curve(self,
                            data_x,
                            data_y,
                            data_v,
                            data_t,
                            curve_title):
        """
        Plot learning curve for algorithms
        """
        fig = plt.figure()
        axes1 = fig.add_axes([0.1, 0.1, 0.8, 0.8]) #left,bottom,width,height
        axes1.plot(data_x, data_y, 'b')
        axes1.set_xlabel("Number of SuperStep", fontsize=12)
        axes1.set_ylabel("Cost (Train)", fontsize=12)
        title_str = [curve_title, " (Train)"]
        axes1.set_title(' '.join(map(str, title_str)), fontsize=14)
        axes1.grid(True, linestyle='-', color='0.75')

        axes2 = fig.add_axes([1.1, 0.1, 0.8, 0.8])
        axes2.plot(data_x, data_v, 'g')
        axes2.set_xlabel("Number of SuperStep", fontsize=12)
        axes2.set_ylabel("RMSE (Validate)", fontsize=12)
        title_str = [curve_title, " (Validate)"]
        axes2.set_title(' '.join(map(str, title_str)), fontsize=14)
        axes2.grid(True, linestyle='-', color='0.75')

        axes3 = fig.add_axes([2.1, 0.1, 0.8, 0.8])
        axes3.plot(data_x, data_t, 'y')
        axes3.set_xlabel("Number of SuperStep", fontsize=12)
        axes3.set_ylabel("RMSE (Test)", fontsize=12)
        title_str = [curve_title, " (Test)"]
        axes3.set_title(' '.join(map(str, title_str)), fontsize=14)
        axes3.grid(True, linestyle='-', color='0.75')
        #axes1.legend(['train', 'validate', 'test'], loc='upper right')
        #show()

    def del_old_output(self, output_path):
        """
        delete old output directory if already exists
        """
        del_cmd = 'if hadoop fs -test -e ' + output_path + \
                         '; then hadoop fs -rmr -skipTrash ' + output_path + '; fi'
        call(del_cmd, shell=True)

    def get_report(self, output_path, file_name, time_str):
        """
        get learning curve/convergence progress report
        """
        report_file = global_config['giraph_report_dir'] + '/' + self._table_name + time_str + '_report.txt'
        cmd = 'hadoop fs -get ' + output_path + '/' + file_name + ' ' + report_file
        call(cmd, shell=True)
        return report_file

    def belief_prop(self,
                    input_vertex_property_list,
                    input_edge_property_list,
                    input_edge_label,
                    output_vertex_property_list,
                    max_supersteps=global_config['giraph_belief_propagation_max_supersteps'],
                    convergence_threshold=global_config['giraph_belief_propagation_convergence_threshold'],
                    smoothing=global_config['giraph_belief_propagation_smoothing'],
                    anchor_threshold=global_config['giraph_belief_propagation_anchor_threshold']):
        """
        Loopy belief propagation on MRF

        Parameters
        ----------
        input_vertex_property_list : vertex properties which contains prior vertex value
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        input_edge_property_list: edge properties which contains input edge value
                                  if more than one edge property is used,
                                  expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        max_supersteps : number of super steps to run in Giraph
        smoothing: the Ising smoothing parameter
        convergence_threshold: the convergence threshold
        anchor_threshold: the anchor threshold [0, 1].
                          Vertices whose normalized prior values are greater than
                          this threshold will not be updated

        Returns
        algorithms results in titan table
        -------
        """

        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/lbp'
        lbp_command = self.get_lbp_command(
            self._table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            max_supersteps,
            convergence_threshold,
            smoothing,
            anchor_threshold,
            output_path)
        lbp_cmd = ' '.join(map(str, lbp_command))
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(lbp_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'smoothing': smoothing,
               'anchor_threshold': anchor_threshold
               }
        self._graph.report = report
        return self._graph

    def get_lbp_command(
        self,
        table_name,
        input_vertex_property_list,
        input_edge_property_list,
        input_edge_label,
        output_vertex_property_list,
        max_supersteps,
        convergence_threshold,
        smoothing,
        anchor_threshold,
        output):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_vertex_property_list'] + global_config['hbase_column_family'] +
                input_vertex_property_list,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_belief_propagation_class'],
                '-vif',
                global_config['giraph_belief_propagation_input_format'],
                '-vof',
                global_config['giraph_belief_propagation_output_format'],
                '-op',
                output,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_belief_propagation_max_supersteps'] + max_supersteps,
                global_config['giraph_param_belief_propagation_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_belief_propagation_smoothing'] + smoothing,
                global_config['giraph_param_belief_propagation_anchor_threshold'] + anchor_threshold]

        #print '%s' % ' '.join(map(str, myList))
        #return myList

    def page_rank(self,
                  input_edge_property_list,
                  input_edge_label,
                  output_vertex_property_list,
                  max_supersteps=global_config['giraph_page_rank_max_supersteps'],
                  convergence_threshold=global_config['giraph_page_rank_convergence_threshold'],
                  reset_probability=global_config['giraph_page_rank_reset_probability'],
                  convergence_output_interval=global_config['giraph_param_page_rank_convergence_output_interval']
                  ):
        """
        The PageRank algorithm, http://en.wikipedia.org/wiki/PageRank

        Parameters
        ----------
        input_edge_property_list: edge properties which contains input edge value.
                                   if more than one edge property is used,
                                   expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        max_supersteps : number of super steps to run in Giraph
        convergence_threshold: the convergence threshold
        reset_probability: the reset probability
        convergence_output_interval: convergence progress output interval (default: every superstep)

        Returns
        algorithm results in titan table
        Convergence curve is accessible through page_rank.stats object
        -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/pr'
        pr_command = self.get_pr_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            max_supersteps,
            convergence_threshold,
            reset_probability,
            convergence_output_interval,
            output_path
        )
        pr_cmd = ' '.join(map(str, pr_command))
        print pr_cmd
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(pr_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report_file = self.get_report(output_path, 'pr-convergence-report_0', time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        #r = re.compiler(r'superstep')
        data_x = []
        data_y = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])

        curve_title = 'Page Rank Convergence Curve'
        curve_ylabel = 'Vertex Value Change'
        self.plot_progress_curve(data_x, data_y, curve_title, curve_ylabel)
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'reset_probability': reset_probability,
               'convergence_output_interval': convergence_output_interval,
               'supersteps': data_x,
               'convergence_progress': data_y
               }
        self._graph.report = report
        return self._graph

    def get_pr_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            max_supersteps,
            convergence_threshold,
            reset_probability,
            convergence_output_interval,
            output_path
            ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_page_rank_class'],
                '-mc',
                global_config['giraph_page_rank_class'] + '\$' + global_config['giraph_page_rank_master_compute'],
                '-aw',
                global_config['giraph_page_rank_class'] + '\$' + global_config['giraph_page_rank_aggregator'],
                '-vif',
                global_config['giraph_page_rank_input_format'],
                '-vof',
                global_config['giraph_page_rank_output_format'],
                '-op',
                output_path,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_page_rank_max_supersteps'] + max_supersteps,
                global_config['giraph_param_page_rank_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_page_rank_reset_probability'] + reset_probability,
                global_config['giraph_param_page_rank_convergence_output_interval'] + convergence_output_interval]


    def avg_path_len(
            self,
            input_edge_label,
            output_vertex_property_list
            ):
        """
        Average path length calculation:

        Parameters
        ----------
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.

        Returns
        algorithm results in titan table
         -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/apl'
        apl_command = self.get_apl_command(
            self._table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path
        )
        apl_cmd = ' '.join(map(str, apl_command))
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(apl_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report = {'graph_name': self._graph.user_graph_name,
                  'time_run': time_str
                  }
        self._graph.report = report
        return self._graph

    def get_apl_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path
            ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_average_path_length_class'],
                '-vif',
                global_config['giraph_average_path_length_input_format'],
                '-vof',
                global_config['giraph_average_path_length_output_format'],
                '-op',
                output_path,                '-w',
                global_config['giraph_workers']]

    def label_prop(
        self,
        input_vertex_property_list,
        input_edge_property_list,
        input_edge_label,
        output_vertex_property_list,
        max_supersteps=global_config['giraph_label_propagation_max_supersteps'],
        convergence_threshold=global_config['giraph_label_propagation_convergence_threshold'],
        lp_lambda=global_config['giraph_label_propagation_lambda'],
        anchor_threshold=global_config['giraph_label_propagation_anchor_threshold']
        ):
        """
        Label Propagation on Gaussian Random Fields
        The algorithm presented in:
          X. Zhu and Z. Ghahramani. Learning from labeled and unlabeled data with
          label propagation. Technical Report CMU-CALD-02-107, CMU, 2002.

        Parameters
        ----------
        input_vertex_property_list : vertex properties which contains prior vertex value
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        input_edge_property_list: edge properties which contains input edge value
                                  if more than one edge property is used,
                                  expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        max_supersteps : number of super steps to run in Giraph
        lambda: radeoff parameter: f = (1-lambda)Pf + lambda*h
        convergence_threshold: the convergence threshold
        anchor_threshold: the anchor threshold [0, 1].
                          Vertices whose normalized prior values are greater than
                          this threshold will not be updated

        Returns
        algorithms results in titan table
        -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/lp'
        lp_command = self.get_lp_command(
            self._table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            max_supersteps,
            convergence_threshold,
            lp_lambda,
            anchor_threshold,
            output_path
        )
        lp_cmd = ' '.join(map(str, lp_command))
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(lp_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'lambda': lp_lambda,
               'anchor_threshold': anchor_threshold
               }
        self._graph.report = report
        return self._graph

    def get_lp_command(
            self,
            table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            max_supersteps,
            convergence_threshold,
            lp_lambda,
            anchor_threshold,
            output_path
            ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_vertex_property_list'] + global_config['hbase_column_family'] +
                input_vertex_property_list,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_label_propagation_class'],
                '-vif',
                global_config['giraph_label_propagation_input_format'],
                '-vof',
                global_config['giraph_label_propagation_output_format'],
                '-op',
                output_path,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_label_propagation_max_supersteps'] + max_supersteps,
                global_config['giraph_param_label_propagation_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_label_propagation_lambda'] + lp_lambda,
                global_config['giraph_param_label_propagation_anchor_threshold'] + anchor_threshold]

    def lda(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            max_supersteps=global_config['giraph_latent_dirichlet_allocation_max_supersteps'],
            alpha=global_config['giraph_latent_dirichlet_allocation_alpha'],
            beta=global_config['giraph_latent_dirichlet_allocation_beta'],
            convergence_threshold=global_config['giraph_latent_dirichlet_allocation_convergence_threshold'],
            evaluate_cost=global_config['giraph_latent_dirichlet_allocation_evaluate_cost'],
            max_val=global_config['giraph_latent_dirichlet_allocation_maxVal'],
            min_val=global_config['giraph_latent_dirichlet_allocation_minVal'],
            num_topics=global_config['giraph_latent_dirichlet_allocation_num_topics']
            ):
        """
        Latent Dirichlet Allocation, http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation

        Parameters
        ----------
        input_edge_property_list: edge properties which contains input edge value.
                                   if more than one edge property is used,
                                   expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        vertex_type: vertex type
        edge_type: edge type
        max_supersteps : number of super steps to run in Giraph
        alpha: document-topic smoothing parameter
        beta: term-topic smoothing parameter
        convergence_threshold: the convergence threshold
        evaluate_cost: turning on/off cost evaluation
        max_val: maximum edge weight value
        min_val: minimum edge weight value
        num_topics: number of topics to identify

        Returns
        algorithm results in titan table
        Convergence curve is accessible through lda.stats object
        -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/lda'
        lda_command = self.get_lda_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            max_supersteps,
            alpha,
            beta,
            convergence_threshold,
            evaluate_cost,
            max_val,
            min_val,
            num_topics,
            output_path
        )
        lda_cmd = ' '.join(map(str, lda_command))
        print lda_cmd
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(lda_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report_file = self.get_report(output_path, 'lda-learning-report_0', time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        #r = re.compiler(r'superstep')
        data_x = []
        data_y = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])

        curve_title = 'LDA Learning Curve'
        if evaluate_cost:
            curve_ylabel = 'Cost'
        else:
            curve_ylabel = 'Max Vertex Value Change'
        self.plot_progress_curve(data_x, data_y, curve_title, curve_ylabel)
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'alpha': alpha,
               'beta': beta,
               'evaluate_cost': evaluate_cost,
               'max_val': max_val,
               'min_val': min_val,
               'num_topics': num_topics,
               'supersteps': data_x,
               'cost': data_y
               }
        self._graph.report = report
        return self._graph

    def get_lda_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            max_supersteps,
            alpha,
            beta,
            convergence_threshold,
            evaluate_cost,
            max_val,
            min_val,
            num_topics,
            output_path
           ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_vertex_type'] + global_config['hbase_column_family'] +
                vertex_type,
                global_config['giraph_latent_dirichlet_allocation_class'],
                '-mc',
                global_config['giraph_latent_dirichlet_allocation_class'] + '\$' + global_config['giraph_latent_dirichlet_allocation_master_compute'],
                '-aw',
                global_config['giraph_latent_dirichlet_allocation_class'] + '\$' + global_config['giraph_latent_dirichlet_allocation_aggregator'],
                '-vif',
                global_config['giraph_latent_dirichlet_allocation_input_format'],
                '-vof',
                global_config['giraph_latent_dirichlet_allocation_output_format'],
                '-op',
                output_path,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_latent_dirichlet_allocation_max_supersteps'] + max_supersteps,
                global_config['giraph_param_latent_dirichlet_allocation_alpha'] + alpha,
                global_config['giraph_param_latent_dirichlet_allocation_beta'] + beta,
                global_config['giraph_param_latent_dirichlet_allocation_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_latent_dirichlet_allocation_evaluate_cost'] + evaluate_cost,
                global_config['giraph_param_latent_dirichlet_allocation_maxVal'] + max_val,
                global_config['giraph_param_latent_dirichlet_allocation_minVal'] + min_val,
                global_config['giraph_param_latent_dirichlet_allocation_num_topics'] + num_topics]


    def als(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps=global_config['giraph_alternative_least_square_max_supersteps'],
            feature_dimension=global_config['giraph_alternative_least_square_feature_dimension'],
            als_lambda=global_config['giraph_alternative_least_square_lambda'],
            convergence_threshold=global_config['giraph_alternative_least_square_convergence_threshold'],
            learning_output_interval=global_config['giraph_alternative_least_square_learning_output_interval'],
            max_val=global_config['giraph_alternative_least_square_maxVal'],
            min_val=global_config['giraph_alternative_least_square_minVal'],
            bias_on=global_config['giraph_alternative_least_square_bias_on']
            ):
        """
        Alternating Least Squares with Bias for collaborative filtering
        The algorithms presented in
        (1) Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan. Large-Scale
            Parallel Collaborative Filtering for the Netflix Prize. 2008.
        (2) Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
            Filtering Model. In ACM KDD 2008. (Equation 5)

        Parameters
        ----------
        input_edge_property_list: edge properties which contains input edge value.
                                   if more than one edge property is used,
                                   expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        vertex_type: vertex type
        edge_type: edge type
        max_supersteps : number of super steps to run in Giraph
        feature_dimension: feature dimension
        als_lambda: regularization parameter, f = L2_error + lambda*Tikhonov_regularization
        convergence_threshold: the convergence threshold
        learning_output_interval: learning curve output interval (default: every iteration)
                                  Since each ALS iteration is composed by 2 super steps,
                                  one iteration means two super steps.
        max_val: maximum edge weight value
        min_val: minimum edge weight value
        bias_on: turn on/off bias

        Returns
        algorithm results in titan table
        Convergence curve is accessible through als.stats object
        -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/als'
        als_command = self.get_als_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps,
            feature_dimension,
            als_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            output_path
        )
        als_cmd = ' '.join(map(str, als_command))
        print als_cmd
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(als_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report_file = self.get_report(output_path, 'als-learning-report_0', time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        data_x = []
        data_y = []
        data_v = []
        data_t = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])
                data_v.append(results[8])
                data_t.append(results[11])

        curve_title = "ALS Learning Curve"
        #curve_ylabel = 'Cost (RMSE)'
        self.plot_learning_curve(data_x, data_y, data_v, data_t, curve_title)
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'feature_dimension': feature_dimension,
               'lambda': als_lambda,
               'learning_output_interval': learning_output_interval,
               'max_val': max_val,
               'min_val': min_val,
               'bias_on': bias_on,
               'supersteps': data_x,
               'cost_train': data_y,
               'rmse_validate': data_v,
               'rmse_test': data_t
               }
        self._graph.report = report
        return self._graph

    def get_als_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps,
            feature_dimension,
            als_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            output_path
           ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_output_bias'] + bias_on,
                global_config['giraph_param_vertex_type'] + global_config['hbase_column_family'] +
                vertex_type,
                global_config['giraph_param_edge_type'] + global_config['hbase_column_family_edge'] +
                edge_type,
                global_config['giraph_alternative_least_square_class'],
                '-mc',
                global_config['giraph_alternative_least_square_class'] + '\$' + global_config['giraph_alternative_least_square_master_compute'],
                '-aw',
                global_config['giraph_alternative_least_square_class'] + '\$' + global_config['giraph_alternative_least_square_aggregator'],
                '-vif',
                global_config['giraph_alternative_least_square_input_format'],
                '-vof',
                global_config['giraph_alternative_least_square_output_format'],
                '-op',
                output_path,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_alternative_least_square_max_supersteps'] + max_supersteps,
                global_config['giraph_param_alternative_least_square_feature_dimension'] + feature_dimension,
                global_config['giraph_param_alternative_least_square_lambda'] + als_lambda,
                global_config['giraph_param_alternative_least_square_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_alternative_least_square_learning_output_interval'] + learning_output_interval,
                global_config['giraph_param_alternative_least_square_maxVal'] + max_val,
                global_config['giraph_param_alternative_least_square_minVal'] + min_val,
                global_config['giraph_param_alternative_least_square_bias_on'] + bias_on]


    def cgd(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps=global_config['giraph_conjugate_gradient_descent_max_supersteps'],
            feature_dimension=global_config['giraph_conjugate_gradient_descent_feature_dimension'],
            cgd_lambda=global_config['giraph_conjugate_gradient_descent_lambda'],
            convergence_threshold=global_config['giraph_conjugate_gradient_descent_convergence_threshold'],
            learning_output_interval=global_config['giraph_conjugate_gradient_descent_learning_output_interval'],
            max_val=global_config['giraph_conjugate_gradient_descent_maxVal'],
            min_val=global_config['giraph_conjugate_gradient_descent_minVal'],
            bias_on=global_config['giraph_conjugate_gradient_descent_bias_on'],
            num_iters=global_config['giraph_conjugate_gradient_descent_num_iters']
            ):
        """
        Conjugate Gradient Descent (CGD) with Bias for collaborative filtering
        CGD implementation of the algorithm presented in
        Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
            Filtering Model. In ACM KDD 2008. (Equation 5)

        Parameters
        ----------
        input_edge_property_list: edge properties which contains input edge value.
                                   if more than one edge property is used,
                                   expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        vertex_type: vertex type
        edge_type: edge type
        max_supersteps : number of super steps to run in Giraph
        feature_dimension: feature dimension
        cgd_lambda: regularization parameter, f = L2_error + lambda*Tikhonov_regularization
        convergence_threshold: the convergence threshold
        learning_output_interval: learning curve output interval (default: every iteration)
                                  Since each CGD iteration is composed by 2 super steps,
                                  one iteration means two super steps.
        max_val: maximum edge weight value
        min_val: minimum edge weight value
        bias_on: turn on/off bias
        num_iters: number of CGD iterations in each super step

        Returns
        algorithm results in titan table
        Convergence curve is accessible through cgd.stats object
        -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/cgd'
        cgd_command = self.get_cgd_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps,
            feature_dimension,
            cgd_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            num_iters,
            output_path
        )
        cgd_cmd = ' '.join(map(str, cgd_command))
        print cgd_cmd
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(cgd_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report_file = self.get_report(output_path, 'cgd-learning-report_0', time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        data_x = []
        data_y = []
        data_v = []
        data_t = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])
                data_v.append(results[8])
                data_t.append(results[11])

        curve_title = "CGD Learning Curve"
        #curve_ylabel = 'Cost (RMSE)'
        self.plot_learning_curve(data_x, data_y, data_v, data_t, curve_title)
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'feature_dimension': feature_dimension,
               'lambda': cgd_lambda,
               'learning_output_interval': learning_output_interval,
               'max_val': max_val,
               'min_val': min_val,
               'bias_on': bias_on,
               'num_iters': num_iters,
               'supersteps': data_x,
               'cost_train': data_y,
               'rmse_validate': data_v,
               'rmse_test': data_t
               }
        self._graph.report = report
        return self._graph

    def get_cgd_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps,
            feature_dimension,
            cgd_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            num_iters,
            output_path
           ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_output_bias'] + bias_on,
                global_config['giraph_param_vertex_type'] + global_config['hbase_column_family'] +
                vertex_type,
                global_config['giraph_param_edge_type'] + global_config['hbase_column_family_edge'] +
                edge_type,
                global_config['giraph_conjugate_gradient_descent_class'],
                '-mc',
                global_config['giraph_conjugate_gradient_descent_class'] + '\$' + global_config['giraph_conjugate_gradient_descent_master_compute'],
                '-aw',
                global_config['giraph_conjugate_gradient_descent_class'] + '\$' + global_config['giraph_conjugate_gradient_descent_aggregator'],
                '-vif',
                global_config['giraph_conjugate_gradient_descent_input_format'],
                '-vof',
                global_config['giraph_conjugate_gradient_descent_output_format'],
                '-op',
                output_path,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_conjugate_gradient_descent_max_supersteps'] + max_supersteps,
                global_config['giraph_param_conjugate_gradient_descent_feature_dimension'] + feature_dimension,
                global_config['giraph_param_conjugate_gradient_descent_lambda'] + cgd_lambda,
                global_config['giraph_param_conjugate_gradient_descent_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_conjugate_gradient_descent_learning_output_interval'] + learning_output_interval,
                global_config['giraph_param_conjugate_gradient_descent_maxVal'] + max_val,
                global_config['giraph_param_conjugate_gradient_descent_minVal'] + min_val,
                global_config['giraph_param_conjugate_gradient_descent_bias_on'] + bias_on,
                global_config['giraph_conjugate_gradient_descent_num_iters'] + num_iters
                ]

    def gd(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps=global_config['giraph_gradient_descent_max_supersteps'],
            feature_dimension=global_config['giraph_gradient_descent_feature_dimension'],
            gd_lambda=global_config['giraph_gradient_descent_lambda'],
            convergence_threshold=global_config['giraph_gradient_descent_convergence_threshold'],
            learning_output_interval=global_config['giraph_gradient_descent_learning_output_interval'],
            max_val=global_config['giraph_gradient_descent_maxVal'],
            min_val=global_config['giraph_gradient_descent_minVal'],
            bias_on=global_config['giraph_gradient_descent_bias_on'],
            discount=global_config['giraph_gradient_descent_discount'],
            learning_rate=global_config['giraph_gradient_descent_learning_rate']
            ):
        """
        Gradient Descent (GD) with Bias for collaborative filtering
        The algorithm presented in
        Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
        Filtering Model. In ACM KDD 2008. (Equation 5)

        Parameters
        ----------
        input_edge_property_list: edge properties which contains input edge value.
                                   if more than one edge property is used,
                                   expect it is a comma separated string list.
        input_edge_label: edge label
        output_vertex_property_list: vertex properties which contains output vertex value.
                                     if more than one vertex property is used,
                                     expect it is a comma separated string list.
        vertex_type: vertex type
        edge_type: edge type
        max_supersteps : number of super steps to run in Giraph
        feature_dimension: feature dimension
        cgd_lambda: regularization parameter, f = L2_error + lambda*Tikhonov_regularization
        convergence_threshold: the convergence threshold
        learning_output_interval: learning curve output interval (default: every iteration)
                                  Since each GD iteration is composed by 2 super steps,
                                  one iteration means two super steps.
        max_val: maximum edge weight value
        min_val: minimum edge weight value
        bias_on: turn on/off bias
        discount: discount ratio on learning factor
                  learningRate(i+1) = discount * learningRate(i)
                  where discount should be in the range of (0, 1].
        learning_rate: learning rate

        Returns
        algorithm results in titan table
        Convergence curve is accessible through gd.stats object
        -------
        """
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/gd'
        gd_command = self.get_gd_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps,
            feature_dimension,
            gd_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            discount,
            learning_rate,
            output_path
        )
        gd_cmd = ' '.join(map(str, gd_command))
        #delete old output directory if already there
        self.del_old_output(output_path)
        time_str = get_time_str()
        call(gd_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        report_file = self.get_report(output_path, 'gd-learning-report_0', time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        data_x = []
        data_y = []
        data_v = []
        data_t = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])
                data_v.append(results[8])
                data_t.append(results[11])

        curve_title = "GD Learning Curve"
        #curve_ylabel = 'Cost (RMSE)'
        self.plot_learning_curve(data_x, data_y, data_v, data_t, curve_title)
        report = {'graph_name': self._graph.user_graph_name,
               'time_run': time_str,
               'max_superstep': max_supersteps,
               'convergence_threshold': convergence_threshold,
               'feature_dimension': feature_dimension,
               'lambda': gd_lambda,
               'learning_output_interval': learning_output_interval,
               'max_val': max_val,
               'min_val': min_val,
               'bias_on': bias_on,
               'discount': discount,
               'learning_rate': learning_rate,
               'supersteps': data_x,
               'cost_train': data_y,
               'rmse_validate': data_v,
               'rmse_test': data_t
               }
        self._graph.report = report
        return self._graph

    def get_gd_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            max_supersteps,
            feature_dimension,
            gd_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            discount,
            learning_rate,
            output_path
           ):

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_storage_backend'] + global_config['titan_storage_backend'],
                global_config['giraph_param_storage_hostname'] + global_config['titan_storage_hostname'],
                global_config['giraph_param_storage_port'] + global_config['titan_storage_port'],
                global_config['giraph_param_storage_connection_timeout'] +
                global_config['titan_storage_connection_timeout'],
                global_config['giraph_param_storage_tablename'] + table_name,
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family_edge'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_output_bias'] + bias_on,
                global_config['giraph_param_vertex_type'] + global_config['hbase_column_family'] +
                vertex_type,
                global_config['giraph_param_edge_type'] + global_config['hbase_column_family_edge'] +
                edge_type,
                global_config['giraph_gradient_descent_class'],
                '-mc',
                global_config['giraph_gradient_descent_class'] + '\$' + global_config['giraph_gradient_descent_master_compute'],
                '-aw',
                global_config['giraph_gradient_descent_class'] + '\$' + global_config['giraph_gradient_descent_aggregator'],
                '-vif',
                global_config['giraph_gradient_descent_input_format'],
                '-vof',
                global_config['giraph_gradient_descent_output_format'],
                '-op',
                output_path,
                '-w',
                global_config['giraph_workers'],
                global_config['giraph_param_gradient_descent_max_supersteps'] + max_supersteps,
                global_config['giraph_param_gradient_descent_feature_dimension'] + feature_dimension,
                global_config['giraph_param_gradient_descent_lambda'] + gd_lambda,
                global_config['giraph_param_gradient_descent_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_gradient_descent_learning_output_interval'] + learning_output_interval,
                global_config['giraph_param_gradient_descent_maxVal'] + max_val,
                global_config['giraph_param_gradient_descent_minVal'] + min_val,
                global_config['giraph_param_gradient_descent_bias_on'] + bias_on,
                global_config['giraph_param_gradient_descent_discount'] + discount,
                global_config['giraph_param_gradient_descent_learning_rate'] + learning_rate
                ]


class BeliefPropagation(object):  #TODO: eventually inherit from base Algo class
    """
    Descriptive class of Belief Propagation algorithm.
	
	Should contain info about the cfg constants, the user supplied parameters
    and what the results are --can also define a specific
    BeliefPropagationResults class if necessary.
    """
    pass


job_completion_pattern = re.compile(r".*?mapred.JobClient: Job complete")


class GiraphProgressReportStrategy(ProgressReportStrategy):

    def report(self, line):
        progress = find_progress(line)

        if progress and len(self.progress_list) < 2:
            if len(self.progress_list) == 0:
                self.initialization_progressbar._disable_animation()
                self.progress_list.append(MapReduceProgress(0, 0))
                self.job_progress_bar_list.append(self.get_new_progress_bar(self.get_next_step_title()))

            # giraph is a mapper only job
            progressGiraph = MapReduceProgress(progress.mapper_progress, 0)
            self.job_progress_bar_list[-1].update(progressGiraph.mapper_progress)
            progressGiraph.total_progress = progressGiraph.mapper_progress
            self.progress_list[-1] = progressGiraph

            # mapper job finishes, create second progress bar automatically since
            # giraph does not print any message indicating beginning of the second phase
            if progress.mapper_progress == 100:
                self.job_progress_bar_list.append(self.get_new_progress_bar(self.get_next_step_title()))
                self.job_progress_bar_list[-1].update(100)
                self.job_progress_bar_list[-1]._enable_animation()
                self.progress_list.append(MapReduceProgress(0, 0))

        if self._is_computation_complete(line):
            self.progress_list[-1] = MapReduceProgress(100, 100)
            self.job_progress_bar_list[-1]._disable_animation()

    def _is_computation_complete(self, line):
        match = re.match(job_completion_pattern, line)
        if match:
            return True
        else:
            return False

