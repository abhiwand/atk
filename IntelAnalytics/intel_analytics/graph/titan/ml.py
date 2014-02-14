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
Methods and classes for Graph Machine Learning.
"""
# (Titan, Giraph)-based

__all__ = [
    'TitanGiraphMachineLearning',
    'AlgorithmReport'
]

if __name__ != '__main__':
    #if this is executing through a test runner on the build server the DISPLAY environment variable will not be set.
    #This will require that we use the Agg  backend in matplotlib so that it can be done in a non-interactive manner.
    import os
    if os.getenv('IN_UNIT_TESTS') and not os.getenv("DISPLAY"):
        import matplotlib
        matplotlib.use("Agg",warn=False)


import matplotlib.pyplot as plt
import re
import time

from intel_analytics.subproc import call
from intel_analytics.config import global_config, get_time_str
from intel_analytics.report import ProgressReportStrategy, find_progress, \
    MapReduceProgress, ReportStrategy
from intel_analytics.progress import Progress


class TitanGiraphMachineLearning(object): # TODO: >0.5, inherit MachineLearning
    """
    Titan-based Giraph Machine Learning instance for a graph
    """

    def __init__(self, graph):
        """
        initialize the global variables in TitanGiraphMachineLearning
        """
        self._graph = graph
        self._table_name = graph.titan_table_name
        self._output_vertex_property_list = ''
        self._vertex_type = ''
        self._edge_type = ''
        self._vector_value=''
        self._bias_on = ''
        self._feature_dimension = ''
        self.report = []
        self._label_font_size = 12
        self._title_font_size = 14

    def _plot_progress_curve(self,
                            data_x,
                            data_y,
                            curve_title,
                            curve_ylabel):
        """
        Plots progress curves for algorithms.
        """
        fig, axes = plt.subplots()
        axes.plot(data_x, data_y, 'b')

        axes.set_title(curve_title, fontsize=self._title_font_size)
        axes.set_xlabel("Number of SuperStep", fontsize=self._label_font_size)
        axes.set_ylabel(curve_ylabel, fontsize=self._label_font_size)
        axes.grid(True, linestyle='-', color='0.75')

    def _plot_learning_curve(self,
                            data_x,
                            data_y,
                            data_v,
                            data_t,
                            curve_title,
                            curve_ylabel1,
                            curve_ylabel2,
                            curve_ylabel3
                            ):
        """
        Plots learning curves for algorithms.
        """
        fig = plt.figure()
        axes1 = fig.add_axes([0.1, 0.1, 0.8, 0.8])  # left,bottom,width,height
        axes1.plot(data_x, data_y, 'b')
        axes1.set_xlabel("Number of SuperStep", fontsize=self._label_font_size)
        axes1.set_ylabel(curve_ylabel1, fontsize=self._label_font_size)
        title_str = [curve_title, " (Train)"]
        axes1.set_title(' '.join(map(str, title_str)), fontsize=self._title_font_size)
        axes1.grid(True, linestyle='-', color='0.75')

        axes2 = fig.add_axes([1.1, 0.1, 0.8, 0.8])
        axes2.plot(data_x, data_v, 'g')
        axes2.set_xlabel("Number of SuperStep", fontsize=self._label_font_size)
        axes2.set_ylabel(curve_ylabel2, fontsize=self._label_font_size)
        title_str = [curve_title, " (Validate)"]
        axes2.set_title(' '.join(map(str, title_str)), fontsize=self._title_font_size)
        axes2.grid(True, linestyle='-', color='0.75')

        axes3 = fig.add_axes([2.1, 0.1, 0.8, 0.8])
        axes3.plot(data_x, data_t, 'y')
        axes3.set_xlabel("Number of SuperStep", fontsize=self._label_font_size)
        axes3.set_ylabel(curve_ylabel3, fontsize=self._label_font_size)
        title_str = [curve_title, " (Test)"]
        axes3.set_title(' '.join(map(str, title_str)), fontsize=self._title_font_size)
        axes3.grid(True, linestyle='-', color='0.75')
        #axes1.legend(['train', 'validate', 'test'], loc='upper right')
        #show()

    def _update_progress_curve(self,
                               output_path,
                               file_name,
                               time_str,
                               curve_title,
                               curve_ylabel):
        report_file = self._get_report(output_path, file_name, time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        data_x = []
        data_y = []
        progress_results = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])
        progress_results.append(data_x)
        progress_results.append(data_y)
        self._plot_progress_curve(data_x, data_y, curve_title, curve_ylabel)
        return progress_results

    def _update_learning_curve(self,
                               output_path,
                               file_name,
                               time_str,
                               curve_title,
                               curve_ylabel1="Cost (Train)",
                               curve_ylabel2="RMSE (Validate)",
                               curve_ylabel3="RMSE (Test)"
                               ):
        report_file = self._get_report(output_path, file_name, time_str)
        #find progress info
        with open(report_file) as result:
            lines = result.readlines()

        data_x = []
        data_y = []
        data_v = []
        data_t = []
        learning_results = []
        for i in range(len(lines)):
            if re.search(r'superstep', lines[i]):
                results = lines[i].split()
                data_x.append(results[2])
                data_y.append(results[5])
                data_v.append(results[8])
                data_t.append(results[11])
        learning_results.append(data_x)
        learning_results.append(data_y)
        learning_results.append(data_v)
        learning_results.append(data_t)
        self._plot_learning_curve(data_x,
                                  data_y,
                                  data_v,
                                  data_t,
                                  curve_title,
                                  curve_ylabel1,
                                  curve_ylabel2,
                                  curve_ylabel3)
        return learning_results

    def _del_old_output(self, output_path):
        """
        Deletes the old output directory if it exists.
        """
        del_cmd = 'if hadoop fs -test -e ' + output_path + \
                  '; then hadoop fs -rmr -skipTrash ' + output_path + '; fi'
        call(del_cmd, shell=True)

    def _get_report(self, output_path, file_name, time_str):
        """
        Gets the learning curve or convergence progress report.
        """
        cmd = 'if [ ! -d ' + global_config['giraph_report_dir'] + ' ]; then mkdir ' + global_config['giraph_report_dir'] + '; fi'
        call(cmd, shell=True)
        report_file = global_config['giraph_report_dir'] + '/' + self._table_name + time_str + '_report.txt'
        cmd = 'hadoop fs -get ' + output_path + '/' + file_name + ' ' + report_file
        call(cmd, shell=True)
        return report_file

    def recommend(self,
                  vertex_id,
                  vertex_type=global_config['giraph_left_vertex_type_str'],
                  output_vertex_property_list='',
                  vector_value='',
                  key_4_vertex_type='',
                  key_4_edge_type='',
                  bias_on='',
                  feature_dimension='',
                  left_vertex_name=global_config['giraph_recommend_left_name'],
                  right_vertex_name=global_config['giraph_recommend_right_name']):
        """
        Make recommendation based on trained model.

        Parameters
        ----------
        vertex_id : String
            vertex id to get recommendation for

        vertex_type : String, optional ("L" or "R")
            vertex type to get recommendation for.
            "L" stands for left-side vertices of a bipartite graph.
            "R" stands for right-side vertices of a bipartite graph.
            The default value is "L"
        output_vertex_property_list : String, optional
            vertex properties which contains output vertex value.
            if more than one vertex property is used,
            expect it is a comma separated string list.
            The default value is the latest vertex_type set by
            algorithm execution.
        vector_value: String, optional
            "true" means supporting a vector as vertex property's value.
            "false" means only support a single value as vertex property's value.
            The default value is "false".
        key_4_vertex_type : String, optional
            The property name for vertex type. The default value is the
            latest vertex_type set by algorithm execution.
        key_4_edge_type : String, optional
            The property name for vertex type. The default value is the
            latest vertex_type set by algorithm execution.
        left_vertex_name : String, optional
            The left-side vertex name. The default value is "user".
        right_vertex_name : String, optional
            The right-side vertex name. The default value is "movie".
        bias_on: String, optional
            Whether to enable bias. The default value is the latest bias_on set by
            algorithm execution

        Returns
        -------
        output : AlgorithmReport
            Top 10 recommendations for the input vertex id
        """
        if output_vertex_property_list == '':
            if self._output_vertex_property_list == '':
                raise ValueError("output_vertex_property_list is empty!")
            else:
                output_vertex_property_list = self._output_vertex_property_list

        if key_4_vertex_type == '':
            if self._vertex_type == '':
                raise ValueError("key_4_vertex_type is empty!")
            else:
                key_4_vertex_type = self._vertex_type

        if key_4_edge_type == '':
            if self._edge_type == '':
                raise ValueError("key_4_edge_type is empty!")
            else:
                key_4_edge_type = self._edge_type

        if vector_value == '':
            if self._vector_value == '':
                raise ValueError("vector_value is empty!")
            else:
                vector_value = self._vector_value

        if bias_on == '':
            if self._bias_on == '':
                raise ValueError("bias_on is empty!")
            else:
                bias_on = self._bias_on

        if feature_dimension == '':
            if self._feature_dimension == '':
                raise ValueError("feature_dimension is empty!")
            else:
                feature_dimension = self._feature_dimension

        rec_cmd1 = 'gremlin.sh -e ' + global_config['giraph_recommend_script']
        rec_command = [self._table_name,
                       vertex_id,
                       output_vertex_property_list,
                       global_config['titan_storage_backend'],
                       global_config['titan_storage_hostname'],
                       global_config['titan_storage_port'],
                       left_vertex_name,
                       right_vertex_name,
                       global_config['giraph_left_vertex_type_str'],
                       global_config['giraph_right_vertex_type_str'],
                       global_config['giraph_train_str'],
                       global_config['giraph_vertex_true_name'],
                       key_4_vertex_type,
                       key_4_edge_type,
                       vertex_type,
                       vector_value,
                       bias_on,
                       feature_dimension]
        rec_cmd2 = '::'.join(map(str, rec_command))
        rec_cmd = rec_cmd1 + ' ' + rec_cmd2
        #print rec_cmd
        #if want to directly use subprocess without progress bar, it is like this:
        #p = subprocess.Popen(rec_cmd, shell=True, stdout=subprocess.PIPE)
        #out = p.communicate()
        time_str = get_time_str()
        start_time = time.time()
        out = call(rec_cmd, shell=True, report_strategy=RecommenderProgressReportStrategy(), return_stdout=1)
        exec_time = time.time() - start_time
        recommend_id = []
        recommend_score = []
        width = 10
        for i in range(len(out)):
            if re.search(r'======', out[i]):
                print out[i]
            if re.search(r'score', out[i]):
                results = out[i].split()
                recommend_id.append(results[1])
                recommend_score.append(results[3])
                print '{0:{width}}'.format(results[0], width=width),
                print '{0:{width}}'.format(results[1], width=width),
                print '{0:{width}}'.format("=>", width=width),
                print '{0:{width}}'.format(results[2], width=width),
                print '{0:{width}}'.format(results[3], width=width),
                print

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.recommend_id = list(recommend_id)
        output.recommend_score = list(recommend_score)
        self.report.append(output)
        return output



    def belief_prop(self,
                    input_vertex_property_list,
                    input_edge_property_list,
                    input_edge_label,
                    output_vertex_property_list,
                    vertex_type,
                    vector_value=global_config['giraph_vector_value'],
                    num_worker=global_config['giraph_workers'],
                    max_supersteps=global_config['giraph_belief_propagation_max_supersteps'],
                    convergence_threshold=global_config['giraph_belief_propagation_convergence_threshold'],
                    smoothing=global_config['giraph_belief_propagation_smoothing'],
                    bidirectional_check=global_config['giraph_belief_propagation_bidirectional_check'],
                    anchor_threshold=global_config['giraph_belief_propagation_anchor_threshold']):
        """
        Loopy belief propagation on Markov Random Fields(MRF).

        This algorithm was originally designed for acyclic graphical models,
        then it was found that the Belief Propagation algorithm can be used
        in general graphs. The algorithm is then sometimes called "loopy"
        belief propagation, because graphs typically contain cycles, or loops.

        In Giraph, we run the algorithm in iterations until it converges.

        Parameters
        ----------
        input_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain prior vertex values if you
            use more than one vertex property.
        input_edge_property_list : List (comma-separated list of strings)
            The edge properties which contain the input edge values if you
            use more than one edge property.
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values if
            you use more than one vertex property.
        vertex_type : String
            The vertex property which contains vertex type.
        vector_value: String, optional
            "true" means a vector as vertex value is supported
            "false" means a vector as vertex value is not supported
        num_worker : String, optional
            The number of Giraph workers.
        max_supersteps : String, optional
            The number of super steps to run in Giraph.
        smoothing : String, optional
            The Ising smoothing parameter.
        convergence_threshold : String, optional
            The convergence threshold which controls how small the change in
            validation error must be in order to meet the convergence criteria.
        bidirectional_check : String, optional
	    If it is true, Giraph will firstly check whether each edge is bidirectional.
            The default value is false.
        anchor_threshold : String, optional
            The anchor threshold [0, 1].
            Those vertices whose normalized prior values are greater than this
            threshold will not be updated.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.
        """
        self._output_vertex_property_list = output_vertex_property_list
        self._vertex_type = global_config['hbase_column_family'] + vertex_type
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/lbp'
        lbp_command = self._get_lbp_command(
            self._table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            self._vertex_type,
            vector_value,
            num_worker,
            max_supersteps,
            convergence_threshold,
            smoothing,
            anchor_threshold,
            bidirectional_check,
            output_path)
        lbp_cmd = ' '.join(map(str, lbp_command))
        #print lbp_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(lbp_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time
        lbp_results = self._update_learning_curve(output_path,
                                                  'lbp-learning-report_0',
                                                  time_str,
                                                  'LBP Learning Curve',
                                                  'Average Train Delta',
                                                  'Average Validation Delta',
                                                  'Average Test Delta')

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.max_superstep = max_supersteps
        output.bidirectional_check = bidirectional_check
        output.convergence_threshold = convergence_threshold
        output.smoothing = smoothing
        output.anchor_threshold = anchor_threshold
        output.super_steps = list(lbp_results[0])
        output.cost_train = list(lbp_results[1])
        output.rmse_validate = list(lbp_results[2])
        output.rmse_test = list(lbp_results[3])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_lbp_command(
            self,
            table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            vector_value,
            num_worker,
            max_supersteps,
            convergence_threshold,
            smoothing,
            anchor_threshold,
            bidirectional_check,
            output):
        """
        generate loopy belief propagation command line
        """

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
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_vertex_type'] + vertex_type,
                global_config['giraph_param_vector_value'] + vector_value,
                global_config['giraph_belief_propagation_class'],
                '-mc',
                global_config['giraph_belief_propagation_class'] + '\$' + global_config['giraph_belief_propagation_master_compute'],
                '-aw',
                global_config['giraph_belief_propagation_class'] + '\$' + global_config['giraph_belief_propagation_aggregator'],
                '-vif',
                global_config['giraph_belief_propagation_input_format'],
                '-vof',
                global_config['giraph_belief_propagation_output_format'],
                '-op',
                output,
                '-w',
                num_worker,
                global_config['giraph_param_belief_propagation_max_supersteps'] + max_supersteps,
                global_config['giraph_param_belief_propagation_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_belief_propagation_smoothing'] + smoothing,
                global_config['giraph_param_belief_propagation_bidirectional_check'] + bidirectional_check,
                global_config['giraph_param_belief_propagation_anchor_threshold'] + anchor_threshold]


    def page_rank(self,
                  input_edge_label,
                  output_vertex_property_list,
                  num_worker=global_config['giraph_workers'],
                  max_supersteps=global_config['giraph_page_rank_max_supersteps'],
                  convergence_threshold=global_config['giraph_page_rank_convergence_threshold'],
                  reset_probability=global_config['giraph_page_rank_reset_probability'],
                  convergence_output_interval=global_config['giraph_convergence_output_interval']):
        """
        The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.

        Parameters
        ----------
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
             The vertex properties which contain the output vertex values
             if you use one vertex property.

        num_worker : String, optional
            The number of workers.
        max_supersteps : String, optional
            The number of super steps to run in Giraph.
        convergence_threshold : String, optional
            The convergence threshold which controls how small the change in
            belief value must be in order to meet the convergence criteria.
        reset_probability : String, optional
            The probability that the random walk of a page is reset.
        convergence_output_interval : String, optional
            The convergence progress output interval
            The default value is 1, which means output every super step.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The progress curve is
            accessible through the report object.
        """
        self._output_vertex_property_list = output_vertex_property_list
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/pr'
        pr_command = self._get_pr_command(
            self._table_name,
            input_edge_label,
            output_vertex_property_list,
            num_worker,
            max_supersteps,
            convergence_threshold,
            reset_probability,
            convergence_output_interval,
            output_path
        )
        pr_cmd = ' '.join(map(str, pr_command))
        #print pr_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(pr_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time
        pr_results = self._update_progress_curve(output_path,
                                                 'pr-convergence-report_0',
                                                 time_str,
                                                 'Page Rank Convergence Curve',
                                                 'Vertex Value Change')
        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.max_superstep = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.reset_probability = reset_probability
        output.convergence_output_interval = convergence_output_interval
        output.super_steps = list(pr_results[0])
        output.convergence_progress = list(pr_results[1])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_pr_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            num_worker,
            max_supersteps,
            convergence_threshold,
            reset_probability,
            convergence_output_interval,
            output_path
    ):
        """
        generate page rank command line
        """

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
                num_worker,
                global_config['giraph_param_page_rank_max_supersteps'] + max_supersteps,
                global_config['giraph_param_page_rank_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_page_rank_reset_probability'] + reset_probability,
                global_config['giraph_param_page_rank_convergence_output_interval'] + convergence_output_interval]


    def avg_path_len(
            self,
            input_edge_label,
            output_vertex_property_list,
            convergence_output_interval=global_config['giraph_convergence_output_interval'],
            num_worker=global_config['giraph_workers']
    ):
        """
        The average path length calculation.

        Parameters
        ----------
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values if
            you use more than one vertex property.

        convergence_output_interval : String, optional
            The convergence progress output interval.
            The default value is 1, which means output every super step.
        num_worker : String, optional
            The number of Giraph workers.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The progress curve is
            accessible through the report object.
        """
        self._output_vertex_property_list = output_vertex_property_list
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/apl'
        apl_command = self._get_apl_command(
            self._table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path,
            num_worker
        )
        apl_cmd = ' '.join(map(str, apl_command))
        #print apl_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(apl_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time
        apl_results = self._update_progress_curve(output_path,
                                                  'apl-convergence-report_0',
                                                  time_str,
                                                  'Avg. Path Length Progress Curve',
                                                  'Num of Vertex Updates')

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.convergence_output_interval = convergence_output_interval
        output.super_steps = list(apl_results[0])
        output.convergence_progress = list(apl_results[1])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_apl_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path,
            num_worker,
    ):
        """
        generate average path length command line
        """

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
                '-mc',
                global_config['giraph_average_path_length_class'] + '\$' + global_config['giraph_average_path_length_master_compute'],
                '-aw',
                global_config['giraph_average_path_length_class'] + '\$' + global_config['giraph_average_path_length_aggregator'],
                '-vif',
                global_config['giraph_average_path_length_input_format'],
                '-vof',
                global_config['giraph_average_path_length_output_format'],
                '-op',
                output_path,
                '-w',
                num_worker]



    def connected_components(
            self,
            input_edge_label,
            output_vertex_property_list,
            convergence_output_interval=global_config['giraph_convergence_output_interval'],
            num_worker=global_config['giraph_workers']
    ):
        """
        The connected components computation.

        Parameters
        ----------
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values if
            you use more than one vertex property.

        convergence_output_interval : String, optional
            The convergence progress output interval.
            The default value is 1, which means output every super step.
        num_worker : String, optional
            The number of Giraph workers.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The progress curve is
            accessible through the report object.
        """
        self._output_vertex_property_list = output_vertex_property_list
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/cc'
        cc_command = self._get_cc_command(
            self._table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path,
            num_worker
        )
        cc_cmd = ' '.join(map(str, cc_command))
        #print cc_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(cc_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time
        cc_results = self._update_progress_curve(output_path,
                                                  'cc-convergence-report_0',
                                                  time_str,
                                                  'Connected Components Progress Curve',
                                                  'Num of Vertex Updates')

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.convergence_output_interval = convergence_output_interval
        output.super_steps = list(cc_results[0])
        output.convergence_progress = list(cc_results[1])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_cc_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path,
            num_worker,
            ):
        """
        generate connected component command line
        """

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
                global_config['giraph_connected_components_class'],
                '-mc',
                global_config['giraph_connected_components_class'] + '\$' + global_config['giraph_connected_components_master_compute'],
                '-aw',
                global_config['giraph_connected_components_class'] + '\$' + global_config['giraph_connected_components_aggregator'],
                '-vif',
                global_config['giraph_connected_components_input_format'],
                '-vof',
                global_config['giraph_connected_components_output_format'],
                '-op',
                output_path,
                '-w',
                num_worker]


    def label_prop(
            self,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vector_value=global_config['giraph_vector_value'],
            num_worker=global_config['giraph_workers'],
            max_supersteps=global_config['giraph_label_propagation_max_supersteps'],
            convergence_threshold=global_config['giraph_label_propagation_convergence_threshold'],
            lp_lambda=global_config['giraph_label_propagation_lambda'],
            bidirectional_check=global_config['giraph_label_propagation_bidirectional_check'],
            anchor_threshold=global_config['giraph_label_propagation_anchor_threshold']
    ):
        """
        Label Propagation on Gaussian Random Fields.

        This algorithm is presented in
        X. Zhu and Z. Ghahramani. Learning from labeled and unlabeled data with
        label propagation. Technical Report CMU-CALD-02-107, CMU, 2002.

        Parameters
        ----------

        num_worker : String, optional
            The number of Giraph workers.
        max_supersteps : String, optional
            The number of super steps to run in Giraph.
        lambda : String, optional
            The tradeoff parameter: f = (1-lambda)Pf + lambda*h
        convergence_threshold : String, optional
            The convergence threshold which controls how small the change in belief value must be
            in order to meet the convergence criteria.
        bidirectional_check : String, optional
            If it is true, Giraph will firstly check whether each edge is bidirectional.
        anchor_threshold : String, optional
            The anchor threshold [0, 1].
            Those vertices whose normalized prior values are greater than this
            threshold will not be updated.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.
        """
        self._output_vertex_property_list = output_vertex_property_list
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/lp'
        lp_command = self._get_lp_command(
            self._table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vector_value,
            num_worker,
            max_supersteps,
            convergence_threshold,
            lp_lambda,
            anchor_threshold,
            bidirectional_check,
            output_path
        )
        lp_cmd = ' '.join(map(str,lp_command))
        #print lp_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(lp_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time
        lp_results = self._update_progress_curve(output_path,
                                                 'lp-learning-report_0',
                                                 time_str,
                                                 'LP Learning Curve',
                                                 'Cost')

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.max_superstep = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.param_lambda = lp_lambda
        output.bidirectional_check = bidirectional_check
        output.anchor_threshold = anchor_threshold
        output.super_steps = list(lp_results[0])
        output.cost = list(lp_results[1])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_lp_command(
            self,
            table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vector_value,
            num_worker,
            max_supersteps,
            convergence_threshold,
            lp_lambda,
            anchor_threshold,
            bidirectional_check,
            output_path
    ):
        """
        generate label propagation command line
        """

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
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_vector_value'] + vector_value,
                global_config['giraph_label_propagation_class'],
                '-mc',
                global_config['giraph_label_propagation_class'] + '\$' + global_config['giraph_label_propagation_master_compute'],
                '-aw',
                global_config['giraph_label_propagation_class'] + '\$' + global_config['giraph_label_propagation_aggregator'],
                '-vif',
                global_config['giraph_label_propagation_input_format'],
                '-vof',
                global_config['giraph_label_propagation_output_format'],
                '-op',
                output_path,
                '-w',
                num_worker,
                global_config['giraph_param_label_propagation_max_supersteps'] + max_supersteps,
                global_config['giraph_param_label_propagation_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_label_propagation_lambda'] + lp_lambda,
                global_config['giraph_param_label_propagation_bidirectional_check'] + bidirectional_check,
                global_config['giraph_param_label_propagation_anchor_threshold'] + anchor_threshold]

    def lda(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            vector_value=global_config['giraph_vector_value'],
            num_worker=global_config['giraph_workers'],
            max_supersteps=global_config['giraph_latent_dirichlet_allocation_max_supersteps'],
            alpha=global_config['giraph_latent_dirichlet_allocation_alpha'],
            beta=global_config['giraph_latent_dirichlet_allocation_beta'],
            convergence_threshold=global_config['giraph_latent_dirichlet_allocation_convergence_threshold'],
            evaluate_cost=global_config['giraph_latent_dirichlet_allocation_evaluate_cost'],
            max_val=global_config['giraph_latent_dirichlet_allocation_maxVal'],
            min_val=global_config['giraph_latent_dirichlet_allocation_minVal'],
            bidirectional_check=global_config['giraph_latent_dirichlet_allocation_bidirectional_check'],
            num_topics=global_config['giraph_latent_dirichlet_allocation_num_topics']
    ):
        """
        The `Latent Dirichlet Allocation <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`_.

        Parameters
        ----------
        input_edge_property_list : List (comma-separated list of strings)
            The edge properties which contain the input edge values if you use
            more than one edge property.
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values if
            you use more than one vertex property.
        vertex_type : String
            The vertex property which contains vertex type.

        vector_value: String, optional
            "true" means a vector as vertex value is supported
            "false" means a vector as vertex value is not supported
        num_worker : String, optional
            The number of Giraph workers.
        max_supersteps : String, optional
            The number of super steps to run in Giraph.
        alpha : String, optional
            The document-topic smoothing parameter.
        beta : String, optional
            The term-topic smoothing parameter.
        convergence_threshold : String, optional
            The convergence threshold which controls how small the change in edge value must be
            in order to meet the convergence criteria.
        evaluate_cost : String, optional
            True means turn on cost evaluation and False means turn off
            cost evaluation.
        max_val : String, optional
            The maximum edge weight value.
            The default value is Float.POSITIVE_INFINITY.
        min_val : String, optional
            The minimum edge weight value.
            The default value is Float.NEGATIVE_INFINITY.
        num_topics : String, optional
            The number of topics to identify.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The convergence curve is
            accessible through the report object.
        """
        self._output_vertex_property_list = output_vertex_property_list
        self._vertex_type = global_config['hbase_column_family'] + vertex_type
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/lda'
        lda_command = self._get_lda_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            self._vertex_type,
            vector_value,
            num_worker,
            max_supersteps,
            alpha,
            beta,
            convergence_threshold,
            evaluate_cost,
            max_val,
            min_val,
            num_topics,
            bidirectional_check,
            output_path
        )
        lda_cmd = ' '.join(map(str, lda_command))
        #print lda_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(lda_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time

        if evaluate_cost:
            curve_ylabel = 'Cost'
        else:
            curve_ylabel = 'Max Vertex Value Change'
        lda_results = self._update_progress_curve(output_path,
                                                  'lda-learning-report_0',
                                                  time_str,
                                                  'LDA Learning Curve',
                                                  curve_ylabel)

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.max_superstep = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.alpha = alpha
        output.beta = beta
        output.evaluate_cost = evaluate_cost
        output.max_val = max_val
        output.min_val = min_val
        output.num_topics = num_topics
        output.super_steps = list(lda_results[0])
        output.cost = list(lda_results[1])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_lda_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            vector_value,
            num_worker,
            max_supersteps,
            alpha,
            beta,
            convergence_threshold,
            evaluate_cost,
            max_val,
            min_val,
            num_topics,
            bidirectional_check,
            output_path
    ):
        """
        generate latent Dirichlet allocation command line
        """

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
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_vector_value'] + vector_value,
                global_config['giraph_param_vertex_type'] + vertex_type,
                global_config['giraph_latent_dirichlet_allocation_class'],
                '-mc',
                global_config['giraph_latent_dirichlet_allocation_class'] + '\$' + global_config[
                    'giraph_latent_dirichlet_allocation_master_compute'],
                '-aw',
                global_config['giraph_latent_dirichlet_allocation_class'] + '\$' + global_config[
                    'giraph_latent_dirichlet_allocation_aggregator'],
                '-vif',
                global_config['giraph_latent_dirichlet_allocation_input_format'],
                '-vof',
                global_config['giraph_latent_dirichlet_allocation_output_format'],
                '-op',
                output_path,
                '-w',
                num_worker,
                global_config['giraph_param_latent_dirichlet_allocation_max_supersteps'] + max_supersteps,
                global_config['giraph_param_latent_dirichlet_allocation_alpha'] + alpha,
                global_config['giraph_param_latent_dirichlet_allocation_beta'] + beta,
                global_config['giraph_param_latent_dirichlet_allocation_convergence_threshold'] + convergence_threshold,
                global_config['giraph_param_latent_dirichlet_allocation_evaluate_cost'] + evaluate_cost,
                global_config['giraph_param_latent_dirichlet_allocation_maxVal'] + max_val,
                global_config['giraph_param_latent_dirichlet_allocation_minVal'] + min_val,
                global_config['giraph_param_latent_dirichlet_allocation_bidirectional_check'] + bidirectional_check,
                global_config['giraph_param_latent_dirichlet_allocation_num_topics'] + num_topics]


    def als(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            vector_value=global_config['giraph_vector_value'],
            num_worker=global_config['giraph_workers'],
            max_supersteps=global_config['giraph_alternating_least_square_max_supersteps'],
            feature_dimension=global_config['giraph_alternating_least_square_feature_dimension'],
            als_lambda=global_config['giraph_alternating_least_square_lambda'],
            convergence_threshold=global_config['giraph_alternating_least_square_convergence_threshold'],
            learning_output_interval=global_config['giraph_learning_output_interval'],
            max_val=global_config['giraph_alternating_least_square_maxVal'],
            min_val=global_config['giraph_alternating_least_square_minVal'],
            bidirectional_check=global_config['giraph_alternating_least_square_bidirectional_check'],
            bias_on=global_config['giraph_alternating_least_square_bias_on']
    ):
        """
        The Alternating Least Squares with Bias for collaborative filtering algorithms.

        The algorithms presented in

        1. Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan. Large-Scale
           Parallel Collaborative Filtering for the Netflix Prize. 2008.
        2. Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
           Filtering Model. In ACM KDD 2008. (Equation 5)

        Parameters
        ----------
        input_edge_property_list : List (comma-separated list of strings)
            The edge properties which contain the input edge values if you use
            more than one edge property.
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values if
            you use more than one vertex property.
        vertex_type : String
            The vertex property which contains vertex type.
        edge_type : String
            The edge property which contains edge type.
        vector_value: String, optional
            "true" means a vector as vertex value is supported
            "false" means a vector as vertex value is not supported
        num_worker : String, optional
            The number of Giraph workers.
        max_supersteps : String, optional
            The number of super steps to run in Giraph.
        feature_dimension : String, optional
            The feature dimension.
        als_lambda : String, optional
            The regularization parameter: f = L2_error + lambda*Tikhonov_regularization
        convergence_threshold : String, optional
            The convergence threshold which controls how small the change in validation error must be
            in order to meet the convergence criteria.
        learning_output_interval : String, optional
            The learning curve output interval.
            Since each ALS iteration is composed by 2 super steps,
            the default one iteration means two super steps.
        max_val : String, optional
            The maximum edge weight value.
            The default value is Float.POSITIVE_INFINITY.
        min_val : String, optional
            The minimum edge weight value.
            The default value is Float.NEGATIVE_INFINITY.
        bidirectional_check : String, optional
            If it is "true", Giraph will firstly check whether each edge is bidirectional.
        bias_on : String, optional
            True means turn on bias calculation and False means turn off
            bias calculation.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The convergence curve is
            accessible through the report object.
        """
        self._output_vertex_property_list = output_vertex_property_list
        self._vertex_type = global_config['hbase_column_family'] + vertex_type
        self._edge_type = global_config['hbase_column_family'] + edge_type
        self._vector_value = vector_value
        self._bias_on = bias_on
        self._feature_dimension = feature_dimension
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/als'
        als_command = self._get_als_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            self._vertex_type,
            self._edge_type,
            vector_value,
            num_worker,
            max_supersteps,
            feature_dimension,
            als_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            bidirectional_check,
            output_path
        )
        als_cmd = ' '.join(map(str,als_command))
        #print als_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(als_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time
        als_results = self._update_learning_curve(output_path,
                                                  'als-learning-report_0',
                                                  time_str,
                                                  'ALS Learning Curve')

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.max_superstep = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.feature_dimension = feature_dimension
        output.param_lambda = als_lambda
        output.learning_output_interval = learning_output_interval
        output.max_val = max_val
        output.min_val = min_val
        output.bias_on = bias_on
        output.super_steps = list(als_results[0])
        output.cost_train = list(als_results[1])
        output.rmse_validate = list(als_results[2])
        output.rmse_test = list(als_results[3])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_als_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            vector_value,
            num_worker,
            max_supersteps,
            feature_dimension,
            als_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            bidirectional_check,
            output_path
    ):
        """
        generate alternating least squares command line
        """

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
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_vector_value'] + vector_value,
                global_config['giraph_param_output_bias'] + bias_on,
                global_config['giraph_param_vertex_type'] + vertex_type,
                global_config['giraph_param_edge_type'] + edge_type,
                global_config['giraph_alternating_least_square_class'],
                '-mc',
                global_config['giraph_alternating_least_square_class'] + '\$' + global_config[
                    'giraph_alternating_least_square_master_compute'],
                '-aw',
                global_config['giraph_alternating_least_square_class'] + '\$' + global_config[
                    'giraph_alternating_least_square_aggregator'],
                '-vif',
                global_config['giraph_alternating_least_square_input_format'],
                '-vof',
                global_config['giraph_alternating_least_square_output_format'],
                '-op',
                output_path,
                '-w',
                num_worker,
                global_config['giraph_param_alternating_least_square_max_supersteps'] + max_supersteps,
                global_config['giraph_param_alternating_least_square_feature_dimension'] + feature_dimension,
                global_config['giraph_param_alternating_least_square_lambda'] + als_lambda,
                global_config['giraph_param_alternating_least_square_convergence_threshold'] + convergence_threshold,
                global_config[
                    'giraph_param_alternating_least_square_learning_output_interval'] + learning_output_interval,
                global_config['giraph_param_alternating_least_square_maxVal'] + max_val,
                global_config['giraph_param_alternating_least_square_minVal'] + min_val,
                global_config['giraph_param_alternating_least_square_bidirectional_check'] + bidirectional_check,
                global_config['giraph_param_alternating_least_square_bias_on'] + bias_on]


    def cgd(
            self,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            vector_value=global_config['giraph_vector_value'],
            num_worker=global_config['giraph_workers'],
            max_supersteps=global_config['giraph_conjugate_gradient_descent_max_supersteps'],
            feature_dimension=global_config['giraph_conjugate_gradient_descent_feature_dimension'],
            cgd_lambda=global_config['giraph_conjugate_gradient_descent_lambda'],
            convergence_threshold=global_config['giraph_conjugate_gradient_descent_convergence_threshold'],
            learning_output_interval=global_config['giraph_learning_output_interval'],
            max_val=global_config['giraph_conjugate_gradient_descent_maxVal'],
            min_val=global_config['giraph_conjugate_gradient_descent_minVal'],
            bias_on=global_config['giraph_conjugate_gradient_descent_bias_on'],
            bidirectional_check=global_config['giraph_conjugate_gradient_descent_bidirectional_check'],
            num_iters=global_config['giraph_conjugate_gradient_descent_num_iters']
    ):
        """
        The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering algorithm.

        CGD implementation of the algorithm presented in
        Y. Koren. Factorization Meets the Neighborhood: a Multifaceted 
        Collaborative Filtering Model. In ACM KDD 2008. (Equation 5)

        Parameters
        ----------
        input_edge_property_list : List (comma-separated list of strings)
            The edge properties which contain the input edge values if you
            use more than one edge property.
        input_edge_label : String
            The edge property which contains the edge label.
        output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values if
            you use more than one vertex property.
        vertex_type : String
            The vertex property which contains vertex type.
        edge_type : String
            The edge property which contains edge type.
        vector_value: String, optional
            "true" means a vector as vertex value is supported
            "false" means a vector as vertex value is not supported
        num_worker : String, optional
            The number of Giraph workers.
        max_supersteps : String, optional
            The number of super steps to run in Giraph.
        feature_dimension :  String, optional
            The feature dimension.
        cgd_lambda : String, optional
            The regularization parameter: f = L2_error + lambda*Tikhonov_regularization
        convergence_threshold : String, optional
            The convergence threshold which controls how small the change in validation error must be
            in order to meet the convergence criteria.
        learning_output_interval : String, optional
            The learning curve output interval.
            The default value is 1.
            Since each CGD iteration is composed by 2 super steps,
            the default one iteration means two super steps.
        max_val : String, optional
            The maximum edge weight value.
        min_val : String, optional
            The minimum edge weight value.
        bias_on : String, optional
            True means turn on bias calculation and False means turn off
            bias calculation.
	bidirectional_check : String, optional
            If it is true, Giraph will firstly check whether each edge is bidirectional.
        num_iters : String, optional
            The number of CGD iterations in each super step.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The convergence curve is
            accessible through the report object.

        """
        self._output_vertex_property_list = output_vertex_property_list
        self._vertex_type = global_config['hbase_column_family'] + vertex_type
        self._edge_type = global_config['hbase_column_family'] + edge_type
        self._vector_value = vector_value
        self._bias_on = bias_on
        self._feature_dimension = feature_dimension
        output_path = global_config['giraph_output_base'] + '/' + self._table_name + '/cgd'
        cgd_command = self._get_cgd_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            self._vertex_type,
            self._edge_type,
            vector_value,
            num_worker,
            max_supersteps,
            feature_dimension,
            cgd_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            num_iters,
            bidirectional_check,
            output_path
        )
        cgd_cmd = ' '.join(map(str,cgd_command))
        #print cgd_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(cgd_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time

        cgd_results = self._update_learning_curve(output_path,
                                                  'cgd-learning-report_0',
                                                  time_str,
                                                  'CGD Learning Curve')

        output = AlgorithmReport()
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.max_superstep = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.feature_dimension = feature_dimension
        output.param_lambda = cgd_lambda
        output.learning_output_interval = learning_output_interval
        output.max_val = max_val
        output.min_val = min_val
        output.bias_on = bias_on
        output.num_iters = num_iters
        output.super_steps = list(cgd_results[0])
        output.cost_train = list(cgd_results[1])
        output.rmse_validate = list(cgd_results[2])
        output.rmse_test = list(cgd_results[3])
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_cgd_command(
            self,
            table_name,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            vertex_type,
            edge_type,
            vector_value,
            num_worker,
            max_supersteps,
            feature_dimension,
            cgd_lambda,
            convergence_threshold,
            learning_output_interval,
            max_val,
            min_val,
            bias_on,
            num_iters,
            bidirectional_check,
            output_path
    ):
        """
        generate conjugate gradient descent command line
        """

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
                global_config['giraph_param_input_edge_property_list'] + global_config['hbase_column_family'] +
                input_edge_property_list,
                global_config['giraph_param_input_edge_label'] + input_edge_label,
                global_config['giraph_param_output_vertex_property_list'] + output_vertex_property_list,
                global_config['giraph_param_vector_value'] + vector_value,
                global_config['giraph_param_output_bias'] + bias_on,
                global_config['giraph_param_vertex_type'] + vertex_type,
                global_config['giraph_param_edge_type'] + edge_type,
                global_config['giraph_conjugate_gradient_descent_class'],
                '-mc',
                global_config['giraph_conjugate_gradient_descent_class'] + '\$' + global_config[
                    'giraph_conjugate_gradient_descent_master_compute'],
                '-aw',
                global_config['giraph_conjugate_gradient_descent_class'] + '\$' + global_config[
                    'giraph_conjugate_gradient_descent_aggregator'],
                '-vif',
                global_config['giraph_conjugate_gradient_descent_input_format'],
                '-vof',
                global_config['giraph_conjugate_gradient_descent_output_format'],
                '-op',
                output_path,
                '-w',
                num_worker,
                global_config['giraph_param_conjugate_gradient_descent_max_supersteps'] + max_supersteps,
                global_config['giraph_param_conjugate_gradient_descent_feature_dimension'] + feature_dimension,
                global_config['giraph_param_conjugate_gradient_descent_lambda'] + cgd_lambda,
                global_config['giraph_param_conjugate_gradient_descent_convergence_threshold'] + convergence_threshold,
                global_config[
                    'giraph_param_conjugate_gradient_descent_learning_output_interval'] + learning_output_interval,
                global_config['giraph_param_conjugate_gradient_descent_maxVal'] + max_val,
                global_config['giraph_param_conjugate_gradient_descent_minVal'] + min_val,
                global_config['giraph_param_conjugate_gradient_descent_bias_on'] + bias_on,
                global_config['giraph_param_conjugate_gradient_descent_bidirectional_check'] + bidirectional_check,
                global_config['giraph_param_conjugate_gradient_descent_num_iters'] + num_iters
        ]


class AlgorithmReport():
    """
    Algorithm execution report object, tailored to each algorithm
    """
    #  Since different algorithms have different properties to report,
    #  we initialize it as an empty class
    pass


job_completion_pattern = re.compile(r".*?Giraph Stats")


class GiraphProgressReportStrategy(ProgressReportStrategy):
    """
    The progress report strategy for Giraph jobs
    """
    def report(self, line):
        """
        to report progress of Giraph job
        """
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
                self.job_progress_bar_list[-1]._enable_animation()
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


class RecommenderProgressReportStrategy(ReportStrategy):
    """
    The progress report strategy for recommender task
    """
    def __init__(self):
        """
        initialize the progress bar
        """
        self.progress_list = []
        progress_bar = Progress("Progress")
        progress_bar._repr_html_()
        progress_bar._enable_animation()
        progress_bar.update(100)
        self.progress_bar = progress_bar

    def report(self, line):
        """
        to report progress of recommender task
        """
        if re.search(r"complete recommend", line):
            self.progress_bar._disable_animation()

    def handle_error(self, error_code, error_message):
        """
        turn the progress bar to red if there is error during execution
        """
        self.progress_bar.alert()
