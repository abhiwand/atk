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

import re
import time

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.path as path
import pydoop.hdfs as hdfs
import numpy as np

from intel_analytics.subproc import call
from intel_analytics.config import global_config, get_time_str
from intel_analytics.report import ProgressReportStrategy, find_progress, \
    MapReduceProgress, ReportStrategy
from intel_analytics.progress import Progress
from intel_analytics.graph.titan.config import titan_config


class TitanGiraphMachineLearning(object):
    """
    Titan-based Giraph Machine Learning instance for a graph
    """

    def __init__(self, graph):
        """
        initialize the global variables in TitanGiraphMachineLearning
        """
        self._graph = graph
        self._table_name = graph.titan_table_name
        self.report = []
        self._label_font_size = 12
        self._title_font_size = 14
        self._num_edges = 0
        self._num_vertices = 0
        self._superstep_pattern = re.compile(r'superstep')
        self._num_vertices_pattern = re.compile(r'Number of vertices')
        self._num_edges_pattern = re.compile(r'Number of edges')
        self._validate_pattern = re.compile(r'.*?validate: 0')
        self._output_pattern = re.compile(r'======')
        self._score_pattern = re.compile(r'.*?score')
        self._result = {'als':[],
                        'avg_path_len':[],
                        'belief_prop':[],
                        'cgd':[],
                        'connected_components':[],
                        'label_prop':[],
                        'lda':[],
                        'page_rank':[]}


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
                            curve_ylabel3,
                            with_validate
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


        if with_validate:
            axes2 = fig.add_axes([1.1, 0.1, 0.8, 0.8])
            axes2.plot(data_x, data_v, 'g')
            axes2.set_xlabel("Number of SuperStep", fontsize=self._label_font_size)
            axes2.set_ylabel(curve_ylabel2, fontsize=self._label_font_size)
            title_str = [curve_title, " (Validate)"]
            axes2.set_title(' '.join(map(str, title_str)), fontsize=self._title_font_size)
            axes2.grid(True, linestyle='-', color='0.75')

            axes3 = fig.add_axes([2.1, 0.1, 0.8, 0.8])
        else:
            axes3 = fig.add_axes([1.1, 0.1, 0.8, 0.8])
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
                               curve_title,
                               curve_ylabel):
        with hdfs.open(output_path) as result:
            data_x = []
            data_y = []
            num_vertices = 0
            num_edges = 0
            progress_results = []
            for line in result:
                if re.match(self._superstep_pattern, line):
                    results = line.split()
                    data_x.append(results[2])
                    data_y.append(results[5])
                elif re.match(self._num_vertices_pattern, line):
                    results = line.split()
                    num_vertices = results[3]
                elif re.match(self._num_edges_pattern, line):
                    results = line.split()
                    num_edges = long(results[3])

        progress_results.append(data_x)
        progress_results.append(data_y)
        progress_results.append(num_vertices)
        progress_results.append(num_edges)
        self._num_vertices = num_vertices
        self._num_edges = num_edges
        self._plot_progress_curve(data_x, data_y, curve_title, curve_ylabel)
        return progress_results

    def _update_learning_curve(self,
                               output_path,
                               curve_title,
                               curve_ylabel1="Cost (Train)",
                               curve_ylabel2="RMSE (Validate)",
                               curve_ylabel3="RMSE (Test)"
                               ):
        with hdfs.open(output_path) as result:
            data_x = []
            data_y = []
            data_v = []
            data_t = []
            num_vertices = 0
            num_edges = 0
            learning_results = []
            with_validate = True
            for line in result:
                if re.match(self._superstep_pattern, line):
                    results = line.split()
                    data_x.append(results[2])
                    data_y.append(results[5])
                    data_v.append(results[8])
                    data_t.append(results[11])
                elif re.match(self._num_vertices_pattern, line):
                    if re.match(self._validate_pattern, line):
                        with_validate = False
                    results = line.split()
                    num_vertices = long(results[3])
                elif re.match(self._num_edges_pattern, line):
                    if re.match(self._validate_pattern, line):
                        with_validate = False
                    results = line.split()
                    num_edges = long(results[3])

        learning_results.append(data_x)
        learning_results.append(data_y)
        learning_results.append(data_v)
        learning_results.append(data_t)
        learning_results.append(num_vertices)
        learning_results.append(num_edges)
        self._num_vertices = num_vertices
        self._num_edges = num_edges
        self._plot_learning_curve(data_x,
                                  data_y,
                                  data_v,
                                  data_t,
                                  curve_title,
                                  curve_ylabel1,
                                  curve_ylabel2,
                                  curve_ylabel3,
                                  with_validate)
        return learning_results

    def _del_old_output(self, output_path):
        """
        Deletes the old output directory if it exists.
        """
        del_cmd = 'if ' + global_config['hadoop'] + ' fs -test -e ' + output_path + \
                  '; then ' + global_config['hadoop'] + ' fs -rmr -skipTrash ' + output_path + '; fi'
        call(del_cmd, shell=True)

    def _create_dir(self, path):
        """
        Create the specified directory if it does not exist
        """
        cmd = 'if [ ! -d ' + global_config['giraph_report_dir'] + ' ]; then mkdir ' + global_config['giraph_report_dir'] + '; fi'
        call(cmd, shell=True)
        cmd = 'if [ ! -d ' + path + ' ]; then mkdir ' + path + '; fi'
        call(cmd, shell=True)

    def _plot_roc_curve(self,
                        fig,
                        data_x,
                        data_y,
                        curve_title,
                        index):
        """
        Plots progress curves for algorithms.
        """
        #fig, axes = plt.subplots()
        axes = fig.add_axes([index+0.1, 0.1, 0.8, 0.8])
        axes.plot(data_x, data_y, 'b')
        auc = np.trapz(data_y, data_x)

        axes.set_title(curve_title + ", AUC: " + str("{0:.2f}".format(auc)) + ")", fontsize=self._title_font_size)
        axes.set_xlabel("False Positive Rate", fontsize=self._label_font_size)
        axes.set_ylabel("True Positive Rate", fontsize=self._label_font_size)
        axes.grid(True, linestyle='-', color='0.75')
        #print "AUC: " + str("{0:.2f}".format(auc))
        return auc

    def _plot_histogram(self,
                        fig,
                        bin_num,
                        data,
                        curve_title,
                        curve_xlabel,
                        index):
        """
        Plots histogram on input data
        """
        #fig = plt.figure()

        axis = fig.add_axes([index+0.1, 0.1, 0.8, 0.8])  # left,bottom,width,height

        y_count, x_bins = np.histogram(data, bin_num)
        np.append(x_bins,1)
        np.append(y_count,0)

        # get the corners of the rectangles for the histogram
        left = np.array(x_bins[:-1])
        right = np.array(x_bins[1:])
        bottom = np.zeros(len(left))
        top = bottom + y_count

        # we need a (numrects x numsides x 2) numpy array for the path helper
        # function to build a compound path
        XY = np.array([[left,left,right,right], [bottom,top,top,bottom]]).T

        # get the Path object
        barpath = path.Path.make_compound_path_from_polys(XY)

        # make a patch out of it
        patch = patches.PathPatch(barpath, facecolor='blue', edgecolor='gray', alpha=0.8)
        axis.add_patch(patch)

        # update the view limits
        axis.set_xlim(left[0], right[-1])
        axis.set_ylim(bottom.min(), top.max())
        axis.set_title(curve_title, fontsize=self._title_font_size)
        axis.set_xlabel(curve_xlabel, fontsize=self._label_font_size)
        axis.set_ylabel("Frequency", fontsize=self._label_font_size)

        #plt.show()

    def get_histogram(self,
                      first_property_name,
                      second_property_name='',
                      enable_roc=global_config['giraph_enable_roc'],
                      roc_threshold=global_config['giraph_roc_threshold'],
                      property_type=global_config['giraph_histogram_property_type'],
                      vertex_type_key=global_config['giraph_vertex_type_key'],
                      split_types=global_config['giraph_roc_split_types'],
                      bin_num=global_config['giraph_histogram_bin_num'],
                      path=global_config['giraph_histogram_dir']):
        """
        Get histogram and optionally ROC curve on property values

        Parameters
        ----------
        first_property_name : String
            The property name on which users want to get histogram.
            When used without second_property_name, this property name can from either prior
            or posterior properties. When used together with second_property_name, expect the
            first_property_name is from prior properties, and the second_property_name is from
            posterior properties.

        second_property_name : String, optional
            The property name on which users want to get histogram.
            The default value is empty string.
        enable_roc : Boolean, optional
            True means to plot ROC curve on the validation (VA) and test(TE) splits of
            the prior and posterior values, as well as calculate the AUC value on each
            feature dimension of the prior and posterior values.
            False means not to plot ROC curve.
            The default value is False.
        roc_threshold: String, optional
            The ROC threshold parameters in "min:step:max" format.
            The default value is "0:0.05:1"
        property_type : String, optional
            The type of the first and second property.
            Valid values are either VERTEX_PROPERTY or EDGE_PROPERTY.
            The default value is VERTEX_PROPERTY.
        vertex_type_key : String, optional
            The property name for vertex type. The default value "vertex_type".
            We need this name to know data is in train, validation or test splits.
        split_types : String, optional
            The left-side vertex name. The default value is "user".
        bin_num : String, optional
            The bin number to plot histogram. The default value is 30.
        path: String, optional
            The path to store histogram data. The default value is /tmp/giraph/histogram/

        Returns
        -------
        output : AlgorithmReport
            Execution time, and AUC values on each feature if ROC is enabled.
        """
        enable_roc = str(enable_roc).lower()
        self._create_dir(path)
        hist_cmd1 = [global_config['titan_gremlin'],
                     '-e',
                     global_config['giraph_histogram_script']]
        hist_cmd1 = ' '.join(hist_cmd1)
        hist_command = [self._table_name,
                        property_type,
                        enable_roc,
                        roc_threshold,
                        global_config['hbase_column_family'] + first_property_name,
                        second_property_name,
                        path,
                        global_config['hbase_column_family'] + vertex_type_key,
                        split_types,
                        global_config['titan_storage_hostname'],
                        global_config['titan_storage_port'],
                        global_config['titan_storage_backend']]
        hist_cmd2 = '::'.join(map(str, hist_command))
        hist_cmd = hist_cmd1 + ' ' + hist_cmd2
        time_str = get_time_str()
        start_time = time.time()
        #print hist_cmd
        call(hist_cmd, shell=True, report_strategy=GroovyProgressReportStrategy())

        #with open(path) as file:
        #    data = f.read().splitlines()
        #    data = [[float(digit) for digit in line.split()] for line in file]
        auc = []
        prior_data = np.genfromtxt(path + global_config['hbase_column_family'] + first_property_name + '.txt', delimiter=' ')
        if second_property_name != '':
            posterior_data = np.genfromtxt(path + second_property_name +'.txt', delimiter=',')

        for i in range(0, len(prior_data[0])):
            fig1 = plt.figure()
            if second_property_name != '':
                prefix = 'Prior: ' + first_property_name + ' - Feature '
            else:
                prefix = 'Property: ' + first_property_name + ' - Feature '
            self._plot_histogram(fig1,
                                 bin_num,
                                 prior_data[:,i],
                                 prefix + str(i) + ' Histgoram',
                                 prefix + str(i),
                                 0)
            if second_property_name != '':
                prefix = 'Posterior: ' + first_property_name + ' - Feature '
                self._plot_histogram(fig1,
                                     bin_num,
                                     posterior_data[:,i],
                                     prefix + str(i) + ' Histgoram',
                                     prefix + str(i),
                                     1)

                if enable_roc == 'true':
                    splits = split_types.split(',')
                    fig2 = plt.figure()
                    for j in range(0, len(splits)):
                        roc_data = np.genfromtxt(path + global_config['hbase_column_family'] + first_property_name +
                                             '_' + second_property_name + '_roc_' + str(i) + '_' +
                                             splits[j] +'.txt', delimiter='\t')

                        normalized_fp = roc_data[:,0]
                        normalized_tp = roc_data[:,1]

                        title = "ROC Curve (" + splits[j] + ")\n(Prior: " + first_property_name +\
                                ", Posterior: " + second_property_name + ")\n" + "(Feature" + str(i)
                        result = self._plot_roc_curve(fig2,
                                                      normalized_fp,
                                                      normalized_tp,
                                                      title,
                                                      j)
                        auc.append(result)

        exec_time = time.time() - start_time
        output = AlgorithmReport()
        output.method = 'get_histogram'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        if len(self.report) > 0:
            if hasattr(self.report[-1], 'output_vertex_property_list'):
                output.output_vertex_property_list = self.report[-1].output_vertex_property_list
            if hasattr(self.report[-1], 'vertex_type'):
                output.vertex_type = self.report[-1].vertex_type
            if hasattr(self.report[-1], 'edge_type'):
                output.edge_type = self.report[-1].edge_type
            if hasattr(self.report[-1], 'vector_value'):
                output.vector_value = self.report[-1].vector_value
            if hasattr(self.report[-1], 'bias_on'):
                output.bias_on = self.report[-1].bias_on
            if hasattr(self.report[-1], 'feature_dimension'):
                output.feature_dimension = self.report[-1].feature_dimension
        if enable_roc == 'true':
            output.auc = list(auc)
        self.report.append(output)
        return output


    def recommend(self,
                  vertex_id,
                  left_vertex_name=global_config['giraph_recommend_left_name'],
                  right_vertex_name=global_config['giraph_recommend_right_name'],
                  vertex_type=global_config['giraph_left_vertex_type_str'],
                  input_report=None):
        """
        Make recommendation based on trained model.

        Parameters
        ----------
        vertex_id : String
            vertex id to get recommendation for

        left_vertex_name : String, optional
            The left-side vertex name. For example, if your input data is
            "user,movie,rating", please input "user" for this parameter.
            The default value is "user".
        right_vertex_name : String, optional
            The right-side vertex name. For example, if your input data is
            "user,movie,rating", please input "movie" for this parameter.
            The default value is "movie".
        vertex_type : String, optional
            vertex type to get recommendation for.
            The valid value is either "L" or "R".
            "L" stands for left-side vertices of a bipartite graph.
            "R" stands for right-side vertices of a bipartite graph.
            For example, if your input data is "user,movie,rating" and you
            want to get recommendation on user, please input "L" because
            user is your left-side vertex. Similarly, please input "R if you want
            to get recommendation for movie.
            The default value is "L"
        input_report : AlgorithmReport Object, optional
            The AlgorithmReport object to use for calculating recommendation.
            The default value is the latest algorithm report.

        Returns
        -------
        output : AlgorithmReport
            Top 10 recommendations for the input vertex id
        """
        if input_report is None:
            if len(self.report) < 1:
                raise ValueError("There is no AlgorithmReport to get recommendation for!")
            else:
                if hasattr(self.report[-1], 'output_vertex_property_list'):
                    output_vertex_property_list = ','.join(self.report[-1].output_vertex_property_list)
                else:
                    raise ValueError("There is no output_vertex_property_list attribute in AlgorithmReport!"
                                     "Recommend method needs this attribute.")
                if hasattr(self.report[-1], 'vertex_type'):
                    vertex_type_key = self.report[-1].vertex_type
                else:
                    raise ValueError("There is no vertex_type attribute in AlgorithmReport!"
                                     "Recommend method needs this attribute.")
                if hasattr(self.report[-1], 'edge_type'):
                    edge_type_key = self.report[-1].edge_type
                else:
                    raise ValueError("There is no edge_type attribute in AlgorithmReport!"
                                     "Recommend method needs this attribute.")
                if hasattr(self.report[-1], 'vector_value'):
                    vector_value = self.report[-1].vector_value
                else:
                    raise ValueError("There is no vector_value attribute in AlgorithmReport!"
                                     "Recommend method needs this attribute.")
                if hasattr(self.report[-1], 'bias_on'):
                    bias_on = self.report[-1].bias_on
                else:
                    raise ValueError("There is no bias_on attribute in AlgorithmReport!"
                                     "Recommend method needs this attribute.")
                if hasattr(self.report[-1], 'feature_dimension'):
                    feature_dimension = self.report[-1].feature_dimension
                else:
                    raise ValueError("There is no feature_dimension attribute in AlgorithmReport!"
                                     "Recommend method needs this attribute.")

        else:
            output_vertex_property_list = ','.join(input_report.output_vertex_property_list)
            vertex_type_key = input_report.vertex_type
            edge_type_key = input_report.edge_type
            vector_value = input_report.vector_value
            bias_on = input_report.bias_on
            feature_dimension = input_report.feature_dimension

        rec_cmd1 = [ global_config['titan_gremlin'],
                     '-e',
                     global_config['giraph_recommend_script']
                     ]
        rec_cmd1 = ' '.join(rec_cmd1)
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
                       global_config['hbase_column_family'] + vertex_type_key,
                       global_config['hbase_column_family'] + edge_type_key,
                       vertex_type,
                       str(vector_value).lower(),
                       str(bias_on).lower(),
                       feature_dimension]
        rec_cmd2 = '::'.join(map(str, rec_command))
        rec_cmd = rec_cmd1 + ' ' + rec_cmd2
        #print rec_cmd
        #if want to directly use subprocess without progress bar, it is like this:
        #p = subprocess.Popen(rec_cmd, shell=True, stdout=subprocess.PIPE)
        #out = p.communicate()
        time_str = get_time_str()
        start_time = time.time()
        out = call(rec_cmd, shell=True, report_strategy=GroovyProgressReportStrategy(), return_stdout=1)
        exec_time = time.time() - start_time
        recommend_id = []
        recommend_score = []
        width = 10
        for line in out:
        #for i in range(len(out)):
            if re.match(self._output_pattern, line):
                print line
            elif re.match(self._score_pattern, line):
                results = line.split()
                recommend_id.append(results[1])
                recommend_score.append(results[3])
                print '{0:{width}}'.format(results[0], width=width),
                print '{0:{width}}'.format(results[1], width=width),
                print '{0:{width}}'.format("=>", width=width),
                print '{0:{width}}'.format(results[2], width=width),
                print '{0:{width}}'.format(results[3], width=width),
                print

        output = AlgorithmReport()
        output.method = 'recommend'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.recommend_id = list(recommend_id)
        output.recommend_score = list(recommend_score)
        if hasattr(self.report[-1], 'output_vertex_property_list'):
            output.output_vertex_property_list = self.report[-1].output_vertex_property_list
        output.vertex_type = vertex_type_key
        output.edge_type = edge_type_key
        output.vector_value = vector_value
        output.bias_on = bias_on
        output.feature_dimension = feature_dimension
        self.report.append(output)
        return output

    def _update_hdfs_file(self,
                     path_name,
                     file_name):
        """
        update hdfs file if it already exists
        """
        if hdfs.path.exists(file_name):
            hdfs.rmr(file_name)
        input_path = os.path.join(path_name, file_name)
        hdfs.put(input_path, file_name)


    def kfold_split_update(self,
                    test_fold_id=2,
                    fold_id_property_key="fold_id",
                    split_name=["TE","TR"],
                    split_property_key='edge_type',
                    type='EDGE'):
        """
        Split user's ML data into Train and Test for k-fold cross-validation.

        Parameters
        ----------
        test_fold_id : Integer, optional
            Which fold to use for test.
            Usually this method is a follow-up of kfold_split method in bigdataframe.
            The valid value range is [1,k], where k is the number of fold configured in kfold_split
            method in bigdataframe.
            The default value is 2.
        fold_id_property_key : String, optional
            The key/name of the property which contains fold_id.
            The default value is "fold_id"
        split_name : List, optional
            Each value is the name for each split.
            The size of the list is 2.
            The default value is ["TE", "TR"]
        split_property_key : string, optional
            The key/name of the property which stores split results.
            The default value is "splits"
        type : string, optional
            To split on edges or on vertices.
            The valid value range is either "EDGE" or "VERTEX".
            The default value is "EDGE"
        """

        #sanity check on parameters
        if not isinstance(test_fold_id, (int, long)):
            raise TypeError("test_fold_id should be an integer.")
        elif test_fold_id < 1:
            raise ValueError("test_fold_id should be positive integer")

        if not isinstance(fold_id_property_key, basestring):
            raise TypeError("fold_id_property_key should be a string.")

        if not isinstance(split_name, list):
            raise TypeError("split_name should be a list.")
        else:
            num_split = len(split_name)
            if num_split != 2:
                raise ValueError("The number of split should be two.")

        if not isinstance(split_property_key, basestring):
            raise TypeError("split_property_key should be a string.")

        if not isinstance(type, basestring):
            raise TypeError("type should be a string.")
        elif type != 'EDGE' and type != 'VERTEX':
            raise ValueError("type should be either 'EDGE' or 'VERTEX'")

        cmd = [self._table_name,
               global_config['titan_storage_backend'],
               global_config['titan_storage_hostname'],
               global_config['titan_storage_port'],
               global_config['titan_storage_connection_timeout'],
               test_fold_id,
               global_config['hbase_column_family'] + fold_id_property_key,
               ';'.join(split_name),
               global_config['hbase_column_family'] + split_property_key,
               type]

        split_cmd1 = '::'.join(map(str, cmd))

        if self._num_edges >= long(global_config['medium_graph_threshold']):
            # run by faunus
            self._update_hdfs_file(global_config['giraph_faunus_script_path'],
                                    global_config['giraph_faunus_split_script_name'])
            faunus_config_file = titan_config.write_faunus_cfg(self._table_name)
            split_cmd2 = [global_config['faunus_gremlin'],
                          ' -i ',
                          faunus_config_file,
                          "\"g.V.script('" + global_config['giraph_faunus_split_script_name'] + "', '" + split_cmd1 + "')\""
                          ]
        else:
            # run by titan gremlin
            split_cmd2 = [global_config['titan_gremlin'],
                            ' -e ',
                            global_config['giraph_titan_split_script'],
                            "'" + split_cmd1 + "'"]

        split_cmd = ' '.join(map(str, split_cmd2))
        #print   split_cmd

        time_str = get_time_str()
        start_time = time.time()
        call(split_cmd, shell=True, report_strategy=GroovyProgressReportStrategy())
        exec_time = time.time() - start_time
        output = AlgorithmReport()
        output.method = 'kfold_split_update'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.test_fold_id = test_fold_id
        output.fold_id_property_key = fold_id_property_key
        output.split_name = split_name
        output.split_property_key = split_property_key
        self.report.append(output)
        return output


    def kfold_combine(self,
                      output_vertex_property_list,
                      k=10,
                      type='AVG',
                      input_result_list=None,
                      enable_standard_deviation=False
                      ):
        """
        Combine results from k iterations as the final k-fold cross validation result

        Parameters
        ----------
        output_vertex_property_list : String List
            The list of the result properties on which to save final results.
            When bias_on was True when running algorithms, the last element is the key for the combined bias.

        k: Integer, optional
            The number of folds for k-fold cross validation.
            The valid value range is positive integer larger than 1.
            The default value is 10.
        type: String, optional
            The type of combine function to run.
            The valid value is in ['AVG'] range.
            The default value is 'AVG'
        input_result_list: List of AlgorithmReport objects, optional
            The list of AlgorithmReports to combine.
            Expect each element is the name of AlgorithmReport for a run in k-fold iteration.
            Please note, bias_on must be consistent for all results which need to be combined.
            The default value is the latest k AlgorithmReports which from the same algorithm as the latest run algorithm
        enable_standard_deviation : Boolean, optional
            True means to calculate standard deviation across k runs of algorithms.
            False means not to calculate standard deviation across k runs of algorithms.
            When it is enabled, the standard deviaion will be stored at ${output_vertex_property_list}_std
            The default value is False.
        """
        #sanity check on parameters
        if not isinstance(output_vertex_property_list, list):
            raise TypeError("output_vertex_property_list should be a list.")

        if not isinstance(k, (int, long)):
            raise TypeError("k should be an integer.")
        elif k < 1:
            raise ValueError("k should be positive integer")

        if not isinstance(type, basestring):
            raise TypeError("type should be a string.")
        elif type not in global_config['giraph_combine_functions'].split(','):
            raise ValueError("combine type " + type + " is not supported!")

        bias_on = False
        input_result_keys = []
        vertex_type = None
        edge_type = None
        feature_dimension = 0
        supported_list = global_config['giraph_supported_algorithms'].split(',')
        if input_result_list is None:
            num_runs = len(self.report)
            if num_runs < k:
                raise ValueError("only " + str(num_runs) + " results are available. "
                                                           "Please configure k correctly.")
            else:
                num_runs = 0
                algorithm = None
                for result in reversed(self.report):
                    # get the latest run algorithm
                    if algorithm is None and (result.method in supported_list):
                        algorithm = result.method
                        if algorithm in ['als', 'cgd']:
                            bias_on = result.bias_on
                            vertex_type = result.vertex_type
                            edge_type = result.edge_type
                            feature_dimension = result.feature_dimension
                    # get the latest k runs of the last run algorithm
                    if algorithm == result.method:
                        input_result_keys.append(result.output_vertex_property_list)
                        num_runs += 1
                        if num_runs == k:
                            break
                if algorithm is not None and num_runs < k:
                    raise ValueError("only " + str(num_runs) + " results are available for " + algorithm)

        elif not isinstance(input_result_list, list):
            raise TypeError("input_result_list should be a list.")
        elif len(input_result_list) != k:
            raise ValueError(str(len(input_result_list)) + " results are configured. It mismatches k (=",
                             + str(k) + "). Please configure k correctly.")
        else:
            algorithm = input_result_list[0].method
            for idx, result in enumerate(input_result_list):
                if not result.method in supported_list:
                    raise ValueError("AlgorithmReport should from supported algorithm!")
                elif result.method != algorithm:
                    raise ValueError("AlgorithmReports should from the same algorithm!")
                elif algorithm in ['als', 'cgd'] and (idx == 0):
                    bias_on = result.bias_on
                    vertex_type = result.vertex_type
                    edge_type = result.edge_type
                    feature_dimension = result.feature_dimension
                if result.bias_on != bias_on:
                    raise ValueError("bias_on should be the same across k runs!")
                input_result_keys.append(result.output_vertex_property_list)

        vector_value = True
        combined_size = len(output_vertex_property_list)
        if bias_on:
            if combined_size < 2:
                raise ValueError("bias_on then at least two keys are expected for output_vertex_property_list.")
            elif combined_size > 2:
                vector_value = False

        #faunus gremlin does not work with "," in command line args, so concatenate result keys with semicolon
        # And concatenate several old results with ":"
        input_property_key = []
        pre_size = 1
        for result in input_result_keys:
            size = len(result)
            if bias_on:
                size -= 1
                #when vector_value was enabled when running algorithm, the length of output_vertex_property
            # is 1. When vector_value was not enabled, the length of output_vertex_property should be the
            # feature size, and it should be the same across k runs
            if pre_size != 1 and size != 1 and size != pre_size:
                raise ValueError("The vector size of different results do not match!")
            else:
                pre_size = size
                input_property_key.append(';'.join(result))


        if enable_standard_deviation:
            output_std_property_key = []
            for name in output_vertex_property_list:
                output_std_property_key.append(name + '_std')
            output_std_property_key = ';'.join(output_std_property_key)
        else:
            output_std_property_key = None

        cmd = [self._table_name,
               global_config['titan_storage_backend'],
               global_config['titan_storage_hostname'].replace(',',';'),
               global_config['titan_storage_port'],
               global_config['titan_storage_connection_timeout'],
               ':'.join(input_property_key),
               type,
               ';'.join(output_vertex_property_list),
               str(bias_on).lower(),
               str(enable_standard_deviation).lower(),
               output_std_property_key]
        combine_cmd1 = '::'.join(map(str, cmd))

        if self._num_edges >= long(global_config['medium_graph_threshold']):
            # run by faunus
            self._update_hdfs_file(global_config['giraph_faunus_script_path'],
                                   global_config['giraph_faunus_combine_script_name'])
            faunus_config_file = titan_config.write_faunus_cfg(self._table_name)
            combine_cmd2 = [global_config['faunus_gremlin'],
                            ' -i ',
                            faunus_config_file,
                            "\"g.V.script('" + global_config['giraph_faunus_combine_script_name'] + "', '" + combine_cmd1 + "')\""
                            ]
        else:
            # run by titan gremlin
            combine_cmd2 = [global_config['titan_gremlin'],
                            ' -e ',
                            global_config['giraph_titan_combine_script'],
                            "'" + combine_cmd1 + "'"]

        combine_cmd = ' '.join(map(str, combine_cmd2))
        #print   combine_cmd

        time_str = get_time_str()
        start_time = time.time()
        call(combine_cmd, shell=True, report_strategy=GroovyProgressReportStrategy())
        exec_time = time.time() - start_time
        output = AlgorithmReport()
        output.method = 'kfold_combine'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.output_vertex_property_list = output_vertex_property_list
        output.k = k
        output.type = type
        output.input_result_list = input_result_keys
        output.enable_standard_deviation = enable_standard_deviation
        output.vertex_type = vertex_type
        output.edge_type = edge_type
        output.feature_dimension = feature_dimension
        output.bias_on = bias_on
        output.vector_value = vector_value
        self.report.append(output)
        return output


    def belief_prop(self,
                    input_vertex_property_list,
                    input_edge_property_list,
                    input_edge_label,
                    output_vertex_property_list,
                    vertex_type=global_config['giraph_vertex_type_key'],
                    num_mapper=global_config['giraph_number_mapper'],
                    mapper_memory=global_config['giraph_mapper_memory'],
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
            The edge properties which contain the input edge values.
            We expect comma-separated list of property names  if you use
            more than one edge property.
        input_edge_label : String
            The name of edge label.
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.

        vertex_type : String, optional
            The name of vertex property which contains vertex type.
            The default value is "vertex_type"
        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        vector_value: Boolean, optional
            True means a vector as vertex value is supported
            False means a vector as vertex value is not supported
            The default value is false.
        num_worker : Integer, optional
            The number of Giraph workers.
            The default value is 15.
        max_supersteps : Integer, optional
            The maximum number of super steps that the algorithm will execute.
            The valid value range is all positive integer.
            The default value is 20.
        smoothing : Float, optional
            The Ising smoothing parameter. This parameter adjusts the relative strength
            of closeness encoded edge weights, similar to the width of Gussian distribution.
            Larger value implies smoother decay and the edge weight beomes less important.
            The default value is 2.0.
        convergence_threshold : Float, optional
            The amount of change in cost function that will be tolerated at convergence.
            If the change is less than this threshold, the algorithm exists earlier
            before it reaches the maximum number of super steps.
            The valid value range is all Float and zero.
            The default value is 0.001.
        bidirectional_check : Boolean, optional
	        If it is true, Giraph will firstly check whether each edge is bidirectional before
	        running algorithm. This option is mainly for graph integrity check. Turning it on
	        only makes sense when all nodes are labeled as "TR", otherwise the algorithm will
	        terminate, because all edges connected to "VA"/"TE" nodes will be treated internally
	        as single directional even though they are defined as bi-directional input graph.
            The default value is false.
        anchor_threshold : Float, optional
            The parameter that determines if a node's posterior will be updated or not.
            If a node's maximum prior value is greater than this threshold, the node
            will be treated as anchor node, whose posterior will inherit from prior without
            update. This is for the case where we have confident prior estimation for some nodes
            and don't want the algorithm updates these nodes.
            The valid value range is in [0, 1].
            The default value is 1.0

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.
        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'lbp')
        lbp_command = self._get_lbp_command(
            self._table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            ','.join(output_vertex_property_list),
            global_config['hbase_column_family'] + vertex_type,
            str(num_mapper),
            mapper_memory,
            str(vector_value).lower(),
            str(num_worker),
            str(max_supersteps),
            str(convergence_threshold),
            str(smoothing),
            str(anchor_threshold),
            str(bidirectional_check).lower(),
            output_path)
        lbp_cmd = ' '.join(map(str, lbp_command))
        #print lbp_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(lbp_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time

        output = AlgorithmReport()
        output.method = 'belief_prop'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_vertex_property_list = input_vertex_property_list
        output.input_edge_property_list = input_edge_property_list
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.vertex_type = vertex_type
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.vector_value = vector_value
        output.num_worker = num_worker
        output.max_supersteps = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.smoothing = smoothing
        output.bidirectional_check = bidirectional_check
        output.anchor_threshold = anchor_threshold
        output_path = os.path.join(output_path, 'lbp-learning-report_0')
        if hdfs.path.exists(output_path):
            lbp_results = self._update_learning_curve(output_path,
                                                      'LBP Learning Curve',
                                                      'Average Train Delta',
                                                      'Average Validation Delta',
                                                      'Average Test Delta')


            output.super_steps = list(lbp_results[0])
            output.cost_train = list(lbp_results[1])
            output.rmse_validate = list(lbp_results[2])
            output.rmse_test = list(lbp_results[3])
            output.num_vertices = lbp_results[4]
            output.num_edges = lbp_results[5]
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
            num_mapper,
            mapper_memory,
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
                global_config['giraph_param_number_mapper'] + num_mapper,
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
                  num_mapper=global_config['giraph_number_mapper'],
                  mapper_memory=global_config['giraph_mapper_memory'],
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
            The name of edge label.
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.
        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        num_worker : Integer, optional
            The number of workers.
            The default value is 15.
        max_supersteps : Integer, optional
            The maximum number of super steps that the algorithm will execute.
            The valid value range is all positive integer.
            The default value is 20.
        convergence_threshold : Float, optional
            The amount of change in cost function that will be tolerated at convergence.
            If the change is less than this threshold, the algorithm exists earlier
            before it reaches the maximum number of super steps.
            The valid value range is all Float and zero.
            The default value is 0.001.
        reset_probability : Float, optional
            The probability that the random walk of a page is reset.
        convergence_output_interval : Integer, optional
            The convergence progress output interval
            The valid value range is [1, max_supersteps]
            The default value is 1, which means output every super step.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The progress curve is
            accessible through the report object.
        """
        output_path = os.path.join(global_config['giraph_output_base'],self._table_name,'pr')
        pr_command = self._get_pr_command(
            self._table_name,
            input_edge_label,
            ','.join(output_vertex_property_list),
            str(num_mapper),
            mapper_memory,
            str(num_worker),
            str(max_supersteps),
            str(convergence_threshold),
            str(reset_probability),
            str(convergence_output_interval),
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

        output = AlgorithmReport()
        output.method = 'page_rank'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.num_worker = num_worker
        output.max_supersteps = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.reset_probability = reset_probability
        output.convergence_output_interval = convergence_output_interval
        output_path = os.path.join(output_path, 'pr-convergence-report_0')
        if hdfs.path.exists(output_path):
            pr_results = self._update_progress_curve(output_path,
                                                     'Page Rank Convergence Curve',
                                                     'Vertex Value Change')

            output.super_steps = list(pr_results[0])
            output.convergence_progress = list(pr_results[1])
            output.num_vertices = pr_results[2]
            output.num_edges = pr_results[3]
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_pr_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            num_mapper,
            mapper_memory,
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
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
            num_mapper=global_config['giraph_number_mapper'],
            mapper_memory=global_config['giraph_mapper_memory'],
            convergence_output_interval=global_config['giraph_convergence_output_interval'],
            num_worker=global_config['giraph_workers']
    ):
        """
        The average path length calculation.
        http://en.wikipedia.org/wiki/Average_path_length

        Parameters
        ----------
        input_edge_label : String
            The name of edge label..
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.

        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        convergence_output_interval : Integer, optional
            The convergence progress output interval.
            The default value is 1, which means output every super step.
        num_worker : Integer, optional
            The number of Giraph workers.
            The default value is 15.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The progress curve is
            accessible through the report object.
        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'apl')
        apl_command = self._get_apl_command(
            self._table_name,
            input_edge_label,
            ','.join(output_vertex_property_list),
            str(num_mapper),
            mapper_memory,
            str(convergence_output_interval),
            output_path,
            str(num_worker)
        )
        apl_cmd = ' '.join(map(str, apl_command))
        #print apl_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(apl_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time

        output = AlgorithmReport()
        output.method = 'avg_path_len'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.convergence_output_interval = convergence_output_interval
        output.num_worker = num_worker
        output_path = os.path.join(output_path, 'apl-convergence-report_0')
        if hdfs.path.exists(output_path):
            apl_results = self._update_progress_curve(output_path,
                                                      'Avg. Path Length Progress Curve',
                                                      'Num of Vertex Updates')
            output.super_steps = list(apl_results[0])
            output.convergence_progress = list(apl_results[1])
            output.num_vertices = apl_results[2]
            output.num_edges = apl_results[3]
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_apl_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            num_mapper,
            mapper_memory,
            convergence_output_interval,
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
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
                num_worker,
                global_config['giraph_param_average_path_length_convergence_output_interval'] + convergence_output_interval]



    def connected_components(
            self,
            input_edge_label,
            output_vertex_property_list,
            num_mapper=global_config['giraph_number_mapper'],
            mapper_memory=global_config['giraph_mapper_memory'],
            convergence_output_interval=global_config['giraph_convergence_output_interval'],
            num_worker=global_config['giraph_workers']
    ):
        """
        The connected components computation.

        Parameters
        ----------
        input_edge_label : String
            The name of edge label..
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.
        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        convergence_output_interval : Integer, optional
            The convergence progress output interval.
            The default value is 1, which means output every super step.
        num_worker : Integer, optional
            The number of Giraph workers.
            The default value is 15.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The progress curve is
            accessible through the report object.
        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'cc')
        cc_command = self._get_cc_command(
            self._table_name,
            input_edge_label,
            ','.join(output_vertex_property_list),
            output_path,
            str(num_mapper),
            mapper_memory,
            str(convergence_output_interval),
            str(num_worker)
        )
        cc_cmd = ' '.join(map(str, cc_command))
        #print cc_cmd
        #delete old output directory if already there
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(cc_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time

        output = AlgorithmReport()
        output.method = 'connected_components'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.convergence_output_interval = convergence_output_interval
        output.num_worker = num_worker
        output_path = os.path.join(output_path, 'cc-convergence-report_0')
        if hdfs.path.exists(output_path):
            cc_results = self._update_progress_curve(output_path,
                                                     'Connected Components Progress Curve',
                                                     'Num of Vertex Updates')
            output.super_steps = list(cc_results[0])
            output.convergence_progress = list(cc_results[1])
            output.num_vertices = cc_results[2]
            output.num_edges = cc_results[3]
        output.graph = self._graph
        self.report.append(output)
        return output

    def _get_cc_command(
            self,
            table_name,
            input_edge_label,
            output_vertex_property_list,
            output_path,
            num_mapper,
            mapper_memory,
            convergence_output_interval,
            num_worker
            ):
        """
        generate connected component command line
        """

        return ['hadoop',
                'jar',
                global_config['giraph_jar'],
                global_config['giraph_runner'],
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
                num_worker,
                global_config['giraph_param_connected_components_convergence_output_interval'] + convergence_output_interval]


    def label_prop(
            self,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            output_vertex_property_list,
            num_mapper=global_config['giraph_number_mapper'],
            mapper_memory=global_config['giraph_mapper_memory'],
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
        input_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain prior vertex values if you
            use more than one vertex property.
        input_edge_property_list : List (comma-separated list of strings)
            The edge properties which contain the input edge values.
            We expect comma-separated list of property names  if you use
            more than one edge property.
        input_edge_label : String
            The name of edge label..
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.

        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        vector_value: Boolean, optional
            True means a vector as vertex value is supported
            False means a vector as vertex value is not supported
            The default value is false.
        num_worker : Integer, optional
            The number of Giraph workers.
            The default value is 15.
        max_supersteps : Integer, optional
            The maximum number of super steps that the algorithm will execute.
            The valid value range is all positive integer.
            The default value is 10.
        lambda : Float, optional
            The tradeoff parameter that controls much influence of external
            classifier's prediction contribution to the final prediction.
            This is for the case where an external classifier is available
            that can produce initial probabilistic classification on unlabled
            examples, and the option allows incorporating external classifier's
            prediction into the LP training process
            The valid value range is [0.0,1.0].
            The default value is 0.
        convergence_threshold : Float, optional
            The amount of change in cost function that will be tolerated at convergence.
            If the change is less than this threshold, the algorithm exists earlier
            before it reaches the maximum number of super steps.
            The valid value range is all Float and zero.
            The default value is 0.001.
        bidirectional_check : Boolean, optional
            If it is true, Giraph will firstly check whether each edge is bidirectional
            before running algorithm. LP expects an undirected input graph and each edge
            therefore should be bi-directional. This option is mainly for graph integrity
            check.
        anchor_threshold : Float, optional
            The parameter that determines if a node's initial prediction from external
            classifier will be updated or not. If a node's maximum initial prediction
            value is greater than this threshold, the node will be treated as anchor
            node, whose final prediction will inherit from prior without update. This
            is for the case where we have confident initial predictions on some nodes
            and don't want the algorithm updates those nodes.
            The valid value range is [0, 1].
            The default value is 1.0

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.
        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'lp')
        lp_command = self._get_lp_command(
            self._table_name,
            input_vertex_property_list,
            input_edge_property_list,
            input_edge_label,
            ','.join(output_vertex_property_list),
            str(num_mapper),
            mapper_memory,
            str(vector_value).lower(),
            str(num_worker),
            str(max_supersteps),
            str(convergence_threshold),
            str(lp_lambda),
            str(anchor_threshold),
            str(bidirectional_check).lower(),
            output_path
        )
        lp_cmd = ' '.join(map(str,lp_command))
        #print lp_cmd
        self._del_old_output(output_path)
        time_str = get_time_str()
        start_time = time.time()
        call(lp_cmd, shell=True, report_strategy=GiraphProgressReportStrategy())
        exec_time = time.time() - start_time

        output = AlgorithmReport()
        output.method = 'label_prop'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_vertex_property_list = input_vertex_property_list
        output.input_edge_property_list = input_edge_property_list
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.vector_value = vector_value
        output.num_worker = num_worker
        output.max_supersteps = max_supersteps
        output.convergence_threshold = convergence_threshold
        output.lp_lambda = lp_lambda
        output.bidirectional_check = bidirectional_check
        output.anchor_threshold = anchor_threshold
        output_path = os.path.join(output_path, 'lp-learning-report_0')
        if hdfs.path.exists(output_path):
            lp_results = self._update_progress_curve(output_path,
                                                     'LP Learning Curve',
                                                     'Cost')


            output.super_steps = list(lp_results[0])
            output.cost = list(lp_results[1])
            output.num_vertices = lp_results[2]
            output.num_edges = lp_results[3]
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
            num_mapper,
            mapper_memory,
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
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
            vertex_type=global_config['giraph_vertex_type_key'],
            num_mapper=global_config['giraph_number_mapper'],
            mapper_memory=global_config['giraph_mapper_memory'],
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
            The edge properties which contain the input edge values.
            We expect comma-separated list of property names  if you use
            more than one edge property.
        input_edge_label : String
            The name of edge label.
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.

        vertex_type : String, optional
            The name of vertex property which contains vertex type.
            The default value is "vertex_type"
        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        vector_value: Boolean, optional
            True means a vector as vertex value is supported
            False means a vector as vertex value is not supported
            The default value is false.
        num_worker : Integer, optional
            The number of Giraph workers to run the algorihm.
            The default value is 15.
        max_supersteps : Integer, optional
            The maximum number of super steps (iterations) that the algorithm
            will execute.
            The valid value range is all positive integer.
            The default value is 20.
        alpha : Float, optional
            The hyper-parameter for document-specific distribution over topics.
            It's mainly used as a smoothing parameter in Bayesian inference.
            Larger value implies that documents are assumed to cover all topics
            more uniformly; smaller value implies that documents are more concentrated
            on a small subset of topics.
            Valid value range is all positive Float.
            The default value is 0.1.
        beta : Float, optional
            The hyper-parameter for word-specific distribution over topics.
            It's mainly used as a smoothing parameter in Bayesian inference.
            Larger value implies that topics contain all words more uniformly and
            smaller value implies that topics are more concentrated on a small
            subset of words.
            Valid value range is all positive Float.
            The default value is 0.1.
        convergence_threshold : Float, optional
            The amount of change in LDA model parameters that will be tolerated
            at convergence. If the change is less than this threshold, the algorithm
            exists earlier before it reaches the maximum number of super steps.
            Valid value range is all positive Float and zero.
            The default value is 0.001.
        evaluate_cost : String, optional
            "True" means turn on cost evaluation and "False" means turn off
            cost evaluation. It's relatively expensive for LDA to evaluate cost function.
            For time-critical applications, this option allows user to turn off cost
            function evaluation.
            The default value is false.
        max_val : Float, optional
            The maximum edge weight value. If an edge weight is larger than this
            value, the algorithm will throw an exception and terminate. This option
            is mainly for graph integrity check.
            Valid value range is all Float.
            The default value is Float.POSITIVE_INFINITY.
        min_val : Float, optional
            The minimum edge weight value. If an edge weight is smaller than this
            value, the algorithm will throw an exception and terminate. This option
            is mainly for graph integrity check.
            Valid value range is all Float.
            The default value is Float.NEGATIVE_INFINITY.
        num_topics : Integer, optional
            The number of topics to identify in the LDA model. Using fewer
            topics will speed up the computation, but the extracted topics
            might be more abstract or less specific; using more topics will
            result in more computation but lead to more specific topics.
            Valid value range is all positive integers.
            The default value is 10.
        bidirectional_check : Boolean, optional
            True means to turn on bidirectional check. False means to turn
            off bidirectional check. LDA expects a bi-partite input graph and
            each edge therefore should be bi-directional. This option is mainly
            for graph integrity check.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The convergence curve is
            accessible through the report object.
        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'lda')
        lda_command = self._get_lda_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            ','.join(output_vertex_property_list),
            global_config['hbase_column_family'] + vertex_type,
            str(num_mapper),
            mapper_memory,
            str(vector_value).lower(),
            str(num_worker),
            str(max_supersteps),
            str(alpha),
            str(beta),
            str(convergence_threshold),
            evaluate_cost,
            str(max_val),
            str(min_val),
            str(num_topics),
            str(bidirectional_check).lower(),
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

        output = AlgorithmReport()
        output.method = 'lda'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_edge_property_list = input_edge_property_list
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.vertex_type = vertex_type
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.vector_value = vector_value
        output.num_worker = num_worker
        output.max_supersteps = max_supersteps
        output.alpha = alpha
        output.beta = beta
        output.convergence_threshold = convergence_threshold
        output.evaluate_cost = evaluate_cost
        output.max_val = max_val
        output.min_val = min_val
        output.bidirectional_check = bidirectional_check
        output.num_topics = num_topics
        output_path = os.path.join(output_path, 'lda-learning-report_0')
        if hdfs.path.exists(output_path):
            if evaluate_cost:
                curve_ylabel = 'Cost'
            else:
                curve_ylabel = 'Max Vertex Value Change'

            lda_results = self._update_progress_curve(output_path,
                                                      'LDA Learning Curve',
                                                      curve_ylabel)
            output.super_steps = list(lda_results[0])
            output.cost = list(lda_results[1])
            output.num_vertices = lda_results[2]
            output.num_edges = lda_results[3]
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
            num_mapper,
            mapper_memory,
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
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
            vertex_type=global_config['giraph_vertex_type_key'],
            edge_type=global_config['giraph_edge_type_key'],
            num_mapper=global_config['giraph_number_mapper'],
            mapper_memory=global_config['giraph_mapper_memory'],
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
            The edge properties which contain the input edge values.
            We expect comma-separated list of property names  if you use
            more than one edge property.
        input_edge_label : String
            The name of edge label
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.

        vertex_type : String, optional
            The name of vertex property which contains vertex type.
            The default value is "vertex_type"
        edge_type : String, optional
            The name of edge property which contains edge type.
            The default value is "edge_type"
        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.
        vector_value: Boolean, optional
            True means a vector as vertex value is supported
            False means a vector as vertex value is not supported
            The default value is false.
        num_worker : Integer, optional
            The number of Giraph workers to run the algorithm.
            The default value is 15.
        max_supersteps : Integer, optional
            The maximum number of super steps (iterations) that the algorithm
            will execute.
        feature_dimension : Integer, optional
            The length of feature vector to use in ALS model.
            Larger value in general results in more accurate parameter estimation,
            but slows down the computation.
            The valid value range is all positive integer.
            The default value is 20.
        als_lambda : Float, optional
            The tradeoff parameter that controls the strength of regularization.
            Larger value implies stronger regularization that helps prevent overfitting
            but may cause the issue of underfitting if the value is too large.
            The value is usually determined by cross validation (CV).
            The valid value range is all positive Float and zero.
            The default value is 0.
        convergence_threshold : Float, optional
            The amount of change in cost function that will be tolerated at convergence.
            If the change is less than this threshold, the algorithm exists earlier
            before it reaches the maximum number of super steps.
            The valid value range is all Float and zero.
            The default value is 0.001.
        learning_output_interval : Integer, optional
            The learning curve output interval.
            Since each ALS iteration is composed by 2 super steps,
            the default one iteration means two super steps.
        max_val : Float, optional
            The maximum edge weight value. If an edge weight is larger than this
            value, the algorithm will throw an exception and terminate. This option
            is mainly for graph integrity check.
            Valid value range is all Float.
            The default value is Float.POSITIVE_INFINITY.
        min_val : Float, optional
            The minimum edge weight value. If an edge weight is smaller than this
            value, the algorithm will throw an exception and terminate. This option
            is mainly for graph integrity check.
            Valid value range is all Float.
            The default value is Float.NEGATIVE_INFINITY.
        bidirectional_check : Boolean, optional
            If it is True, Giraph will firstly check whether each edge is bidirectional
            before executing algorithm. ALS expects a bi-partite input graph and each edge
            therefore should be bi-directional. This option is mainly for graph integrity check.
        bias_on : Boolean, optional
            True means turn on the update for bias term and False means turn off
            the update for bias term. Turning it on often yields more accurate model with
            minor performance penalty; turning it off disables term update and leaves the
            value of bias term to be zero.
            The default value is false.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The convergence curve is
            accessible through the report object.
        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'als')
        als_command = self._get_als_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            ','.join(output_vertex_property_list),
            global_config['hbase_column_family'] + vertex_type,
            global_config['hbase_column_family'] + edge_type,
            str(num_mapper),
            mapper_memory,
            str(vector_value).lower(),
            str(num_worker),
            str(max_supersteps),
            str(feature_dimension),
            str(als_lambda),
            str(convergence_threshold),
            str(learning_output_interval),
            str(max_val),
            str(min_val),
            str(bias_on).lower(),
            str(bidirectional_check).lower(),
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
        output = AlgorithmReport()
        output.method = 'als'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_edge_property_list = input_edge_property_list
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.vertex_type = vertex_type
        output.edge_type = edge_type
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.vector_value = vector_value
        output.num_worker = num_worker
        output.max_supersteps = max_supersteps
        output.feature_dimension = feature_dimension
        output.als_lambda = als_lambda
        output.convergence_threshold = convergence_threshold
        output.learning_output_interval = learning_output_interval
        output.max_val = max_val
        output.min_val = min_val
        output.bidirectional_check = bidirectional_check
        output.bias_on = bias_on
        output_path = os.path.join(output_path, 'als-learning-report_0')
        if hdfs.path.exists(output_path):
            als_results = self._update_learning_curve(output_path,
                                                      'ALS Learning Curve')


            output.super_steps = list(als_results[0])
            output.cost_train = list(als_results[1])
            output.rmse_validate = list(als_results[2])
            output.rmse_test = list(als_results[3])
            output.num_vertices = als_results[4]
            output.num_edges = als_results[5]
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
            num_mapper,
            mapper_memory,
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
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
            vertex_type=global_config['giraph_vertex_type_key'],
            edge_type=global_config['giraph_edge_type_key'],
            num_mapper=global_config['giraph_number_mapper'],
            mapper_memory=global_config['giraph_mapper_memory'],
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
            The edge properties which contain the input edge values.
            We expect comma-separated list of property names  if you use
            more than one edge property.
        input_edge_label : String
            The name of edge label.
        output_vertex_property_list : String List
            The list of vertex properties to store output vertex values.
        vertex_type : String, optional
            The name of vertex property which contains vertex type.
            The default value is "vertex_type"
        edge_type : String, optional
            The name of edge property which contains edge type.
            The default value is "edge_type"
        num_mapper: Integer, optional
            It reconfigures Hadoop parameter mapred.tasktracker.map.tasks.maximum
            on the fly when it is needed for users' data sets.
            The default value is 4.
        mapper_memory: String, optional
            It reconfigures Hadoop parameter mapred.map.child.java.opts
            on the fly when it is needed for users' data sets.
            The default value is 12G.

        vector_value: Boolean, optional
            True means a vector as vertex value is supported
            False means a vector as vertex value is not supported
            The default value is false.
        num_worker : Integer, optional
            The number of Giraph workers.
            The default value is 15.
        max_supersteps : Integer, optional
            The maximum number of super steps that the algorithm will execute.
            The valid value range is all positive integer.
            The default value is 20.
        feature_dimension :  Integer, optional
            The length of feature vector to use in ALS model.
            Larger value in general results in more accurate parameter estimation,
            but slows down the computation.
            The valid value range is all positive integer.
            The default value is 20.
        cgd_lambda : Float, optional
            The tradeoff parameter that controls the strength of regularization.
            Larger value implies stronger regularization that helps prevent overfitting
            but may cause the issue of underfitting if the value is too large.
            The value is usually determined by cross validation (CV).
            The valid value range is all positive Float and zero.
            The default value is 0.
        convergence_threshold : Float, optional
            The amount of change in cost function that will be tolerated at convergence.
            If the change is less than this threshold, the algorithm exists earlier
            before it reaches the maximum number of super steps.
            The valid value range is all Float and zero.
            The default value is 0.001.
        learning_output_interval : Integer, optional
            The learning curve output interval.
            The default value is 1.
            Since each CGD iteration is composed by 2 super steps,
            the default one iteration means two super steps.
        max_val : Float, optional
            The maximum edge weight value. If an edge weight is larger than this
            value, the algorithm will throw an exception and terminate. This option
            is mainly for graph integrity check.
            Valid value range is all Float.
            The default value is Float.POSITIVE_INFINITY.
        min_val : Float, optional
            The minimum edge weight value. If an edge weight is smaller than this
            value, the algorithm will throw an exception and terminate. This option
            is mainly for graph integrity check.
            Valid value range is all Float.
            The default value is Float.NEGATIVE_INFINITY.
        bias_on : Boolean, optional
            True means turn on the update for bias term and False means turn off
            the update for bias term. Turning it on often yields more accurate model with
            minor performance penalty; turning it off disables term update and leaves the
            value of bias term to be zero.
            The default value is false.
	    bidirectional_check : Boolean, optional
            If it is true, Giraph will firstly check whether each edge is bidirectional.
        num_iters : Integer, optional
            The number of CGD iterations in each super step. Larger value results in more
            accurate parameter estimation, but slows down the computation.
            The valid value range is all positive Integer.
            The default value is 5.

        Returns
        -------
        output : AlgorithmReport
            The algorithm's results in database.  The convergence curve is
            accessible through the report object.

        """
        output_path = os.path.join(global_config['giraph_output_base'], self._table_name, 'cgd')
        cgd_command = self._get_cgd_command(
            self._table_name,
            input_edge_property_list,
            input_edge_label,
            ','.join(output_vertex_property_list),
            global_config['hbase_column_family'] + vertex_type,
            global_config['hbase_column_family'] + edge_type,
            str(num_mapper),
            mapper_memory,
            str(vector_value).lower(),
            str(num_worker),
            str(max_supersteps),
            str(feature_dimension),
            str(cgd_lambda),
            str(convergence_threshold),
            str(learning_output_interval),
            str(max_val),
            str(min_val),
            str(bias_on).lower(),
            str(num_iters),
            str(bidirectional_check).lower(),
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

        output = AlgorithmReport()
        output.method = 'cgd'
        output.graph_name = self._graph.user_graph_name
        output.start_time = time_str
        output.exec_time = str(exec_time) + ' seconds'
        output.input_edge_property_list = input_edge_property_list
        output.input_edge_label = input_edge_label
        output.output_vertex_property_list = output_vertex_property_list
        output.vertex_type = vertex_type
        output.edge_type = edge_type
        output.num_mapper = num_mapper
        output.mapper_memory = mapper_memory
        output.vector_value = vector_value
        output.num_worker = num_worker
        output.max_supersteps = max_supersteps
        output.feature_dimension = feature_dimension
        output.cgd_lambda = cgd_lambda
        output.convergence_threshold = convergence_threshold
        output.learning_output_interval = learning_output_interval
        output.max_val = max_val
        output.min_val = min_val
        output.bias_on = bias_on
        output.bidirectional_check = bidirectional_check
        output.num_iters = num_iters
        output_path = os.path.join(output_path, 'cgd-learning-report_0')
        if hdfs.path.exists(output_path):
            cgd_results = self._update_learning_curve(output_path,
                                                      'CGD Learning Curve')
            output.super_steps = list(cgd_results[0])
            output.cost_train = list(cgd_results[1])
            output.rmse_validate = list(cgd_results[2])
            output.rmse_test = list(cgd_results[3])
            output.num_vertices = cgd_results[4]
            output.num_edges = cgd_results[5]
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
            num_mapper,
            mapper_memory,
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
                global_config['giraph_param_number_mapper'] + str(num_mapper),
                global_config['giraph_param_mapper_memory'] + mapper_memory,
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
    pass


job_completion_pattern = re.compile(r".*?Giraph Stats")
groovy_completion_pattern = re.compile(r"complete execution")
groovy_completion_pattern2 = re.compile(r".*?Job complete:")

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


class GroovyProgressReportStrategy(ReportStrategy):
    """
    The progress report strategy for gremlin query related task
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
        if re.match(groovy_completion_pattern, line) or re.match(groovy_completion_pattern2, line):
            self.progress_bar._disable_animation()

    def handle_error(self, error_code, error_message):
        """
        turn the progress bar to red if there is error during execution
        """
        self.progress_bar.alert()
