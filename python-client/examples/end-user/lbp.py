#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


def run(path=r"datasets/lbp_edge.csv"):
    """
    The default home directory is hdfs://user/taproot all the sample data sets are saved to
    hdfs://user/taproot/datasets when installing through the rpm
    you will need to copy the data sets to hdfs manually otherwise and adjust the data set location path accordingly
    :param path: data set hdfs path can be full and relative path
    """
    import taprootanalytics as ta

    ta.connect()

    #csv schema definition
    schema = [("source", ta.int32),
              ("dest", ta.int32),
              ("weight", ta.float32),
              ("labels", Vector())]

    #csv schema definition
    csv = ta.CsvFile(path, schema, skip_header_lines=1)

    print "Building data frame 'lbp'"

    frame = ta.Frame(csv, "lbp")

    print "Done building data frame"

    print "Inspecting frame 'lbp'"

    print frame.inspect()

    print frame.loopy_belief_propagation("source", "dest", "weight", "labels")
