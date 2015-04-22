##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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

import intelanalytics as ia

ia.connect()


print("define csv file")
csv = ia.CsvFile("/lda.csv", schema= [('doc_id', str),
                                        ('word_id', str),
                                        ('word_count', ia.int64)], skip_header_lines=1)
print("create frame")
frame = ia.Frame(csv)

print("inspect frame")
frame.inspect(20)
print("frame row count " + str(frame.row_count))

model = ia.LdaModel()
results = model.train(frame,
            'doc_id', 'word_id', 'word_count',
            max_iterations = 3,
            num_topics = 2)

doc_results = results['doc_results']
word_results = results['word_results']
report = results['report']

doc_results.inspect()
word_results.inspect()
print report

print("compute lda score")
doc_results.rename_columns({'lda_results' : 'lda_results_doc'})
word_results.rename_columns({'lda_results' : 'lda_results_word'})

frame= frame.join(doc_results, left_on="doc_id", right_on="doc_id", how="left")
frame= frame.join(word_results, left_on="word_id", right_on="word_id", how="left")

frame.dot_product(['lda_results_doc'], ['lda_results_word'], 'lda_score')
frame.inspect()

print("compute histogram of scores")
word_hist = frame.histogram('word_count')
lda_hist = frame.histogram('lda_score')
group_frame = frame.group_by('word_id_L', {'word_count': ia.agg.histogram(word_hist.cutoffs), 'lda_score':  ia.agg.histogram(lda_hist.cutoffs)})
group_frame.inspect()
