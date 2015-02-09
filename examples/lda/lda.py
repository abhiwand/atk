import intelanalytics as ia

ia.connect()


print("define csv file")
csv = ia.CsvFile("/lda.csv", schema= [('doc_id', str),
                                        ('word_id', str),
                                        ('word_count', ia.int64)], skip_header_lines=1)
print("create big frame")
frame = ia.Frame(csv)

print("inspect frame")
frame.inspect(20)
print("frame row count " + str(frame.row_count))

model = ia.LdaModel()
results = model.train(frame,
            'doc_id', 'word_id', 'word_count',
            max_supersteps = 3,
            num_topics = 2)

doc_results = results['doc_results']
word_results = results['word_results']
report = results['report']

doc_results.inspect()
word_results.inspect()
print report

