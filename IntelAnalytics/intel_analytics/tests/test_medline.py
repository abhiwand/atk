from intel_analytics.table.bigdataframe import get_frame_builder, get_frame_names, get_frame
import time
start_time = time.time()
fb = get_frame_builder()
csvfile = '/user/hadoop/data/e2e_recommend.csv'
medTerms1='/user/hadoop/medline/medline-terms-toy-1000rows-1.csv'
medTerms2='/user/hadoop/medline/medline-terms-toy-1000rows-2.csv'
medOntology='/user/hadoop/medline/mesh-ontology-5-levels.csv'
medTermSchema='article_id: chararray, year: int, term: chararray, qualifiers: chararray'
medOntoSchema='treenumber: chararray, type: chararray, term: chararray'

def get_load_medToy(name, filename, schema):
    try:
        mL = get_frame(name)
        print 'Loaded %s originally imported from %s' % (name, filename)
    except:
        print 'Frame %s not found, loading from %s...' % (name, filename)
        mL = fb.build_from_csv(frame_name=name,     \
                               file_name=filename,  \
                               schema=schema,       \
                               overwrite=True)
        print 'Imported %s imported from %s' % (name, filename)
    return mL


m1 = get_load_medToy('med1', medTerms1, medTermSchema)
m2 = get_load_medToy('med2', medTerms1, medTermSchema)
mO = get_load_medToy('medOnto', medOntology, medOntoSchema)

m1.inspect()
m2.inspect()
mO.inspect()

# test1
print 'Join %s and %s' % (medTerms1, medOntology)
mJ1 = fb.join_data_frame(m1, mO,        \
                        how='inner',    \
                        left_on='term', \
                        right_on='term',\
                        suffixes=['_l', '_r'], \
                        join_frame_name='medJoin1')
mJ1.inspect()

print 'Join %s and %s' % (medTerms2, medOntology)
mJ2 = fb.join_data_frame(m1, mO, \
                        how='inner', \
                        left_on='term', \
                        right_on='term', \
                        suffixes=['_l', '_r'], \
                        join_frame_name='medJoin2')
mJ2.inspect()
