"""
The UDF developer is responsible for handling the corner cases and exceptions. If we throw an exception within this UDF the execution will just stop.
"""
@outputSchema("squared:long")
def square(num):
    if num == None:#missing input
        return ""
        #raise Exception('missing input')
    
    if isinstance(num, unicode): 
        num_str = num.encode('utf-8')
        print ">>>>>>",type(num_str),num_str
        if num_str =='':
            raise Exception('empty')
#             print 'returning empty' 
#             return ''
        else:
            try:
                num_f = float(num_str)
            except:
                print 'caught exception'
                return None
            print 'returning',num_f*num_f
            return num_f*num_f
