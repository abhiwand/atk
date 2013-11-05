import re

@outputSchema("tuple(timestamp:chararray, event_type:chararray, method:chararray, duration:double, item_id:chararray, src_tms:chararray, dst_tms:chararray)")
def parseRecord(line):
    #TODO: may need to do some type checks here with the schema that the user specifies and the parsed values
    #otherwise we get class cast exceptions from pig with type mismatches
      return parseRawRecord(line)



#### USER PARSING LOGIC #####
keywords = ['eventType', 'itemid', 'duration', 'method', 'destContext', 'srcContext']#keys we are interested in

def parse_mediaPurchase(remaining_cols):
    #['source=vod', 'prog=-1', 'price={blocked}']
    parsed={}
    extracted = remaining_cols[0].split("?")[1].split("&")
    for e in extracted:
        cols = e.split("=")
        parsed[cols[0]]=cols[1]
    return parsed

def parse_gameOver(remaining_cols):
    parsed={}
    for e in remaining_cols:
        cols = e.split("=")
        parsed[cols[0]]=cols[1]
    return parsed

def parse_programChange(remaining_cols):
    parsed={}
    #print remaining_cols[0].split("&")
    #print remaining_cols[1].split("&")
    src_tms_id=None
    dest_tms_id=None

    cols= []
    try:
      if 'srcContext' in remaining_cols[0]:
          r = re.compile('\?tmsid=(.*?)&')
          m = r.search(remaining_cols[0])
          src_tms_id=None
          if m:
              src_tms_id=m.group(1)
      if 'destContext' in remaining_cols[1]:
          r = re.compile('\?tmsid=(.*?)&')
          m = r.search(remaining_cols[1])
          dest_tms_id=None
          if m:
              dest_tms_id=m.group(1)

          cols=remaining_cols[2].split("=")
    except:
  #         print "exception ", remaining_cols
        pass
    if len(cols)>0:#if we have the method field
      parsed[cols[0]]=cols[1]
    try:#try parsing the duration field
      cols=remaining_cols[3].split("=")
      parsed[cols[0]]=cols[1]
    except:
      pass
    parsed['src_tms'] = src_tms_id
    parsed['dst_tms'] = dest_tms_id
    return parsed

def parse_stop(remaining_cols):
    parsed={}
    for col in remaining_cols:
      index = col.find('=')
      key = col[0:index]
      value = col[index + 1:]
      parsed[key]=value
    return parsed

def parse_select(remaining_line):
    parsed={}
    src = None

    try:
        match_obj = re.compile('srcContext=(.*)(?:method)').search(remaining_line)
        if match_obj != None:
          src = match_obj.group(1).strip()
    except:
      pass
  
    try:
        for col in remaining_line.split():
          cols = col.split('=')
          key = cols[0]
          if key not in keywords:
            continue
          else:
            parsed[key]=cols[1]

#        cols= remaining_cols[len(remaining_cols)-1].split("=")
#        print ">>>> ", cols
#        parsed[cols[0]]=cols[1]
    except:
        pass

    
    parsed['src']=src
    return parsed

def parse_transport(remaining_cols):
    parsed={}
    src_tms_id=None
    dest_tms_id=None

    try:
        cols= remaining_cols[len(remaining_cols)-1].split("=")
        parsed[cols[0]]=cols[1]
    except:
        pass
    
    try:
        cols=remaining_cols[2].split("=")
        parsed[cols[0]]=cols[1]
    except:
        pass

    try:
      if 'srcContext' in remaining_cols[0]:
          r = re.compile('\?tmsid=(.*?)&')
          m = r.search(remaining_cols[0])
          if m:
              src_tms_id=m.group(1)

      if 'destContext' in remaining_cols[1]:
          r = re.compile('\?tmsid=(.*?)&')
          m = r.search(remaining_cols[1])
          if m:
              dest_tms_id=m.group(1)
    except:
      pass
    parsed['src_tms'] = src_tms_id
    parsed['dst_tms'] = dest_tms_id
    return parsed


def parse_play(remaining_cols):
    parsed={}
    item_id=None

    try:
      if 'itemid' in remaining_cols[0]:
#          print remaining_cols[0]
          r = re.compile('itemid=(.*)')
          m = r.search(remaining_cols[0])
          if m:
              item_id=m.group(1)
    except:
      pass
    parsed['item_id'] = item_id
    return parsed

def parseRawRecord(line):
    cols = line.split()
    ts = None
    event_type = None
    
    try:
        ts = cols[0].split("|")[0]
    except:
        pass
    
    try:
        event_type = cols[1]
        event_type=event_type.split("=")[1]
    except:
        pass    
    
    parsed={}    
    parsed['timestamp']=ts
    parsed['event_type']=event_type
    
    tmp={}
    if event_type=='mediaPurchase':
        tmp = parse_mediaPurchase(cols[2:])
    elif event_type=='gameOver':
        tmp = parse_gameOver(cols[2:])
    elif event_type=='programChange':
        tmp = parse_programChange(cols[2:])
    elif event_type=='stop':
        tmp = parse_stop(cols[2:])
    elif event_type=='select':
       #handle in a special way since there can be spaces within select records and we can't split the line and pass the cols to parse_select()
        m = re.compile('(?:eventType=select)').search(line)
        tmp = parse_select(line[m.end():].strip())
        #tmp = parse_select(cols[2:])
    elif event_type=='transportControl':
          tmp = parse_transport(cols[2:])
    elif event_type=='play':
          tmp = parse_play(cols[2:])
    parsed.update(tmp)
#     print parsed

    output=[]
    output.append(parsed.get('timestamp', None))
    output.append(parsed.get('event_type', None))
    output.append(parsed.get('method', None))
    
    duration = parsed.get('duration', None)
    if duration == None:
        output.append(None)
    else:
        output.append(float(duration))
#         output.append(duration)
    
    if event_type=='stop':
        output.append(parsed.get('itemid', None))#stop event has itemid field, not item_id like the other event types
    else:
        output.append(parsed.get('item_id', None))
        
    output.append(parsed.get('src_tms', None))
    output.append(parsed.get('dst_tms', None))
    
#     ts = parsed.get('timestamp', None)
#     event_type = parsed.get('event_type', None)
#     method = parsed.get('method', None)
#     duration = parsed.get('duration', None)
#     if duration != None:## will later standardize on this, so needs to be a float
#         duration = float(duration)
#     item_id = parsed.get('item_id', None)
#     src_tms = parsed.get('src_tms', None)
#     dst_tms = parsed.get('dst_tms', None)
#     t = (ts, event_type, method, duration, item_id, src_tms, dst_tms)
#     print ">>> ", t
#     return t
    return tuple(output)
