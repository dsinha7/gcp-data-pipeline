import apache_beam as beam 
import csv 
import logging

def addtimezone(lat, lon):
    try:
        import timezonefinder
        tf = timezonefinder.TimezoneFinder()
        tz = tf.timezone_at(lng=float(lon), lat=float(lat))
        if tz is None:
            tz = 'UTC'
        return ( lat, lon, tz)
    except ValueError:
        return(lat, lon, 'TIMEZONE') 

def as_utc(date, hhmm, tzone):
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False)
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return '' # empty string corresponds to canceled flights
    except ValueError as e:
        print('{} {} {}'.format(date, hhmm, tzone))
        raise e            
def add_24h_if_before(arrtime, deptime):
   import datetime
   if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
      adt = datetime.datetime.strptime(arrtime, '%Y-%m-%d %H:%M:%S')
      adt += datetime.timedelta(hours=24)
      return adt.strftime('%Y-%m-%d %H:%M:%S')
   else:
      return arrtime


def tz_correct(line, airport_tz):
    
    logging.info("%s",line)
    fields = line.split(',')
    #only if not a header line 
    if fields[0] != 'FL_DATE' and len(fields) == 27:

        dep_airport_id = fields[6]
        arr_airport_id = fields[10]
        dep_tz = airport_tz[dep_airport_id][2]
        arr_tz = airport_tz[arr_airport_id][2]
    #convert the arrival and depurture fields to UTC from the airport tz
    # this converion is happening inline      
        for f in [13,14,17]:
            fields[f] = as_utc(fields[0], fields[f], dep_tz)
        for f in [18, 20, 21]: #wheelson, crsarrtime, arrtime
            fields[f] = as_utc(fields[0], fields[f], arr_tz)

        for f in [17, 18, 20, 21]:
            fields[f] = add_24h_if_before(fields[f], fields[14])

        fields.extend(airport_tz[dep_airport_id])
        fields[-1] = str(dep_tz)
        fields.extend(airport_tz[arr_airport_id])
        fields[-1] = str(arr_tz)

        yield fields   

def get_next_event(fields):
    if len(fields[14]) > 0:
        event = list(fields) #copy 
        event.extend(['departed', fields[14]])
        for f in [16,17,18,19,21,22,25]:
          event[f] = ''  # not knowable at departure time
          yield event
    if len(fields[21]) > 0:
        event = list(fields)
        event.extend(['arrived', fields[21]])
        yield event
def run():
    import logging
    logging.getLogger().setLevel(logging.INFO)
    logging.info('starting the flow')
    with beam.Pipeline('DirectRunner') as pipeline:
        airports = (
            pipeline 
            | 'airports:read' >> beam.io.ReadFromText('small_airport_list.csv')
            | 'airports:fields' >> beam.Map(lambda line : next(csv.reader([line])))
            | 'airports:tz' >> beam.Map(lambda fields : (fields[0], addtimezone(fields[21], fields[26])))
        )
    logging.info('after the airports step')
    # (airports
    # | 'debug:format' >> beam.Map(lambda data : '{},{}'.format(data[0], ','.join(data[1])))
    # | 'debug:write_to_file' >> beam.io.WriteToText('extracted_airports')
    # )

    flights = (
        pipeline
        | 'flights:read' >> beam.io.ReadFromText('trips.csv')
        | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
    )

    #flights | 'debug:write' >> beam.io.WriteToText('all_flights')
    logging.info('after the flight tzcorrected step')
    (flights
    | 'flights:tostring' >> beam.Map(lambda fields: ','.join(fields))
    | 'flights:out' >> beam.io.textio.WriteToText('all_flights') 
    )

    events = flights | beam.FlatMap(get_next_event)

    (events
    | 'event.tostring' >> beam.Map(lambda fields: ','.join(fields))
    | 'events:out' >> beam.io.textio.WriteToText('all_events')    
    )  
    pipeline.run()

    logging.info('completed!')

if __name__ == "__main__":

    import logging
    logging.getLogger().setLevel(logging.INFO)
    logging.info('starting the flow')
    run()
    logging.info('completed!')
