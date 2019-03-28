
## This code segment shows how we can ingest complex JSON data in functional way

# Get the schema as defined in the intended table
user_schema = self.sc.sql("select * from {table_name} limit 1".format(table_name=user_table)).schema


# Define a mapper function which will read through each lines and extract column values
def __initial_mapper(row):
    "Loop through columns to convert raw data to schema of table"
    user_fields = [f.name for f in user_schema]
    schema_dict = {i.name: i.dataType for i in user_schema}
    rd = row.asDict()

    data_json = {}
    result = {k: None for k in user_fields}

    if 'custom_attributes' in rd:
        data_json['custom_attributes'] = {}
        custom_attr_dict = rd['custom_attributes'].asDict()

        for k, v in custom_attr_dict.iteritems():
            if k in schema_dict:
                if schema_dict[k] == TimestampType():
                    temp_date = v
                    temp_date = "1970-01-01T00:00:00.000Z" if temp_date is None else temp_date
                    result[k] = isoparse(temp_date)
                else:
                    result[k] = v
            elif v is not None:
                data_json['custom_attributes'][k] = v

        del rd['custom_attributes']

    if 'custom_events' in rd:
        if rd['custom_events'] is not None:
            custom_events = []
            for i in rd['custom_events']:
                custom_events.append({
                    'name': i.name,
                    'first': i.first,
                    'last': i.last,
                    # 'count': i.count()
                })
            data_json['custom_events'] = custom_events
        del rd['custom_events']

    if 'user_aliases' in rd:
        user_aliases = []
        for u in rd['user_aliases']:
            user_aliases.append({
                'alias_label': u.alias_label,
                'alias_name': u.alias_name,
            })
        if user_aliases:
            if 'user_aliases' in user_fields:
                result['user_aliases'] = json.dumps(user_aliases)
            else:
                data_json['user_aliases'] = user_aliases
        del rd['user_aliases']

    for k in rd:
        if k in user_fields:
            result[k] = rd[k]
        elif rd[k] is not None:
            data_json[k] = rd[k]
    result['data_json'] = json.dumps(data_json)

    return Row(*user_fields)(*[result[k] for k in user_fields])

# read json files as rdd
braze_segment_data_rdd = self.sc.read.json(files)


# Now pass the rdd through the mapper function, coerce to the schema as in table and make a dataframe
braze_segment_data_df = braze_segment_data_rdd.rdd.map(__initial_mapper).toDF(schema=user_schema)

