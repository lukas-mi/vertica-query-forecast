import os
import json
import vertica_python

CON_INFO = {
    'host': os.getenv('VERTICA_HOST'),
    'port': os.getenv('VERTICA_PORT'),
    'user': os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
    'database': os.getenv('VERTICA_DATABASE'),
    'read_timeout': 120
}

QUERIES_PATH = os.getenv("QUERIES_PATH")


def main():
    select_query = """
    SELECT
        query,
        user_name,
        session_id,
        query_start_epoch,
        query_start::DATE query_start_date,
        query_start,
        (query_duration_us / 1e3)::INT query_duration
    FROM
        query_profiles
    WHERE
        query_type = 'QUERY' AND
        query ILIKE 'select%' AND
        error_code IS NULL
    ;"""

    con = vertica_python.connect(**CON_INFO)
    cursor = con.cursor('dict')

    print "querying Vertica..."
    cursor.execute(select_query)

    grouped_queries = {}

    print "iterating through queries..."
    for row in cursor.iterate():
        query = row['query']

        user_name = row['user_name']
        query_start_date = row['query_start_date']

        date_queries = grouped_queries.get(query_start_date)
        if not date_queries:
            date_queries = {}
            grouped_queries[query_start_date] = date_queries

        user_queries = date_queries.get(user_name)
        if not user_queries:
            user_queries = []
            date_queries[user_name] = user_queries

        user_queries.append({
            'query': query,
            'query_start_epoch': row['query_start_epoch'],
            'query_start': row['query_start'],
            'query_duration': row['query_duration'],
            'session_id': row['session_id']
        })

    cursor.close()
    con.close()

    print "creating files..."
    os.mkdir(QUERIES_PATH)
    for (date, date_queries) in grouped_queries.iteritems():
        date_dir_path = '{}/{}'.format(QUERIES_PATH, date)
        os.mkdir(date_dir_path)

        for (user_name, user_queries) in date_queries.iteritems():
            query_count = str(len(user_queries))
            query_count_padded = '0' * (7 - len(query_count)) + query_count
            user_dir_path = '{}/{}_{}'.format(date_dir_path, query_count_padded, user_name)
            os.mkdir(user_dir_path)

            for query_info in user_queries:
                query_file_path = '{}/{}.sql'.format(user_dir_path, query_info['query_start_epoch'])
                query_meta_path = '{}/{}.json'.format(user_dir_path, query_info['query_start_epoch'])

                if not os.path.exists(query_file_path):
                    with open(query_file_path, 'w+') as f:
                        try:
                            f.write(query_info['query'].encode('utf-8'))
                        except Exception as e:
                            print e
                            print query_info['query']
                            print query_info['query'].encode('utf-8')
                    with open(query_meta_path, 'w+') as f:
                        f.write(json.dumps({
                            'session_id': query_info['session_id'],
                            'query_start': query_info['query_start'],
                            'query_duration': query_info['query_duration'],
                            'epoch': query_info['query_start_epoch'],
                            'user_name': user_name
                        }, indent=2))

    print "done"


if __name__ == '__main__':
    main()
