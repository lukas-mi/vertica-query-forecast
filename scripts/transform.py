import os
import shutil
import sqlparse
from subprocess import Popen, PIPE, STDOUT


QUERIES_PATH = os.getenv('QUERIES_PATH')
CATALOG_PATH = os.getenv('CATALOG_PATH')
ANALYSER_PATH = os.getenv('VQ_ANALYSER_PATH')


def has_sub(original, sub):
    return original.lower().find(sub) >= 0


def is_one_of(original, *others):
    return any(original.lower() == o for o in others)


CONTENT_FILTERS = [
    lambda c: has_sub(c, 'where user_name = current_user()'),
    lambda c: has_sub(c, 'query_profiles'),
    lambda c: has_sub(c, 'query_requests'),
    lambda c: has_sub(c, 'set session autocommit to'),
    lambda c: has_sub(c, 'show search_path'),
    lambda c: has_sub(c, 'v_catalog'),
    lambda c: has_sub(c, 'v_monitor'),
    lambda c: has_sub(c, 'set session characteristics as transaction read write'),
    lambda c: has_sub(c, 'select 1 /*jdbc connection.isvalid() check*/'),
    lambda c: has_sub(c, 'set session characteristics as transaction read only'),
    lambda c: is_one_of(c, 'select 1', 'select 1;'),
    lambda c: is_one_of(c, 'rollback', 'rollback;'),
    lambda c: is_one_of(c, 'commit', 'commit;'),
]


EXCLUDED_USERS = []


def remove_excluded_dir(queries_dir):
    is_excluded = any(queries_dir.find(excluded) > -1 for excluded in EXCLUDED_USERS)
    if is_excluded:
        shutil.rmtree(queries_dir)
        return True
    else:
        return False


def filter_queries(queries_dir):
    all_count = 0
    filtered_count = 0

    paths = (os.path.join(queries_dir, fn) for fn in os.listdir(queries_dir) if fn.endswith(".sql"))
    for fp in paths:
        with open(fp, 'r') as f:
            content = f.read()
            match = any(cf(content) for cf in CONTENT_FILTERS)

        if match:
            os.remove(fp)
            if os.path.isfile(fp + '.json'):
                os.remove(fp + '.json')
            if os.path.isfile(fp.split('.sql')[0] + '.json'):
                os.remove(fp.split('.sql')[0] + '.json')
            filtered_count += 1

        all_count += 1

    print '\tfiltered {}/{}'.format(filtered_count, all_count)


def format_queries(queries_dir):
    paths = (os.path.join(queries_dir, fn) for fn in os.listdir(queries_dir) if fn.endswith(".sql"))
    for fp in paths:
        with open(fp, 'r+') as f:
            content = f.read()
            formatted_content = sqlparse.format(content, reindent=True, keyword_case='upper')

            f.seek(0)
            f.write(formatted_content.encode('utf-8'))
            f.truncate()

    print '\tformatted sql files'


def analyse_queries(dir_to_analyze):
    process = Popen([ANALYSER_PATH, CATALOG_PATH, '-d', dir_to_analyze], stdout=PIPE, stderr=PIPE, env=os.environ)
    out, err = process.communicate()

    if err:
        print err
        exit(1)
    else:
        print '\t{}'.format(out.strip())


def format_jsons(dir_with_jsons):
    paths = (os.path.join(dir_with_jsons, fn) for fn in os.listdir(dir_with_jsons) if fn.endswith(".sql.json"))
    for fp in paths:
        with open(fp, 'r+') as f:
            content = f.read()
            formatted_json, err = Popen(['jq', '.'], stdout=PIPE, stdin=PIPE, stderr=STDOUT).communicate(content)
            if err:
                print err
                exit(1)

            f.seek(0)
            f.write(formatted_json)
            f.truncate()

    print '\tformatted json files'


def analyse_path(queries_path):
    filter_queries(queries_path)
    format_queries(queries_path)
    analyse_queries(queries_path)
    format_jsons(queries_path)


def main():
    for root, dirs, files in os.walk(QUERIES_PATH):
        abs_root = os.path.abspath(root)

        if remove_excluded_dir(abs_root):
            continue
        if len(files) == 0:
            continue

        print abs_root

        analyse_path(abs_root)


if __name__ == '__main__':
    main()
