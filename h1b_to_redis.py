# -*- coding: utf-8 -*-
"""
Imports h1b data from a CSV file into redis

h1b_kaggle.csv contains ~3 million h1b applications. This script 
will create ~4 million keys in redis.

Motivation:
-----------
This script is a part of redis-rdb-tools, 
and helps us generate realistic dump files to help in testing

Pre-requisites:
---------------
1. Download h1b_kaggle.csv from https://www.kaggle.com/nsharan/h-1b-visa. 
    (you need to create a kaggle account to download this file)
2. Place h1b_kaggle.csv in the same directory as this python script
3. Ensure redis is running on localhost:6379 without a password
    (Or, adjust the connection string in the __main__ section at the bottom)


Keyspace Details:
-----------------

1. applications:<id> -> a hash containing all details about the application
2. applications:_location -> a geo data strucutre that is a mapping 
        from application id -> (long, lat)
3. employers:<id> -> a hash containing employer details
4. employers:<id>:wages_by_application -> sorted set. 
        Contains a mapping from applicationid to the wage
        All applications  in this sorted set belong to this particular employer
5. employer_name_to_id -> hash from employer name to employer id
        Used to lookup employer's id
6. employer_id_counter -> a string used to generate unique ids
7. soc:<id> -> a hash containing soc details
8. soc:<id>:wages_by_application -> sorted set. 
        Contains a mapping from applicationid to the wage.
        All applications  in this sorted set belong to this particular SOC
9. soc_name_to_id -> hash from soc name to soc id
        Used to lookup soc's id
10. soc_id_counter -> a string used to generate unique ids
11. jobs:<id> -> a hash containing job details
12. jobs:<id>:wages_by_application -> sorted set. 
        Contains a mapping from jobid to the wage.
        All applications in this sorted set belong to this particular job
13. job_name_to_id -> hash from job name to job id
        Used to lookup job's id
14. job_id_counter -> a string used to generate unique ids

Implementation Details:
-----------------------
1. The script uses pipelining, but is single threaded. 
    It should be possible to increase throughput by making it multiple threaded
2. To insert jobs/soc/employers, we use a lua script. 
    Generating a unique id requires incremeing and reading from a redis key. 
    Since pipelining does not allow us to mix read and writes, we insert the data
    as part of a lua script. The lua script itself is run as part of the pipeline.

TODOs:
------
1. Accept command line parameters for csv file and redis connection parameters
2. The lua script for job/soc/employer is mostly identical, and can be genearalized

"""
import csv
import itertools

from collections import namedtuple
import redis
try:
    from itertools import izip_longest
except:
    from itertools import zip_longest as izip_longest


Application = namedtuple('Application', ['id', 'case_status', 'employer_name', 'soc_name',
    'job_title', 'full_time_position', 'prevailing_wage', 'year', 'worksite', 'long', 'lat'])

def _coerce_float(val, default=None):
    try:
        return float(val)
    except:
        return default

def parse_applications(file):
    with open(file, 'r') as csvfile:
        is_header_row = True
        for row in csv.reader(csvfile):
            if is_header_row:
                is_header_row = False
                continue
            
            row[6] = _coerce_float(row[6], default=-1) #salary
            row[9] = _coerce_float(row[9]) #long
            row[10] = _coerce_float(row[10]) # lat

            application = Application._make(row)
            if application.id:
                yield application

def get_batches(iterable, batch_size, fillvalue=None):
    args = [iter(iterable)] * batch_size
    return izip_longest(fillvalue=fillvalue, *args)

def import_in_batches(objs, callback, red):
    for batch in get_batches(objs, 100):
        pipe = red.pipeline()
        for obj in batch:
            if obj:
                callback(obj, pipe)
        pipe.execute()

def save_application(application, pipe):
    # applications:id -> application details
    # application_location -> geo add applicationid to (long, lat) 
    if application.lat and application.long:
        pipe.geoadd("application_location", application.long, application.lat, application.id)
    pipe.hmset("applications:%s" % application.id, application._asdict())

    _save_employer(application, pipe)
    _save_soc(application, pipe)
    _save_job(application, pipe)

def _load_save_employer_script(red):
    # KEYS=["employer_name_to_id", "employer_id_counter"]
    # ARGS=["<employer name>", "<applicationid>", <salary>]
    script = """
        local EMPLOYER_NAME_TO_ID = KEYS[1]
        local EMPLOYER_ID_COUNTER = KEYS[2]
        local employer_name = ARGV[1]
        local applicationid = ARGV[2]
        local salary = ARGV[3]
        local employerid = -1
        if redis.call("hexists", EMPLOYER_NAME_TO_ID, employer_name) == 1 then
            employerid = redis.call("hget", EMPLOYER_NAME_TO_ID, employer_name)
        else
            employerid = redis.call("incrby", EMPLOYER_ID_COUNTER, 1)
            redis.call("hset", EMPLOYER_NAME_TO_ID, employer_name, employerid)
            redis.call("hmset", "employers:"..employerid, "name", employer_name, "id", employerid)
        end
        redis.call("zadd", "employers:"..employerid..":wages_by_application", salary, applicationid)
    """
    return red.register_script(script)

def _save_employer(application, pipe):
    # employers:id -> employer details
    # employers:id:wages_by_application -> sorted set, applicationid : salary
    # employer_name_to_id -> hash from employee name to employee id
    # employer_id_counter -> string, increment
    save_employer_script(
        keys=["employer_name_to_id", "employer_id_counter"],
        args=[application.employer_name, application.id, application.prevailing_wage],
        client=pipe)

def _load_save_soc_script(red):
    # KEYS=["soc_name_to_id", "soc_id_counter"]
    # ARGS=["<soc name>", "<applicationid>", <salary>]
    script = """
        local SOC_NAME_TO_ID = KEYS[1]
        local SOC_ID_COUNTER = KEYS[2]
        local soc_name = ARGV[1]
        local applicationid = ARGV[2]
        local salary = ARGV[3]
        local socid = -1
        if redis.call("hexists", SOC_NAME_TO_ID, soc_name) == 1 then
            socid = redis.call("hget", SOC_NAME_TO_ID, soc_name)
        else
            socid = redis.call("incrby", SOC_ID_COUNTER, 1)
            redis.call("hset", SOC_NAME_TO_ID, soc_name, socid)
            redis.call("hmset", "soc:"..socid, "name", soc_name, "id", socid)
        end
        redis.call("zadd", "soc:"..socid..":wages_by_application", salary, applicationid)
    """
    return red.register_script(script)

def _save_soc(application, pipe):     
    # soc:id -> soc details
    # soc:id:wages_by_application -> sorted set, applicationid : salary
    # soc_name_to_id -> hash from soc name to soc id
    # soc_id_counter -> string, increment
    save_soc_script(
        keys=["soc_name_to_id", "soc_id_counter"],
        args=[application.soc_name, application.id, application.prevailing_wage],
        client=pipe)

def _load_save_job_script(red):
    # KEYS=["job_name_to_id", "job_id_counter"]
    # ARGS=["<job name>", "<applicationid>", <salary>]
    script = """
        local JOB_NAME_TO_ID = KEYS[1]
        local JOB_ID_COUNTER = KEYS[2]
        local job_name = ARGV[1]
        local applicationid = ARGV[2]
        local salary = ARGV[3]
        local jobid = -1
        if redis.call("hexists", JOB_NAME_TO_ID, job_name) == 1 then
            jobid = redis.call("hget", JOB_NAME_TO_ID, job_name)
        else
            jobid = redis.call("incrby", JOB_ID_COUNTER, 1)
            redis.call("hset", JOB_NAME_TO_ID, job_name, jobid)
            redis.call("hmset", "jobs:"..jobid, "name", job_name, "id", jobid)
        end
        redis.call("zadd", "jobs:"..jobid..":wages_by_application", salary, applicationid)
    """
    return red.register_script(script)

def _save_job(application, pipe):
    # jobs:id -> job details
    # jobs_salary_range -> sorted set, applicationid: salary
    # job_mapping -> hash from job title to id 
    # job_counter -> string increment
    save_job_script(
        keys=["job_name_to_id", "job_id_counter"],
        args=[application.job_title, application.id, application.prevailing_wage],
        client=pipe)

def import_applications(h1bfile, red):
    applications = parse_applications(h1bfile)
    #applications = itertools.islice(applications, 100)
    import_in_batches(applications, save_application, red)

if __name__ == '__main__':
    red = redis.StrictRedis()
    save_employer_script = _load_save_employer_script(red)
    save_soc_script = _load_save_soc_script(red)
    save_job_script = _load_save_job_script(red)
    import_applications("h1b_kaggle.csv", red)
