""" Salesforce Bulk API Client """

from itertools import islice
import collections


import json
import polling
import requests


class DefaultTimeoutAdaptor(requests.adapters.HTTPAdapter):
    """ The default timeout adaptor allows us to specify a per session timeout. """

    def __init__(self, timeout=None, *args, **kwargs):
        if timeout is None:
            timeout = 10
        self.timeout = timeout
        super(DefaultTimeoutAdaptor, self).__init__(*args, **kwargs)

    def send(self, *args, **kwargs):  # pylint: disable=W0221
        kwargs['timeout'] = self.timeout
        return super(DefaultTimeoutAdaptor, self).send(*args, **kwargs)


SalesforceConnection = collections.namedtuple(
    'SalesforceConnection', ['session_id', 'instance_url', 'api_version'])


# TODO : should async_url take a session instead?
def async_url(connection):
    """ Get the base url for Bulk API """
    return "{instance_url}/services/async/{api_version}".format(
        instance_url=connection.instance_url, api_version=connection.api_version)


def session(connection, content_type=None):
    """ Configures and returns a requests.Session for the supplied connection """
    if content_type is None:
        content_type = 'application/json'

    rv = requests.Session()
    rv.headers.update(
        {'Content-Type': content_type, 'X-SFDC-Session': connection.session_id})
    rv.mount('https://', DefaultTimeoutAdaptor())
    return rv


def new_job(operation, sobject, connection, content_type=None):
    """ creates a new async job for the connection """

    if content_type is None:
        content_type = 'JSON'

    request_json = json.dumps(
        {'object': sobject, 'operation': operation, 'contentType': content_type})

    with session(connection) as sess:
        response = sess.post(
            url="{root}/job".format(root=async_url(connection)),
            data=request_json
        )

    return response.json()


def check_job(job_id, connection):
    """ get job status """

    with session(connection) as sess:
        response = sess.get(
            url="{root}/job/{job}".format(
                root=async_url(connection), job=job_id)
        )

    return response.json()


def close_job(job_id, connection):
    """ close a job, indicating no more batches will be added """

    request_json = json.dumps({'state': 'Closed'})
    with session(connection) as sess:
        response = sess.post(
            url="{root}/job/{job}".format(
                root=async_url(connection), job=job_id),
            data=request_json
        )

    return response.json()


def query(job_id, query_str, connection):
    """ add a query to a job """

    with session(connection) as sess:
        response = sess.post(
            url="{root}/job/{job}/batch".format(
                root=async_url(connection), job=job_id),
            data=query_str
        )

    return response.json()


def get_batches(job_id, connection):
    """ get all batchinfo """
    with session(connection) as sess:
        response = sess.get(
            url="{root}/job/{job}/batch".format(
                root=async_url(connection), job=job_id))

    return response.json()


def get_batch(job_id, batch_id, connection):
    """ get a single batchinfo """
    with session(connection) as sess:
        response = sess.get(
            url="{root}/job/{job}/batch/{batch}".format(
                root=async_url(connection), job=job_id, batch=batch_id))

    return response.json()


def get_results(job_id, batch_id, connection):
    """ get the results for a batch, returns results, or result IDs for a query """
    with session(connection) as sess:
        response = sess.get(
            url="{root}/job/{job}/batch/{batch}/result".format(
                root=async_url(connection), job=job_id, batch=batch_id))

    return response.json()


def get_request_data(job_id, batch_id, connection):
    """ get the request data that was submitted for a batch, which can be aligned with result data """
    with session(connection) as sess:
        response = sess.get(
            url="{root}/job/{job}/batch/{batch}/request".format(
                root=async_url(connection), job=job_id, batch=batch_id
            )
        )

    return response.json()


def get_query_results(job_id, batch_id, result_id, connection):
    """ get an individual result set for query results """
    with session(connection) as sess:
        response = sess.get(
            url="{root}/job/{job}/batch/{batch}/result/{result}".format(
                root=async_url(connection), job=job_id, batch=batch_id, result=result_id))

    return response.json()


def add_batch(job_id, records, connection):
    """ add a batch to an open job """
    with session(connection) as sess:
        response = sess.post(
            url="{root}/job/{job}/batch".format(
                root=async_url(connection), job=job_id
            ),
            data=json.dumps(records)
        )

    return response.json()

def get_job(job,conn):
    """ if this is a job_id, get the job dict """
    if not isinstance(job, collections.Mapping):
        job = check_job(job, conn)
    return job

def is_job_finished(job):
    """ checks if the job is finished """

    fin = job['numberBatchesCompleted'] + job['numberBatchesFailed']
    return fin == job['numberBatchesTotal']

def wait_for_job(job, conn):
    """ Wait until a job is complete """

    job = get_job(job, conn)

    return polling.poll(
        target=check_job,
        step=10,
        args=(job['id'], conn),
        poll_forever=True,
        check_success=is_job_finished
    )

def chunk(hunk, size):
    """ break an iterable into chunks of a given size """
    hunk = iter(hunk)
    return iter(lambda: tuple(islice(hunk, size)), ())

def add_records(job, conn, records, batch_size=2500):
    """ add an iterable of records to the job, in batches """
    job = get_job(job, conn)
    batches = []
    for batch in chunk(records, batch_size):
        batches.append(add_batch(job['id'], batch, conn))

    return batches

def get_records_from_mockaroo(schema, key, count=100):
    response = requests.get(
        'https://www.mockaroo.com/{s}/download?count={c}&key={k}'.format(
            s=schema, c=count, k=key))
    data = response.json()
    for sub in data:
        for key in sub:
            if sub[key] is not None:
                sub[key] = str(sub[key])

    return data

from cumulusci.tasks.salesforce import BaseSalesforceApiTask

class SuperData(BaseSalesforceApiTask):
    task_options = { 'count': {'description': 'number of records'}}

    def _run_task(self):
        conn = SalesforceConnection(self.org_config.access_token,self.org_config.instance_url,'39.0') 
        self.logger.info('loading data')
        data = get_records_from_mockaroo('3f0f6310','d86c50e0',self.options['count'])
        self.logger.info('creating job')
        job = new_job('insert','DataImport__c',conn)
        self.logger.info('loading records')
        add_records(job,conn,data)
        close_job(job['id'],conn)
        self.logger.info('waiting')
        res = wait_for_job(job,conn)
        self.logger.info(res)