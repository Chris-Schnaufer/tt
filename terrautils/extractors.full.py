"""Extractors

This module provides useful reference methods for extractors.
"""

import datetime
import time
import logging
import json
import os
import re
import requests
import utm
from urllib3.filepost import encode_multipart_formdata

from pyclowder.extractors import Extractor
from pyclowder.datasets import get_file_list, download_metadata as download_dataset_metadata
from terrautils.influx import Influx, add_arguments as add_influx_arguments
from terrautils.metadata import get_terraref_metadata, get_pipeline_metadata
from terrautils.sensors import Sensors, add_arguments as add_sensor_arguments
from terrautils.users import get_dataset_username


logging.basicConfig(format='%(asctime)s %(message)s')

DEFAULT_EXPERIMENT_JSON_FILENAME = 'experiment.json'

def add_arguments(parser):
    """Add command line arguments for extractors
    Args:
        parser(object): command line parser instance
    """

    # TODO: Move defaults into a level-based dict
    parser.add_argument('--clowderspace',
                        default=os.getenv('CLOWDER_SPACE', "58da6b924f0c430e2baa823f"),
                        help='sets the default Clowder space for creating new things')

    parser.add_argument('--overwrite',
                        default=False,
                        action='store_true',
                        help='enable overwriting of existing files')

    parser.add_argument('--debug', '-d', action='store_const',
                        default=logging.INFO, const=logging.DEBUG,
                        help='enable debugging (default=WARN)')

    parser.add_argument('--clowder_user',
                        default=os.getenv('CLOWDER_USER', "terrarefglobus+uamac@ncsa.illinois.edu"),
                        help='clowder user to use when creating new datasets')

    parser.add_argument('--clowder_pass',
                        default=os.getenv('CLOWDER_PASS', ''),
                        help='clowder password to use when creating new datasets')

    parser.add_argument('--experiment_json_file', nargs='?', dest='experiment_json_file',
                        default=os.getenv('EXPERIMENT_CONFIG', DEFAULT_EXPERIMENT_JSON_FILENAME),
                        help='Default name of experiment configuration file used to' \
                             ' provide additional processing information')

# pylint: disable=too-many-instance-attributes
class TerrarefExtractor(Extractor):
    """Base extractor for TERRA REF project"""

    def __init__(self):

        super(TerrarefExtractor, self).__init__()

        add_arguments(self.parser)
        add_sensor_arguments(self.parser)
        add_influx_arguments(self.parser)

        # Initialize instance variables
        self.bytes, self.created = (0, 0)
        self.clowderspace, self.clowder_user, self.clowder_pass = (None, None, None)
        self.debug, self.overwrite = (False, False)
        self.get_sensor_path, self.influx, self.logger, self.starttime, self.sensors = \
                                                        (None, None, None, None, None)
        self.dataset_metadata, self.terraref_metadata, self.experiment_metadata = \
                                                        (None, None, None)
        self.experiment_json_file = None

    # pylint: disable=arguments-differ
    def setup(self, base='', site='', sensor=''):

        super(TerrarefExtractor, self).setup()

        self.clowderspace = self.args.clowderspace
        self.debug = self.args.debug
        self.overwrite = self.args.overwrite
        self.clowder_user = self.args.clowder_user
        self.clowder_pass = self.args.clowder_pass
        self.experiment_json_file = self.args.experiment_json_file

        # pylint: disable=multiple-statements
        if not base: base = self.args.terraref_base
        if not site: site = self.args.terraref_site
        if not sensor: sensor = self.args.sensor
        # pylint: enable=multiple-statements

        logging.getLogger('pyclowder').setLevel(self.args.debug)
        logging.getLogger('__main__').setLevel(self.args.debug)
        self.logger = logging.getLogger("extractor")

        self.sensors = Sensors(base=base, station=site, sensor=sensor)
        self.get_sensor_path = self.sensors.get_sensor_path

        self.influx = Influx(self.args.influx_host, self.args.influx_port,
                             self.args.influx_db, self.args.influx_user,
                             self.args.influx_pass)
    # pylint: enable=arguments-differ

    @property
    def config_file_name(self):
        """Returns the name of the expected configuration file
        """
        return DEFAULT_EXPERIMENT_JSON_FILENAME if not self.experiment_json_file \
                                                else self.experiment_json_file

    @property
    def date_format_regex(self):
        """Returns array of regex expressions for different date formats
        """
        # We lead with the best formatting to use, add on the rest
        return [r'(\d{4}(/|-){1}\d{1,2}(/|-){1}\d{1,2})',
                r'(\d{1,2}(/|-){1}\d{1,2}(/|-){1}\d{4})'
               ]

    @property
    def dataset_metadata_file_ending(self):
        """ Returns the ending string of a dataset metadata JSON file name
        """
        return '_dataset_metadata.json'

    def start_check(self, resource):
        """Standard format for extractor logs on check_message."""
        self.logger.info("[%s] %s - Checking message." % (resource['id'], resource['name']))


    def start_message(self, resource):
        """Prepares the extractor for starting to process a message"""
        self.logger.info("[%s] %s - Processing message." % (resource['id'], resource['name']))
        self.starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.created = 0
        self.bytes = 0


    def end_message(self, resource):
        """Logs some extractor statistics when message processing is complete"""
        self.logger.info("[%s] %s - Done." % (resource['id'], resource['name']))
        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.influx.log(self.extractor_info['name'],
                        self.starttime, endtime,
                        self.created, self.bytes)


    def log_info(self, resource, msg):
        """Standard format for extractor logs regarding progress."""
        self.logger.info("[%s] %s - %s" % (resource['id'], resource['name'], msg))


    def log_error(self, resource, msg):
        """Standard format for extractor logs regarding errors/failures."""
        self.logger.error("[%s] %s - %s" % (resource['id'], resource['name'], msg))


    def log_skip(self, resource, msg):
        """Standard format for extractor logs regarding skipped extractions."""
        self.logger.info("[%s] %s - SKIP: %s" % (resource['id'], resource['name'], msg))

    # pylint: disable=too-many-arguments, too-many-branches
    def process_message(self, connector, host, secret_key, resource, parameters):
        """Preliminary handling of a message
        Keyword arguments:
            connector(obj): the message queue connector instance
            host(str): the URI of the host making the connection
            secret_key(str): used with the host API
            resource(dict): dictionary containing the resources associated with the request
            parameters(json): json object of the triggering message contents
        Notes:
            Loads dataset metadata if it's available. Looks for terraref metadata in the dataset
            metadata and stores a reference to that, if available. Looks for an experiment
            configuration file and loads that, if found.
        """
        # Setup to default value
        self.dataset_metadata = None
        self.terraref_metadata = None
        self.experiment_metadata = None

        try:
            # Find the meta data for the dataset and other files of interest
            dataset_file = None
            experiment_file = None
            for onefile in resource['local_paths']:
                if onefile.endswith(self.dataset_metadata_file_ending):
                    dataset_file = onefile
                elif os.path.basename(onefile) == self.config_file_name:
                    experiment_file = onefile
                if not dataset_file is None and not experiment_file is None:
                    break

            # If we don't have dataset metadata already, download it
            dataset_md = None
            if dataset_file is None:
                dataset_id = None
                if 'type' in resource:
                    if resource['type'] == 'dataset':
                        dataset_id = resource['id']
                    elif resource['type'] == 'file' and 'parent' in resource:
                        if 'type' in resource['parent'] and resource['parent']['type'] == 'dataset':
                            dataset_id = resource['parent']['id'] if 'id' in resource['parent'] \
                                                                                    else dataset_id
                if not dataset_id is None:
                    dataset_md = download_dataset_metadata(connector, host, secret_key, dataset_id)
            else:
                # Load the dataset metadata from disk
                dataset_md = load_json_file(dataset_file)

            # If we have terraref metadata then store it and dataset metadata for later use
            if not dataset_md is None:
                terraref_md = get_terraref_metadata(dataset_md)
                if terraref_md:
                    md_len = len(terraref_md)
                    if md_len > 0:
                        self.terraref_metadata = terraref_md

                md_len = len(dataset_md)
                if md_len > 0:
                    self.dataset_metadata = dataset_md
                    self.experiment_metadata = get_pipeline_metadata(dataset_md)

            # Now we load any experiment configuration file
            if not experiment_file is None:
                experiment_md = load_json_file(experiment_file)
                if experiment_md:
                    md_len = len(experiment_md)
                    if md_len > 0:
                        if 'pipeline' in experiment_md:
                            self.experiment_metadata = experiment_md['pipeline']
                        else:
                            self.experiment_metadata = experiment_md

        # pylint: disable=broad-except
        except Exception as ex:
            self.log_error(resource, "Exception caught while loading dataset metadata: " + str(ex))

    # pylint: disable=too-many-nested-blocks
    def extract_datestamp(self, date_string):
        """Extracts the timestamp from a string. The parts of a date can be separated by
           single hyphens or slashes ('-' or '/') and no white space.
        Keyword arguments:
            date_string(str): string to lookup a datestamp in. The first found datestamp is
            returned.
        Returns:
            The extracted datestamp as YYYY-MM-DD. Returns None if a date isn't found
        Notes:
            This function only cares if the timestamp looks correct. It doesn't try to figure
            out if year, month, and day have correct values. The found date string may be
            reformatted to match the expected return.
        """

        # Check the string
        if not date_string or not isinstance(date_string, basestring):
            return None

        date_string_len = len(date_string)
        if date_string_len <= 0:
            return None

        # Find a date
        for part in date_string.split(' - '):
            for form in self.date_format_regex:
                res = re.search(form, part)
                if res:
                    date = res.group(0).replace('/', '-')
                    # Check for hyphen in first 4 characters to see if we need to move things
                    if not '-' in date[:4]:
                        return date
                    else:
                        split_date = date.split('-')
                        if len(split_date) == 3:
                            return split_date[2] + "-" + split_date[1] + "-" + split_date[0]

        return None

    def find_datestamp(self, text=None):
        """Returns a date stamp
        Keyword arguments:
            text(str): optional text string to search for a date stamp
        Return:
            A date stamp in the format of YYYY-MM-DD. No checks are made to determine if the
            returned string is a valid date.
        Notes:
            The following places are searched for a valid date; the first date found is the one
            that's returned: the text parameter, the name of the dataset associated with the
            current message being processed, the JSON configuration file as defined by the
            config_file_name() property, the current GMT date.
        """
        datestamp = None

        if not text is None:
            datestamp = self.extract_datestamp(text)

        if datestamp is None and not self.dataset_metadata is None:
            if 'name' in self.dataset_metadata:
                datestamp = self.extract_datestamp(self.dataset_metadata['name'])

        if datestamp is None and not self.experiment_metadata is None:
            if 'observationTimeStamp' in self.experiment_metadata:
                datestamp = self.extract_datestamp(self.experiment_metadata['observationTimeStamp'])

        if datestamp is None:
            datestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d')

        return datestamp

    def get_username_with_base_path(self, host, key, dataset_id, base_path=None):
        """Looks up the name of the user associated with the dataset. If unable to find
           the user's name from the dataset, the clowder_user variable is used instead.
           If not able to find a valid user name, the string 'unknown' is returned.

        Keyword arguments:
            host(str): the partial URI of the API path including protocol ('/api' portion and
                       after is not needed); assumes a terminating '/'
            key(str): access key for API use
            dataset_id(str): the id of the dataset belonging to the user to lookup
            base_path(str): optional starting path which will have the user name appended
        Return:
            A list of user name and modified base_path.
            The user name as defined in get_dataset_username(), or the any specified clowder user
            name, or, finally, the string 'unknown'. Underscores replace whitespace, and invalid
            characters are changed to periods ('.').
            The base_path with the user name appended to it, or None if base_path is None
        """
        try:
            username = get_dataset_username(host, key, dataset_id)
        # pylint: disable=broad-except
        except Exception:
            username = None
        # pylint: enable=broad-except

        # If we don't have a name, see if a user name was specified at runtime
        if (username is None) and (not self.clowder_user is None):
            username = self.clowder_user.strip()
            un_len = len(username)
            if un_len <= 0:
                username = None

        # If we have an experiment configuation, look for a name in there
        if not self.experiment_metadata is None:
            if 'extractors' in self.experiment_metadata:
                ex_username = None
                if 'firstName' in self.experiment_metadata['extractors']:
                    ex_username = self.experiment_metadata['extractors']['firstName']
                if 'lastName' in self.experiment_metadata['extractors']:
                    ex_username = ex_username + ('' if ex_username is None else ' ') + \
                                                self.experiment_metadata['extractors']['lastName']
                if not ex_username is None:
                    username = ex_username

        # Clean up the string
        if not username is None:
            # pylint: disable=line-too-long
            # First replace with periods, them replace with underscores
            username = username.replace('/', '.').replace('\\', '.').replace('&', '.').replace('*', '.').replace("'", '.').replace('"', '.').replace('`', '.')
            username = username.replace('@', '.')

            username = username.replace(' ', '_').replace('\t', '_').replace('\r', '_')
            # pylint: enable=line-too-long
        else:
            username = 'unknown'

        # Build up the path if the caller desired that
        new_base_path = None
        if not base_path is None:
            new_base_path = os.path.join(base_path, username)
            new_base_path = new_base_path.rstrip('/')

        return (username, new_base_path)


# BASIC UTILS -------------------------------------
# pylint: disable=too-many-arguments
def build_metadata(clowderhost, extractorinfo, target_id, content, target_type='file', context=[]):
    """Construct extractor metadata object ready for submission to a Clowder file/dataset.

        clowderhost -- root URL of Clowder target instance (before /api)
        extractorname -- name of extractor, in extractors usually self.extractor_info['name']
        target_id -- UUID of file or dataset that metadata will be sent to
        content -- actual JSON contents of metadata
        target_type -- type of target resource, 'file' or 'dataset'
        context -- (optional) list of JSON-LD contexts
    """
    if context == []:
        context = ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"]

    content['extractor_version'] = extractorinfo['version']

    md = {
        # TODO: Generate JSON-LD context for additional fields
        "@context": context,
        "content": content,
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": clowderhost + ("" if clowderhost.endswith("/") else "/") + \
                            "api/extractors/"+ extractorinfo['name'],
            "version": extractorinfo['version'],
            "name": extractorinfo['name']
        }
    }

    if target_type == 'dataset':
        md['dataset_id'] = target_id
    else:
        md['file_id'] = target_id

    return md


def is_latest_file(resource):
    """Check whether the extractor-triggering file is the latest file in the dataset.

    This simple check should be used in dataset extractors to avoid collisions between 2+
    instances of the same extractor trying to process the same dataset simultaneously by
    triggering off of 2 different uploaded files.

    Note that in the resource dictionary, "triggering_file" is the file that triggered the
    extraction (i.e. latest file at the time of message generation), not necessarily the newest
    file in the dataset.
    """
    trig = None
    if 'triggering_file' in resource:
        trig = resource['triggering_file']
    elif 'latest_file' in resource:
        trig = resource['latest_file']

    if trig:
        latest_file = ""
        latest_time = "Sun Jan 01 00:00:01 CDT 1920"

        for f in resource['files']:
            try:
                create_time = datetime.datetime.strptime(f['date-created'],
                                                         "%a %b %d %H:%M:%S %Z %Y")
                latest_dt = datetime.datetime.strptime(latest_time,
                                                       "%a %b %d %H:%M:%S %Z %Y")

                if f['filename'] == trig:
                    trig_dt = create_time

                if create_time > latest_dt:
                    latest_time = f['date-created']
                    latest_file = f['filename']
            # pylint: disable=broad-except
            except Exception:
                return True

        return latest_file == trig or latest_time == trig_dt

    # If unable to determine triggering file, return True
    return True


def contains_required_files(resource, required_list):
    """Iterate through files in resource and check if all of required list is found."""
    for req in required_list:
        found_req = False
        for f in resource['files']:
            if f['filename'].endswith(req):
                found_req = True
        if not found_req:
            return False
    return True


def load_json_file(filepath):
    """Load contents of a .json file on disk into a JSON object.
    """
    try:
        with open(filepath, 'r') as jsonfile:
            return json.load(jsonfile)
    # pylint: disable=broad-except
    except Exception:
        logging.error('could not load .json file %s' % filepath)
        return None


def file_exists(filepath, max_age_mins=3):
    """Return True if a file already exists on disk.

    If the file is zero bytes in size, return False if the file is more than
    max_age minutes old.
    """

    if os.path.exists(filepath):
        if os.path.getsize(filepath) > 0:
            return True

        age_seconds = time.time() - os.path.getmtime(filepath)
        return age_seconds < (max_age_mins*60)

    return False

# CLOWDER UTILS -------------------------------------
# TODO: Remove redundant ones of these once PyClowder2 supports user/password
# pylint: disable=too-many-locals
def build_dataset_hierarchy(host, secret_key, clowder_user, clowder_pass, root_space,
                            season, experiment, root_coll_name, year='', month='', date='',
                            leaf_ds_name=''):
    """This will build collections if needed in parent space.

        Typical hierarchy:
        MAIN LEVEL 1 DATA SPACE IN CLOWDER
        - Season ("Season 6")
        - Experiment ("Sorghum BAP")
        - Root collection for sensor ("stereoRGB geotiffs")
            - Year collection ("stereoRGB geotiffs - 2017")
                - Month collection ("stereoRGB geotiffs - 2017-01")
                    - Date collection ("stereoRGB geotiffs - 2017-01-01")
                        - Dataset ("stereoRGB geotiffs - 2017-01-01__01-02-03-456")

        Omitting year, month or date will result in dataset being added to next level up.
    """
    if season:
        season_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                  season, parent_space=root_space)

        experiment_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                      experiment, season_collect,
                                                      parent_space=root_space)

        sensor_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                  root_coll_name, experiment_collect,
                                                  parent_space=root_space)
    elif experiment:
        experiment_collect = get_collection_or_create(host, secret_key, clowder_user,
                                                      clowder_pass, experiment,
                                                      parent_space=root_space)

        sensor_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                  root_coll_name, experiment_collect,
                                                  parent_space=root_space)
    else:
        sensor_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                  root_coll_name, parent_space=root_space)

    if year:
        # Create year-level collection
        year_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                "%s - %s" % (root_coll_name, year),
                                                sensor_collect, parent_space=root_space)
        #verify_collection_in_space(host, secret_key, year_collect, root_space)
        if month:
            # Create month-level collection
            month_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                     "%s - %s-%s" % (root_coll_name, year, month),
                                                     year_collect, parent_space=root_space)
            #verify_collection_in_space(host, secret_key, month_collect, root_space)
            if date:
                targ_collect = get_collection_or_create(host, secret_key, clowder_user,
                                                        clowder_pass, "%s - %s-%s-%s" % \
                                                            (root_coll_name, year, month, date),
                                                        month_collect, parent_space=root_space)
                #verify_collection_in_space(host, secret_key, targ_collect, root_space)
            else:
                targ_collect = month_collect
        else:
            targ_collect = year_collect
    else:
        targ_collect = sensor_collect

    target_dsid = get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, leaf_ds_name,
                                        targ_collect, root_space)
    #verify_dataset_in_space(host, secret_key, target_dsid, root_space)
    return target_dsid


def build_dataset_hierarchy_crawl(host, secret_key, clowder_user, clowder_pass, root_space,
                                  season=None, experiment=None, sensor=None, year=None, month=None,
                                  date=None, leaf_ds_name=None):
    """This will build collections if needed in parent space.

        Typical hierarchy:
        MAIN LEVEL 1 DATA SPACE IN CLOWDER
        - Season ("Season 6")
        - Experiment ("Sorghum BAP")
        - Root collection for sensor ("stereoRGB geotiffs")
            - Year collection ("stereoRGB geotiffs - 2017")
                - Month collection ("stereoRGB geotiffs - 2017-01")
                    - Date collection ("stereoRGB geotiffs - 2017-01-01")
                        - Dataset ("stereoRGB geotiffs - 2017-01-01__01-02-03-456")

        Omitting year, month or date will result in dataset being added to next level up.

        Start at the root collection and check children until we get to the final one.
    """
    if season and experiment and sensor:
        season_c = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, season,
                                            parent_space=root_space)
        experiment_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass,
                                                     root_space, season_c, experiment)
        sensor_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass,
                                                 root_space, experiment_c, sensor)
    elif sensor:
        sensor_c = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, sensor,
                                            parent_space=root_space)
    else:
        sensor_c = None

    if year:
        year_c_name = "%s - %s" % (sensor, year)
        year_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass,
                                               root_space, sensor_c, year_c_name)

        if month:
            month_c_name = "%s - %s-%s" % (sensor, year, month)
            month_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass,
                                                    root_space, year_c, month_c_name)

            if date:
                date_c_name = "%s - %s-%s-%s" % (sensor, year, month, date)
                targ_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass,
                                                       root_space, month_c, date_c_name)

            else:
                targ_c = month_c
        else:
            targ_c = year_c
    else:
        targ_c = sensor_c

    target_dsid = get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, leaf_ds_name,
                                        targ_c, root_space)
    return target_dsid

# pylint: disable=too-many-locals
def get_collection_or_create(host, secret_key, clowder_user, clowder_pass, cname,
                             parent_colln=None, parent_space=None):
    """Finds an existing collection, or creates it.
    Keyword arguments:
        host(str): path to host including protocol
        secret_key(str): security key for accessing collections on host
        clowder_user(str): username to use when creating a collection
        clowder_pass(str): password associated with the username parameter
        cname(str): the name of the collection to find or create
        parent_colln(str): parent collection ID
        parent_space(str): space to create the collection in
    Return:
        Returns the ID of the found collection if it already exists, or the ID of the new
        collection, if one is created
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/collections?key=%s&title=%s&exact=true" % (host, secret_key, cname)
    result = requests.get(url)
    result.raise_for_status()

    json_len = len(result.json())
    if json_len == 0:
        return create_empty_collection(host, clowder_user, clowder_pass, cname, "", parent_colln,
                                       parent_space)

    coll_id = result.json()[0]['id']
    if parent_colln:
        add_collection_to_collection(host, secret_key, parent_colln, coll_id)
    if parent_space:
        add_collection_to_space(host, secret_key, coll_id, parent_space)
    return coll_id

def get_child_collections(host, secret_key, collection_id):
    """Returns the child collection information for a parent collection
    Keyword arguments:
        host(str): path to host including protocol
        secret_key(str): security key for accessing collections on host
        collection_id(str): the ID of the parent collection
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    url = "%sapi/collections/%s/getChildCollections?key=%s" % (host, collection_id, secret_key)
    result = requests.get(url)
    result.raise_for_status()

    return result.json()

def create_empty_collection(host, clowder_user, clowder_pass, collectionname, description,
                            parentid=None, spaceid=None):
    """Create a new collection in Clowder.

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        collectionname -- name of new dataset to create
        description -- description of new dataset
        parentid -- id of parent collection
        spaceid -- id of the space to add dataset to
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    logger = logging.getLogger(__name__)

    if parentid:
        if spaceid:
            url = '%sapi/collections/newCollectionWithParent' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname,
                                                    "description": description,
                                                    "parentId": parentid, "space": spaceid}),
                                   auth=(clowder_user, clowder_pass))
        else:
            url = '%sapi/collections/newCollectionWithParent' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname,
                                                    "description": description,
                                                    "parentId": parentid}),
                                   auth=(clowder_user, clowder_pass))
    else:
        if spaceid:
            url = '%sapi/collections' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname,
                                                    "description": description,
                                                    "space": spaceid}),
                                   auth=(clowder_user, clowder_pass))
        else:
            url = '%sapi/collections' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname,
                                                    "description": description}),
                                   auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    collectionid = result.json()['id']
    logger.debug("collection id = [%s]", collectionid)

    return collectionid

def get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, dsname, parent_colln=None,
                          parent_space=None):
    """Returns the ID of an existing dataset or returns the ID of the created dataset
    Keyword arguments:
        host(str): path to host including protocol
        secret_key(str): security key for accessing collections on host
        clowder_user(str): username to use when creating a collection
        clowder_pass(str): password associated with the username parameter
        dsname(str): the name of the dataset to find or create
        parent_colln(str): parent collection ID
        parent_space(str): space to create the collection in
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/datasets?key=%s&title=%s&exact=true" % (host, secret_key, dsname)
    result = requests.get(url)
    result.raise_for_status()

    json_len = len(result.json())
    if json_len == 0:
        return create_empty_dataset(host, clowder_user, clowder_pass, dsname, "",
                                    parent_colln, parent_space)

    ds_id = result.json()[0]['id']
    if parent_colln:
        add_dataset_to_collection(host, secret_key, ds_id, parent_colln)
    if parent_space:
        add_dataset_to_space(host, secret_key, ds_id, parent_space)
    return ds_id

def create_empty_dataset(host, clowder_user, clowder_pass, datasetname, description,
                         parentid=None, spaceid=None):
    """Create a new dataset in Clowder.

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        datasetname -- name of new dataset to create
        description -- description of new dataset
        parentid -- id of parent collection
        spaceid -- id of the space to add dataset to
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    logger = logging.getLogger(__name__)

    url = '%sapi/datasets/createempty' % host

    if parentid:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid], "space": [spaceid]}),
                                   auth=(clowder_user, clowder_pass))
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid]}),
                                   auth=(clowder_user, clowder_pass))
    else:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "space": [spaceid]}),
                                   auth=(clowder_user, clowder_pass))
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname,
                                                    "description": description}),
                                   auth=(clowder_user, clowder_pass))

    result.raise_for_status()

    datasetid = result.json()['id']
    logger.debug("dataset id = [%s]", datasetid)

    return datasetid

def upload_to_dataset(connector, host, clowder_user, clowder_pass, datasetid, filepath):
    """Upload file to existing Clowder dataset.

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        datasetid -- the dataset that the file should be associated with
        filepath -- path to file
        check_duplicate -- check if filename already exists in dataset and skip upload if so
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    logger = logging.getLogger(__name__)

    for source_path in connector.mounted_paths:
        if filepath.startswith(connector.mounted_paths[source_path]):
            return _upload_to_dataset_local(connector, host, clowder_user, clowder_pass,
                                            datasetid, filepath)

    url = '%sapi/uploadToDataset/%s' % (host, datasetid)

    if os.path.exists(filepath):
        result = connector.post(url, files={"File": open(filepath, 'rb')},
                                auth=(clowder_user, clowder_pass))

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid

    logger.error("unable to upload file %s (not found)", filepath)
    return None

def _upload_to_dataset_local(connector, host, clowder_user, clowder_pass, datasetid, filepath):
    """Upload file POINTER to existing Clowder dataset. Does not copy actual file bytes.

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        datasetid -- the dataset that the file should be associated with
        filepath -- path to file
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    logger = logging.getLogger(__name__)
    url = '%sapi/uploadToDataset/%s' % (host, datasetid)

    if os.path.exists(filepath):
        # Replace local path with remote path before uploading
        for source_path in connector.mounted_paths:
            if filepath.startswith(connector.mounted_paths[source_path]):
                filepath = filepath.replace(connector.mounted_paths[source_path],
                                            source_path)
                break

        (content, header) = encode_multipart_formdata([
            ("file", '{"path":"%s"}' % filepath)
        ])
        result = connector.post(url, data=content, headers={'Content-Type': header},
                                auth=(clowder_user, clowder_pass))

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid

    logger.error("unable to upload local file %s (not found)", filepath)
    return None

def get_child_collections(host, secret_key, collectionid):
    """Get list of child collections in collection by UUID.

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        collectionid -- the collection to get children of
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    if collectionid:
        url = "%sapi/collections/%s/getChildCollections?key=%s" % (host, collectionid, secret_key)

        result = requests.get(url)
        result.raise_for_status()

        return json.loads(result.text)

    return []

def get_datasets(host, clowder_user, clowder_pass, collectionid):
    """Get list of datasets in collection by UUID.

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        collectionid -- the collection to get datasets of
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    url = "%sapi/collections/%s/datasets" % (host, collectionid)

    result = requests.get(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)

def delete_dataset(host, clowder_user, clowder_pass, datasetid):
    """Deletes a dataset

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a /
        datasetid -- the dataset to delete
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    url = "%sapi/datasets/%s" % (host, datasetid)

    result = requests.delete(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)

def delete_dataset_metadata(host, clowder_user, clowder_pass, datasetid):
    """Deletes the metadata of a dataset

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a /
        datasetid -- the dataset to delete meta data from
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    url = "%sapi/datasets/%s/metadata.jsonld" % (host, datasetid)

    result = requests.delete(url, stream=True, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)

def delete_collection(host, clowder_user, clowder_pass, collectionid):
    """Deletes a collection

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a /
        collectionid -- the collection to delete
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    url = "%sapi/collections/%s" % (host, collectionid)

    result = requests.delete(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)

def delete_dataset_metadata_in_collection(host, clowder_user, clowder_pass, collectionid,
                                          recursive=True):
    """Deletes the collection metadata

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a '/'
        collectionid -- the collection to delete metadata from
        recursive - set to True to delete metadata for all children collections
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    dslist = get_datasets(host, clowder_user, clowder_pass, collectionid)

    logging.info("deleting dataset metadata in collection %s" % collectionid)
    for ds in dslist:
        delete_dataset_metadata(host, clowder_user, clowder_pass, ds['id'])
    logging.info("completed %s datasets" % len(dslist))

    if recursive:
        childcolls = get_child_collections(host, clowder_user, clowder_pass, collectionid)
        for coll in childcolls:
            delete_dataset_metadata_in_collection(host, clowder_user, clowder_pass, coll['id'],
                                                  recursive)

def delete_datasets_in_collection(host, clowder_user, clowder_pass, collectionid, recursive=True,
                                  delete_colls=True):
    """Deletes collections and, depending upon the delete_colls parameter, deletes child
       collections

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a '/'
        collectionid -- the collection to delete metadata from
        recursive - set to True to delete metadata for all children collections
        delete_colls - set to True to delete the collections as well
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    dslist = get_datasets(host, clowder_user, clowder_pass, collectionid)

    logging.info("deleting datasets in collection %s" % collectionid)
    for ds in dslist:
        delete_dataset(host, clowder_user, clowder_pass, ds['id'])
    logging.info("completed %s datasets" % len(dslist))

    if recursive:
        childcolls = get_child_collections(host, clowder_user, clowder_pass, collectionid)
        for coll in childcolls:
            delete_datasets_in_collection(host, clowder_user, clowder_pass, coll['id'], recursive,
                                          delete_colls)

    if delete_colls:
        logging.info("deleting collection %s" % collectionid)
        delete_collection(host, clowder_user, clowder_pass, collectionid)

def create_empty_space(host, clowder_user, clowder_pass, space_name, description=""):
    """Create a new space in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    space_name -- name of new space to create
    """

    logger = logging.getLogger(__name__)

    url = '%sapi/spaces' % host
    result = requests.post(url, headers={"Content-Type": "application/json"},
                           data=json.dumps({"name": space_name, "description": description}),
                           auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    spaceid = result.json()['id']
    logger.debug("space id = [%s]", spaceid)

    return spaceid

def get_space_or_create(host, secret_key, clowder_user, clowder_pass, space_name):
    """Returns the ID of an existing space, or the ID of the created space

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        space_name - the name of the space to lookup or the name of the new space
    Exceptions:
        HTTPException: if there's a problem with making requests
    """

    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/spaces?key=%s&title=%s&exact=true" % (host, secret_key, space_name)
    result = requests.get(url)
    result.raise_for_status()

    json_len = len(result.json())
    if json_len == 0:
        return create_empty_collection(host, clowder_user, clowder_pass, space_name, "")

    return result.json()[0]['id']

def delete_file(host, secret_key, fileid):
    """Deletes a file

    Keyword arguments:
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        file_id - the ID of the file to delete
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    url = "%sapi/files/%s?key=%s" % (host, fileid, secret_key)
    result = requests.delete(url)
    result.raise_for_status()

def check_file_in_dataset(connector, host, secret_key, dsid, filepath, remove=False,
                          forcepath=False, replacements=[]):
    """Returns whether or not a file is in a dataset

    Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        dsid - the ID of the dataset the file resides in
        file_path - the path to the file to check
        remove - set to True to have a found file deleted from the dataset (default False)
        forcepath - set to True to have only the path portion of the file compared for a match
                    before the file is deleted (default False - file names must match)
        replacements - optional parameters consisting of a pair of strings that identify a search
                       replacement string to apply to the file path
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Replacements = [("L2","L1")]
    # Each tuple is checked replacing first element in filepath with second element for existing
    dest_files = get_file_list(connector, host, secret_key, dsid)

    replacement_len = len(replacements)
    if replacement_len > 0:
        for r in replacements:
            filepath = filepath.replace(r[0], r[1])

    for source_path in connector.mounted_paths:
        if filepath.startswith(connector.mounted_paths[source_path]):
            filepath = filepath.replace(connector.mounted_paths[source_path], source_path)

    filename = os.path.basename(filepath)

    found_file = False
    for f in dest_files:
        #pylint: disable=line-too-long
        if (not forcepath and f['filename'] == filename) or (forcepath and f['filepath'] == filepath):
            if remove:
                delete_file(host, secret_key, f['id'])
            found_file = True
        #pylint: enable=line-too-long

    return found_file

def ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, parent_space,
                                  parent_coll_id, child_name):
    """Check if named collection is among parent's children, and create if not found."""
    child_collections = get_child_collections(host, secret_key, parent_coll_id)
    for c in child_collections:
        if c['name'] == child_name:
            return str(c['id'])

    # If we didn't find it, create it
    return create_empty_collection(host, clowder_user, clowder_pass, child_name, "",
                                   parent_coll_id, parent_space)

def add_dataset_to_collection(host, secret_key, dataset_id, collection_id):
    """Adds a dataset to a collection
    Args:
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        dataset_id - the ID of the dataset to add to the collection
        collection_id - the ID of the collection to place the dataset into
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Didn't find space, so we must associate it now
    url = "%sapi/collections/%s/datasets/%s?key=%s" % (host, collection_id, dataset_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_dataset_to_space(host, secret_key, dataset_id, space_id):
    """Adds a dataset to a space
    Args:
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        dataset_id - the ID of the dataset to add to the collection
        space_id - the ID of the space to place the dataset into
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Didn't find space, so we must associate it now
    url = "%sapi/spaces/%s/addDatasetToSpace/%s?key=%s" % (host, space_id, dataset_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_collection_to_collection(host, secret_key, parent_coll_id, child_coll_id):
    """Adds a collection to a collection
    Args:
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        parent_coll_id - the ID of the parent collection
        child_coll_id - the ID of the collection to add to the parent collection
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Didn't find space, so we must associate it now
    url = "%sapi/collections/%s/addSubCollection/%s?key=%s" % \
                                                (host, parent_coll_id, child_coll_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_collection_to_space(host, secret_key, collection_id, space_id):
    """Adds a collection to a space
    Args:
        host -- the clowder host, including http and port, should end with a '/'
        secret_key - the key used to access the API
        collection_id - the ID of the collection to add to the space
        space_id - the ID of the space to add the collection to
    Exceptions:
        HTTPException: if there's a problem with making requests
    """
    # Didn't find space, so we must associate it now
    url = "%sapi/spaces/%s/addCollectionToSpace/%s?key=%s" % \
                                                (host, space_id, collection_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()


# PRIVATE -------------------------------------
def _get_bounding_box_with_formula(center_position, fov):
    """Convert scannerbox center position & sensor field-of-view to actual bounding box

        Linear transformation formula adapted from:
        https://terraref.gitbooks.io/terraref-documentation/content/user/geospatial-information.html

        Returns:
            tuple of coordinates as: (  lat (y) min, lat (y) max,
                                        long (x) min, long (x) max )
    """

    # Get UTM information from southeast corner of field
    SE_utm = utm.from_latlon(33.07451869, -111.97477775)
    utm_zone = SE_utm[2]
    utm_num = SE_utm[3]

    # TODO: Hard-coded
    # Linear transformation coefficients
    ay = 3659974.971; by = 1.0002; cy = 0.0078
    ax = 409012.2032; bx = 0.009; cx = - 0.9986
    lon_shift = 0.000020308287
    lat_shift = 0.000015258894

    # min/max bounding box x,y values
    y_w = center_position[1] + fov[1]/2
    y_e = center_position[1] - fov[1]/2
    x_n = center_position[0] + fov[0]/2
    x_s = center_position[0] - fov[0]/2
    # coordinates of northwest bounding box vertex
    Mx_nw = ax + bx * x_n + cx * y_w
    My_nw = ay + by * x_n + cy * y_w
    # coordinates if southeast bounding box vertex
    Mx_se = ax + bx * x_s + cx * y_e
    My_se = ay + by * x_s + cy * y_e
    # bounding box vertex coordinates
    bbox_nw_latlon = utm.to_latlon(Mx_nw, My_nw, utm_zone, utm_num)
    bbox_se_latlon = utm.to_latlon(Mx_se, My_se, utm_zone, utm_num)

    return (bbox_se_latlon[0] - lat_shift,
            bbox_nw_latlon[0] - lat_shift,
            bbox_nw_latlon[1] + lon_shift,
            bbox_se_latlon[1] + lon_shift)


def _search_for_key(metadata, key_variants):
    """Check for presence of any key variants in metadata. Does basic capitalization check.

        Returns:
        value if found, or None
    """
    val = None
    for variant in key_variants:
        if variant in metadata:
            val = metadata[variant]
        elif variant.capitalize() in metadata:
            val = metadata[variant.capitalize()]

    # If a value was found, try to parse as float
    if val:
        try:
            return float(val.encode("utf-8"))
        # pylint: disable=broad-except
        except Exception:
            return val

    return None
