import re
import tempfile
import zipfile
import os
import hashlib
import math
import copy
import requests
import six
import ckanapi
import ckanapi.datapackage

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from ckan.lib.plugins import DefaultTranslation
from ckan import model

### INLINED
# from tasks import update_zip
# import helpers
# import action


log = __import__('logging').getLogger(__name__)


class DownloadallPlugin(plugins.SingletonPlugin, DefaultTranslation):
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IActions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'downloadall')

    # IDomainObjectModification

    def notify(self, entity, operation):
        u'''
        Send a notification on entity modification.

        :param entity: instance of module.Package.
        :param operation: 'new', 'changed' or 'deleted'.
        '''
        if operation == 'deleted':
            return

        log.debug(u'{} {} \'{}\''
                  .format(operation, type(entity).__name__, entity.name))
        # We should regenerate zip if these happen:
        # 1 change of title, description etc (goes into package.json)
        # 2 add/change/delete resource metadata
        # 3 change resource data by upload (results in URL change)
        # 4 change resource data by remote data
        # BUT not:
        # 5 if this was just an update of the Download All zip itself
        #   (or you get an infinite loop)
        #
        # 4 - we're ignoring this for now (ideally new data means a new URL)
        # 1&2&3 - will change package.json and notify(res) and possibly
        #         notify(package) too
        # 5 - will cause these notifies but package.json only in limit places
        #
        # SO if package.json (not including Package Zip bits) remains the same
        # then we don't need to regenerate zip.
        if isinstance(entity, model.Package):
            enqueue_update_zip(entity.name, entity.id, operation)
        elif isinstance(entity, model.Resource):
            if entity.extras.get('downloadall_metadata_modified'):
                # this is the zip of all the resources - no need to react to
                # it being changed
                log.debug('Ignoring change to zip resource')
                return
            dataset = entity.related_packages()[0]
            enqueue_update_zip(dataset.name, dataset.id, operation)
        else:
            return

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'downloadall__pop_zip_resource': pop_zip_resource,
        }

    # IPackageController

    def before_index(self, pkg_dict):
        try:
            if u'All resource data' in pkg_dict['res_name']:
                # we've got a 'Download all zip', so remove it's ZIP from the
                # SOLR facet of resource formats, as it's not really a data
                # resource
                pkg_dict['res_format'].remove('ZIP')
        except KeyError:
            # this happens when you save a new package without a resource yet
            pass
        return pkg_dict

    # IActions

    def get_actions(self):
        actions = {}
        if plugins.get_plugin('datastore'):
            # datastore is enabled, so we need to chain the datastore_create
            # action, to update the zip when it is called
            actions['datastore_create'] = datastore_create
        return actions


def enqueue_update_zip(dataset_name, dataset_id, operation):
    # skip task if the dataset is already queued
    queue = DEFAULT_QUEUE_NAME
    jobs = toolkit.get_action('job_list')(
        {'ignore_auth': True}, {'queues': [queue]})
    if jobs:
        for job in jobs:
            if not job['title']:
                continue
            match = re.match(
                r'DownloadAll \w+ "[^"]*" ([\w-]+)', job[u'title'])
            if match:
                queued_dataset_id = match.groups()[0]
                if dataset_id == queued_dataset_id:
                    log.info('Already queued dataset: {} {}'
                             .format(dataset_name, dataset_id))
                    return

    # add this dataset to the queue
    log.debug(u'Queuing job update_zip: {} {}'
              .format(operation, dataset_name))

    toolkit.enqueue_job(
        update_zip, [dataset_id],
        title=u'DownloadAll {} "{}" {}'.format(operation, dataset_name,
                                               dataset_id),
        queue=queue)

## tasks.update_zip
def update_zip(package_id, skip_if_no_changes=True):
    '''
    Create/update the a dataset's zip resource, containing the other resources
    and some metadata.

    :param skip_if_no_changes: If true, and there is an existing zip for this
        dataset, it will compare a freshly generated package.json against what
        is in the existing zip, and if there are no changes (ignoring the
        Download All Zip) then it will skip downloading the resources and
        updating the zip.
    '''
    # TODO deal with private datasets - 'ignore_auth': True
    context = {'model': model, 'session': model.Session}
    dataset = toolkit.get_action('package_show')(context, {'id': package_id})
    log.debug('Updating zip: {}'.format(dataset['name']))

    datapackage, ckan_and_datapackage_resources, existing_zip_resource = \
        generate_datapackage_json(package_id)

    if skip_if_no_changes and existing_zip_resource and \
            not has_datapackage_changed_significantly(
                datapackage, ckan_and_datapackage_resources,
                existing_zip_resource):
        log.info('Skipping updating the zip - the datapackage.json is not '
                 'changed sufficiently: {}'.format(dataset['name']))
        return

    prefix = "{}-".format(dataset[u'name'])
    with tempfile.NamedTemporaryFile(prefix=prefix, suffix='.zip') as fp:
        write_zip(fp, datapackage, ckan_and_datapackage_resources)

        # Upload resource to CKAN as a new/updated resource
        local_ckan = ckanapi.LocalCKAN()
        fp.seek(0)
        resource = dict(
            package_id=dataset['id'],
            url='dummy-value',
            upload=fp,
            name=u'All resource data',
            format=u'ZIP',
            downloadall_metadata_modified=dataset['metadata_modified'],
            downloadall_datapackage_hash=hash_datapackage(datapackage)
        )

        if not existing_zip_resource:
            log.debug('Writing new zip resource - {}'.format(dataset['name']))
            local_ckan.action.resource_create(**resource)
        else:
            # TODO update the existing zip resource (using patch?)
            log.debug('Updating zip resource - {}'.format(dataset['name']))
            local_ckan.action.resource_patch(
                id=existing_zip_resource['id'],
                **resource)

## helpers.py
def pop_zip_resource(pkg):
    '''Finds the zip resource in a package's resources, removes it from the
    package and returns it. NB the package doesn't have the zip resource in it
    any more.
    '''
    zip_res = None
    non_zip_resources = []
    for res in pkg.get('resources', []):
        if res.get('downloadall_metadata_modified'):
            zip_res = res
        else:
            non_zip_resources.append(res)
    pkg['resources'] = non_zip_resources
    return zip_res

## action.py
@plugins.toolkit.chained_action  # requires CKAN 2.7+
def datastore_create(original_action, context, data_dict):
    # This gets called when xloader or datapusher loads a new resource or
    # data dictionary is changed. We need to regenerate the zip when the latter
    # happens, and it's ok if it happens at the other times too.
    result = original_action(context, data_dict)

    # update the zip
    if 'resource_id' in data_dict:
        res = model.Resource.get(data_dict['resource_id'])
        if res:
            dataset = res.related_packages()[0]
            plugin.enqueue_update_zip(dataset.name, dataset.id,
                                      'datastore_create')

    return result
