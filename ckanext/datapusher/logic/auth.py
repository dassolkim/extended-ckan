# encoding: utf-8

import ckanext.datastore.logic.auth as auth
import ckan.plugins as p

def datapusher_submit(context, data_dict):
    return auth.datastore_auth(context, data_dict)


def datapusher_status(context, data_dict):
    return auth.datastore_auth(context, data_dict)

def datapusher_hook(context, data_dict):
    return {'success': True}

def resource_upload(context, data_dict):
    # return p.toolkit.check_access('resource_show', context, data_dict)
    return {'success': True}
