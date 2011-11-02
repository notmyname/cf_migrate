#!/usr/bin/env python

from swift.common import client

################################
###### START CHANGE THESE ######

old_account_username = 'foo'
old_account_apikey = 'bar'

new_account_username = 'baz'
new_account_apikey = 'quux'

old_auth_endpoint = 'https://auth.api.rackspacecloud.com/v1.0'
new_auth_endpoint = 'https://auth.api.rackspacecloud.com/v1.0'

OLD_SNET = True
NEW_SNET = True

concurrency = 20

###### END CHANGE THESE #####
#############################

old_conn = client.Connection(old_auth_endpoint, old_account_username,
            old_account_apikey, snet=OLD_SNET)
new_conn = client.Connection(new_auth_endpoint, new_account_username,
            new_account_apikey, snet=NEW_SNET)

old_account_headers, old_listing = old_conn.get_account(full_listing=True)
new_conn.post_account(old_account_headers)

all_old_objects = []
for container_info in old_listing:
    container_name = container_info['name'].encode('utf8')
    old_container_headers, obj_listing = old_conn.get_container(container_name,
                                            full_listing=True)
    new_conn.put_container(container_name, old_container_headers)
    for obj_info in obj_listing:
        obj_name = obj_info['name'].encode('utf8')
        all_old_objects.append((container_name, obj_name))


def copy_objects(object_list):
    old_conn = client.Connection(old_auth_endpoint, old_account_username,
                old_account_apikey, snet=OLD_SNET)
    new_conn = client.Connection(new_auth_endpoint, new_account_username,
                new_account_apikey, snet=NEW_SNET)
    for container_name, object_name in object_list:
        chunk_size = 65535
        headers, body_obj = old_conn.get_object(container_name, object_name,
                        resp_chunk_size=chunk_size)
        etag = headers['etag']
        content_length = headers['content-length']
        content_type = headers['content-type']
        new_conn.put_object(container_name, object_name, body_obj, etag=etag,
                            chunk_size=chunk_size,
                            content_length=content_length,
                            content_type=content_type,
                            headers=headers)

len_object_list = len(all_old_objects)
pool = eventlet.GreenPool(size=concurrency)
[pool.spawn(copy_objects, object_list[concurrency * i:concurrency * (i + 1)])
    for i in xrange(len_object_list / concurrency)]
pool.waitall()
