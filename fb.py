import logging
import json
import re
import time
import sys
from s3util import S3
import glob
import os
import html
import requests
import reverse_geocode
from functools import partial

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)


class FacebookArchiveReader:
    """
        Read the posts from a Facebook info download
    """

    @staticmethod
    def read(archive_dir):
        files = glob.glob(archive_dir + "/posts/*")
        posts = []
        for file in files:
            if "posts" in os.path.basename(file):
                file_posts = FacebookArchiveReader.read_file(archive_dir, file)
                posts.extend(file_posts)
        return posts

    @staticmethod
    def fix_bad_fb_unicode(file):
        # Facebook doesn't dump UTF-8 characters correctly, so we have to work around it
        fix_escapes = partial(re.compile(rb'\\u00([\da-f]{2})').sub, lambda m: bytes.fromhex(m.group(1).decode()))
        raw = open(file, 'rb').read()
        content = fix_escapes(raw)
        return content

    @staticmethod
    def _sanitize(str):
        return html.unescape(str).replace('"', '')

    @staticmethod
    def _reverse_gcode(latitude, longitude):
        if latitude:
            cities = reverse_geocode.search([(latitude, longitude)])
            if cities:
                city = FacebookArchiveReader._sanitize(cities[0]['city'] + ", " + cities[0]['country'])
                return city
        return None

    @staticmethod
    def read_file(archive_dir, file):
        posts = []
        content = FacebookArchiveReader.fix_bad_fb_unicode(file)
        post_array = json.loads(content.decode('utf8'))
        for post in post_array:
            images = []
            places = []
            timestamp = post['timestamp']
            tags = post.get("tags")
            message = ""
            locations = []
            if post.get("data"):
                for item in post.get("data"):
                    if item.get("post"):
                        message = message + FacebookArchiveReader._sanitize(item["post"]) + "\n"
            if post.get("attachments"):
                attachments = post['attachments']
                for attachment in attachments:
                    if attachment.get("data"):
                        for item in attachment['data']:
                            if item.get('media'):
                                media = item['media']
                                description = media['description'] if media.get('description') else None
                                image_file = archive_dir + "/" + media["uri"]
                                if media.get('media_metadata') and media['media_metadata'].get('photo_metadata'):
                                    latitude = media['media_metadata']['photo_metadata'].get('latitude')
                                    longitude = media['media_metadata']['photo_metadata'].get('longitude')
                                    orientation = media['media_metadata']['photo_metadata'].get('orientation')

                                    # the photo geocode much more trust worth than the FB app geocode, use it
                                    addr = FacebookArchiveReader._reverse_gcode(latitude, longitude)
                                    if addr:
                                        locations.append(addr)

                                image = {'file': image_file, 'latitude': latitude, 'longitude': longitude,
                                         'orientation': orientation}
                                images.append(image)

                                if description and not message:
                                    # the message is in the image description
                                    message = message + description

                            elif item.get('place'):
                                p = item['place']
                                latitude = p['coordinate']['latitude'] if p.get('coordinate') else None
                                longitude = p['coordinate']['longitude'] if p.get('coordinate') else None
                                address = FacebookArchiveReader._sanitize(p.get('address'))
                                if not address and locations:
                                    # borrow from the photos
                                    address = locations[0]

                                place = {'name': FacebookArchiveReader._sanitize(p['name']),
                                         'address': address,
                                         'latitude': latitude,
                                         'longitude': longitude}

                                # sometimes we get duplicate places from Facebook download. Just use the 1st one
                                if len(places) == 0:
                                    places.append(place)

                            elif item.get('external_context'):
                                econtext = item['external_context']
                                if econtext.get('url'):
                                    line = econtext['name'] + ' - ' + econtext['url'] if econtext.get('name') else econtext['url']
                                    # don't share app activities
                                    if 'spotify' not in econtext['url'] and 'pinterest' not in econtext['url'] and '/fbapp/' not in econtext['url']:
                                        message = message + line + "\n"

            post_id = str(timestamp)
            created_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            if message or images or places or tags:
                locs = set(locations) if locations else []
                logging.info("Adding post " + post_id + ": " + message + ", " + str(images) + ", " + str(
                    places) + ", " + str(tags)  + ", locations: " + str(locs))
                posts.append((post_id, created_time, message, images, locs, places, tags))
            else:
                logging.info("Skipping unknown post " + post_id)

        return posts



class FacebookExporter:
    """
        Download the posts and their attachments.
        Check out https://developers.facebook.com/tools/explorer/
    """

    def __init__(self, fb_tokens, tmp_dir='/tmp', hourly_limit=180):
        """
        :param fb_tokens: the FB user access tokens. Each token should have user_photos, user_posts and public_profile permissions.
        """
        self.fb_tokens = fb_tokens
        self.tmp_dir = tmp_dir
        self.curr_token_idx = 0
        self.secs_between_fb_calls = 3600 / len(fb_tokens) / hourly_limit

    def _fb_token(self):
        return self.fb_tokens[self.curr_token_idx]

    @staticmethod
    def get_long_lived_token(app_id, app_secret, fb_token):
        request_url = "https://graph.facebook.com/oauth/access_token?fb_exchange_token=%s&grant_type=fb_exchange_token&client_id=%s&client_secret=%s" % (
        fb_token, app_id, app_secret)
        response = requests.get(request_url)
        if response.status_code != 200:
            raise Exception("Failed to exchange given token for long lived token " + str(response.status_code))
        result = json.loads(response.text)
        token = result['access_token']
        return token

    @staticmethod
    def _cache_key(request_url):
        key = re.sub("v7.0/", "", request_url) # get rid of the API version
        key = re.sub(".*facebook.com/", "", key) # get rid of the API server
        key = re.sub(".access_token.*", "", key) # get rid of the access token
        return "fb_cached_" + key.replace("/", "_")

    def _call(self, request_url, ignore_error=True):

        # FB has a severe rate limit, so we ue the file system as a cache

        cache_file = self.tmp_dir + "/" + self._cache_key(request_url)

        if os.path.isfile(cache_file):
            cache_content = open(cache_file, 'r').read()
            result = json.loads(cache_content)
            return result
        else:
            while True:
                logging.info("Fetching " + request_url + "...")
                response = requests.get(request_url)
                if response.status_code > 400:
                    # Rate limit exceeded? switch to next app/token
                    self.curr_token_idx = self.curr_token_idx + 1
                    if self.curr_token_idx >= len(self.fb_tokens):
                        # exhausted all tokens, nothing we can do
                        raise Exception("Got 40x, possible exceeded rate limit: " + str(response.status_code))
                    else:
                        logging.warning("Got 40x, trying next token: " + str(response.status_code))
                else:
                    time.sleep(self.secs_between_fb_calls)
                    break

            if response.status_code < 300:
                open(cache_file, 'w').write(response.text)
                result = json.loads(response.text)
                return result
            elif response.status_code == 400:
                # cache bad request so we don't repeat it
                open(cache_file, 'w').write(response.text)
                result = json.loads(response.text)
                return None
            elif ignore_error:
                logging.info('Error fetching %s, status %d' % (request_url, response.status_code))
                return None
            else:
                return None

    def get_posts_meta(self, max_pages=0, page_size=100, ignore_error=True):
        """
        Enumerate the posts (no attachment)
        :param max_pages:
        :param page_size:
        :param ignore_error:
        :return: list of post ids
        """

        post_ids = []

        myinfo = self._call('https://graph.facebook.com/me?access_token=' + self._fb_token(), ignore_error=False)
        if myinfo:
            logging.info("Fetching posts for %s " % myinfo['name'])
            page_url = 'https://graph.facebook.com/me/posts?limit=' + str(
                page_size) + '&access_token=' + self._fb_token()
            total = 0;
            pages = 0;
            while max_pages == 0 or pages < max_pages:
                result = self._call(page_url)
                if not result or not result.get('data'):
                    logging.warning("Empty page result, probably done: " + page_url)
                    break
                posts_meta = result['data']
                for post_meta in posts_meta:
                    created_time = post_meta['created_time']
                    message = post_meta['message'] if 'message' in post_meta else None
                    post_id = post_meta['id']
                    post_ids.append(( post_id, created_time, message))
                    logging.info("Post %s : %s %s" % (post_id, created_time, message))
                    total = total + 1
                pages = pages + 1
                if 'paging' in result:
                    result_paging = result['paging']
                    if 'next' in result_paging:
                        next_page_url = result_paging['next']
                        if next_page_url == page_url:
                            break
                        else:
                            # make sure the access token is at the end
                            page_url = re.sub('.access_token.*&limit', '?limit',
                                              next_page_url) + '&access_token=' + self._fb_token()
                else:
                    break

        logging.info("Fetched %d posts in %d pages" % (total, pages))
        return post_ids

    def get_posts(self, max_pages=0, page_size=100, ignore_error=True):

        posts_meta = self.get_posts_meta(max_pages, page_size, ignore_error)
        posts = []
        for post_id, created_time, message in posts_meta:
            logging.info("Getting attachments for post %s : %s %s" % (post_id, created_time, message))
            attachment_result = self._call('https://graph.facebook.com/' + post_id + '/attachments?access_token=' + self._fb_token(), ignore_error)
            images = []
            locations = []
            if attachment_result:
                attachments = attachment_result['data']
                for attachment in attachments:
                    attachment_type = attachment['type']
                    media = attachment.get('media')
                    logging.info(attachment_type + ", " + str(media))
                    if attachment_type == 'photo' or attachment_type == 'cover_photo':
                        images.append(media['image'])
                    elif attachment_type == 'map':
                        img = media['image']
                        locations.append( { "name" : attachment['title'], "url" : attachment['url'] })
                        images.append(media['image'])
                    elif attachment_type == 'album':
                        subattachments = attachment['subattachments']['data']
                        for subattachment in subattachments:
                            media = subattachment.get('media')
                            images.append(media['image'])
                logging.info("  Images: " + str(images));

            posts.append((post_id, created_time, message, images, locations, None, None))

        return posts


if __name__ == "__main__":

    if len(sys.argv) < 5 :
        print("usage: <cache dir> <facebook app id> <facebook app secret> <user access token> [ <s3 bucket> <s3 image folder> ] ")
        exit(-1)
    cache_dir = sys.argv[1]
    app_id = sys.argv[2]
    app_secret = sys.argv[3]
    user_access_token = sys.argv[4]

    upload_images = False
    if len(sys.argv) == 7:
        upload_images = True
        s3_bucket = sys.argv[5]
        s3_image_folder = sys.argv[6]
        logging.info("Will upload all images to " + s3_bucket + "/" + s3_image_folder)

    fb_exporter = FacebookExporter(FacebookExporter.get_long_lived_token(app_id, app_secret, user_access_token), tmp_dir=cache_dir)
    posts = fb_exporter.get_posts(0, ignore_error=True)

    if upload_images:
        S3.upload_images_to_s3(s3_bucket, s3_image_folder, posts)


