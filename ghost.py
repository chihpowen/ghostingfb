import requests
import jwt
import logging
import os
import json
import hashlib
import re
from datetime import datetime as date
import sys
from fb import FacebookExporter
from pybars import Compiler


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)


class GhostImporter:
    def __init__(self, api_url, admin_api_key, user_slug):
        """
        :param admin_api_key: the Ghost Admin API key
        """
        self.admin_api_key = admin_api_key
        self.api_url = api_url
        self.user_slug = user_slug

    def _get_jwt_token(self):

        # Split the key into ID and SECRET
        id, secret = self.admin_api_key.split(':')

        # Prepare header and payload
        iat = int(date.now().timestamp())

        header = {'alg': 'HS256', 'typ': 'JWT', 'kid': id}
        payload = {
            'iat': iat,
            'exp': iat + 5 * 60,
            'aud': '/v3/admin/'
        }

        # Create the token (including decoding secret)
        token = jwt.encode(payload, bytes.fromhex(secret), algorithm='HS256', headers=header)

        return token

    def get_post(self, post_id):
        url = self.api_url + '/admin/posts/' + post_id
        headers = {'Authorization': 'Ghost {}'.format(self._get_jwt_token().decode())}
        response = requests.get(url, headers=headers)
        result = json.loads(response.text) if response.status_code == 200 else None
        return result

    def get_posts(self, max_pages=0):
        slug_to_post = {}
        total = 0
        page = 1
        while max_pages == 0 or page < max_pages:
            url = self.api_url + '/admin/posts?order=title%20asc&page=' + str(page)
            headers = {'Authorization': 'Ghost {}'.format(self._get_jwt_token().decode())}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                results = json.loads(response.text)
                posts = results['posts']
                if posts:
                    for post in posts:
                        post_id = post['id']
                        slug = post['slug']
                        title = post['title']
                        authors = post['authors']
                        author_slugs = [a["slug"] for a in authors]
                        logging.info("Post %s %s: %s by %s" % (post_id, slug, title, ",".join(author_slugs)))
                        total = total + 1
                        slug_to_post[slug] = post
                    page = page + 1
                else:
                    # done with all pages
                    break
            else:
                logging.error("page request failed %d: %s" % (response.status_code, url))
        logging.info("Fetched %d pages and %d posts" % (page - 1, total))
        return slug_to_post

    @staticmethod
    def group_posts_by_5years(posts):
        posts_by_5years = {}

        for post in posts:
            (post_id, created_time, message, images, locations, places, tags) = post
            year = re.sub('-.*', '', created_time)
            year5 = int((int(year) - 1) / 5) * 5
            if not posts_by_5years.get(year5) :
                posts_by_5years[year5] = []
            posts_by_5years[year5].append(post)

        return posts_by_5years

    @staticmethod
    def _resize_image(image, max_width=512):
        width = image['width']
        height = image['height']
        if width > max_width:
            height = (int)(height / (width / max_width))
            width = max_width
            image['width'] = width
            image['height'] = height

    @staticmethod
    def render_post_json(posts, images_per_row=2, max_width=512, template_file='post.hb'):

        fb_posts = []
        post_idx = 0
        for post_id, created_time, message, images, locations, places, tags in posts:
            post_idx = post_idx + 1
            post_images = []
            for i in range(0, len(images)):
                image = images[i]
                if image.get('src'):
                    image_url = image['src']
                    image_url_hash = hashlib.md5(image_url.encode()).hexdigest()
                    GhostImporter._resize_image(image)
                    post_image = {
                        'filename' : image_url_hash,
                        'width' : image['width'],
                        'height': image['height'],
                        'src' : image_url,
                        'row' : int(i / images_per_row)
                    }
                    post_images.append(post_image)

            message_lines =  [m for m in message.replace('"','').splitlines() if m.strip()] if message else []

            fb_post = {
                'date' : re.sub(" .*", "", created_time),
                'message' : message_lines,
                'has_image' : len(post_images) > 0,
                'has_locations': locations and len(locations) > 0,
                'gallery_idx' : post_idx,
                'images': post_images,
                'locations' : ",".join(locations) if locations else None,
                'places' : places,
                'tags' : [ ("with " + ", ".join(tags)) ] if tags else None
            }
            fb_posts.append(fb_post)

        compiler = Compiler()
        template_source = open(template_file, 'r').read()
        template = compiler.compile(template_source)
        output = template( { 'fb_posts' : fb_posts })
        return output

    def create_post(self, slug, title, posts, replace=True):
        headers = {'Authorization': 'Ghost {}'.format(self._get_jwt_token().decode()),
                   'Content-Type': 'application/json'}

        # check if the post already exists

        request_url = self.api_url + "/admin/posts/slug/" + slug
        response = requests.get(request_url, headers=headers)
        existing_post = None
        if response.status_code == 200:
            existing_post = json.loads(response.text)['posts'][0]

        mdoc_json = GhostImporter.render_post_json(posts)
        post = { 'posts': [{ 'slug': slug, 'title' : title, 'mobiledoc' : mdoc_json }] }

        if existing_post:
            # delete first
            request_url = self.api_url + '/admin/posts/' + existing_post['id']
            response = requests.delete(request_url, headers=headers)
            if response.status_code != 204:
                raise Exception("Failed to clean up post %d : %s" % (response.status_code, response.text))

        request_url = self.api_url + '/admin/posts'
        payload = json.dumps(post)
        response = requests.post(request_url, headers=headers, data=payload)

        if response.status_code == 201:
            logging.info("Created post " + response.text)
        else:
            logging.error(mdoc_json)
            raise Exception("Failed to create post %d : %s" % (response.status_code, response.text))

