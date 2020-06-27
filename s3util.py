import boto3
import botocore
import logging
import requests
import hashlib
import os
from PIL import Image

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)


class S3:

    @staticmethod
    def get_keys(s3_bucket, key_prefix):
        s3 = boto3.client('s3')
        kwargs = {'Bucket': s3_bucket, 'Prefix': key_prefix}
        while True:
            resp = s3.list_objects_v2(**kwargs)
            if resp.get('Contents'):
                for obj in resp['Contents']:
                    key = obj['Key']
                    yield key

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    @staticmethod
    def _upload_image_to_s3(source_image_url, s3_bucket, s3_image_key):
        session = boto3.Session()
        s3 = session.resource('s3')

        # does it exist already? if so, skip the potentially much more expensive upload
        exists = True
        try:
            s3.Object(s3_bucket, s3_image_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise e
        if exists:
            logging.info("Image already exists in S3:" + s3_image_key)
            return

        # open a download stream
        response = requests.get(source_image_url, stream=True)
        if response.status_code != 200:
            raise Exception("Failed to open source image url:" + source_image_url)

        # upload stream to s3 as public file
        bucket = s3.Bucket(s3_bucket)
        bucket.upload_fileobj(response.raw, s3_image_key, ExtraArgs={'ACL': 'public-read'})

    @staticmethod
    def _upload_file_to_s3(file, s3_bucket, s3_image_key):
        session = boto3.Session()
        s3 = session.resource('s3')
        bucket = s3.Bucket(s3_bucket)
        bucket.upload_file(file, s3_image_key, ExtraArgs={'ACL': 'public-read'})

    @staticmethod
    def _get_s3_image_key(s3_image_folder, post_id, image):
        image_url_hash = hashlib.md5(image.encode()).hexdigest()
        key = "{folder}/{post_id}-{hash}".format(folder=s3_image_folder, post_id=post_id, hash=image_url_hash)
        return key

    @staticmethod
    def _get_s3_image_url(s3_bucket, key):
        return "https://{bucket}.s3.amazonaws.com/{key}".format(bucket=s3_bucket, key=key)

    @staticmethod
    def get_s3_image_url(s3_bucket, s3_image_folder, post_id, image):
        key = S3._get_s3_image_key(s3_image_folder, post_id, image)
        return "https://{bucket}.s3.amazonaws.com/{key}".format(bucket=s3_bucket, key=key)

    @staticmethod
    def upload_images_to_s3(s3_bucket, s3_image_folder, posts, ignore_error=True):
        existing_keys = [k for k in S3.get_keys(s3_bucket, s3_image_folder)]

        total = 0
        for (post_id, created_time, message, images, *_) in posts:
            for image in images:
                image_url = image['src']
                key = S3._get_s3_image_key(s3_image_folder, post_id, image_url)
                if key in existing_keys:
                    logging.info("Skipping existing s3 image: " + key)
                else:
                    logging.info("Uploading to {s3_url}: {image_url}".format(s3_url=S3._get_s3_image_url(s3_bucket, key), image_url=image_url))
                    try:
                        S3._upload_image_to_s3(image_url, s3_bucket, key)
                    except Exception as e:
                        if ignore_error:
                            logging.error("Failed to upload image " + image_url)
                        else:
                            raise e

                    total = total + 1
        logging.info("Uploaded %d images" % total)


    @staticmethod
    def upload_local_images_to_s3(s3_bucket, s3_image_folder, posts, ignore_error=True, check_size=True):
        existing_keys = [k for k in S3.get_keys(s3_bucket, s3_image_folder)]

        total = 0
        for (post_id, created_time, message, images, *_) in posts:
            for image in images:
                if image.get('file') and not image.get('src'):
                    image_file = image['file']

                    if check_size and not image.get('width'):
                        try:
                            pimg = Image.open(image_file)
                            width, height = pimg.size
                            image['height'] = height
                            image['width'] = width
                            logging.info("Image dimension for %s, %d w x %d h" % (image_file, width, height))
                        except:
                            logging.error("Failed to identify image " + image_file)
                            # probably not an image? skip
                            continue

                    key = S3._get_s3_image_key(s3_image_folder, post_id, image_file)
                    if key in existing_keys:
                        logging.info("Skipping existing s3 image: " + key)
                    else:
                        logging.info("Uploading to {s3_url}: {image_file}".format(s3_url=S3._get_s3_image_url(s3_bucket, key), image_file=image_file))
                        try:
                            S3._upload_file_to_s3(image_file, s3_bucket, key)
                        except Exception as e:
                            if ignore_error:
                                logging.error("Failed to upload image " + image_file)
                            else:
                                raise e
                    image['src'] = S3.get_s3_image_url(s3_bucket, s3_image_folder, post_id, image_file)

                    total = total + 1

        logging.info("Uploaded %d local images" % total)
        return posts

if __name__ == "__main__":

    keys = S3.get_keys("headexploding", "www.headexploding.com/fb_images")
    print([ k for k in keys])

    file = '/Users/wen/Downloads/10157717211877734_10157529835037734-02144361015936dd7f280a9c160c3b5e.jpg'
    image = Image.open(file)
    width, height = image.size
    print(width, height)
#

