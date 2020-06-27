import sys
import logging
from fb import FacebookArchiveReader
from fb import FacebookExporter
from ghost import GhostImporter
from s3util import S3

if __name__ == "__main__":

    if len(sys.argv) < 2 or sys.argv[1] == 'help':
        print(""" usage: 
                         help
                            OR
                         api <cache dir> <facebook app id> <facebook app secret> <facebook user access token> 
                            <ghost api url (including version)> <ghost api key> <ghost user slug>  
                            [ <s3 bucket> <s3 image folder> ]
                            OR
                         download <facebook download dir>  <ghost api url (including version)> <ghost api key> <ghost user slug> <s3 bucket> <s3 image folder> 
              """)
    elif sys.argv[1] == 'api' :

        cache_dir = sys.argv[2]
        app_id = sys.argv[3]
        app_secret = sys.argv[4]
        user_access_token = sys.argv[5]
        api_url = sys.argv[6]
        api_key = sys.argv[7]
        user_slug = sys.argv[8]

        upload_images = False

        if len(sys.argv) > 9:
            upload_images = True
            s3_bucket = sys.argv[9]
            s3_image_folder = sys.argv[10]
            logging.info("Will upload all images to " + s3_bucket + "/" + s3_image_folder)

            # export from Facebook

            fb_exporter = FacebookExporter(FacebookExporter.get_long_lived_token(app_id, app_secret, user_access_token), tmp_dir=cache_dir)
            posts = fb_exporter.get_posts(0, ignore_error=True)

            if upload_images:
                S3.upload_images_to_s3(s3_bucket, s3_image_folder, posts, ignore_error=True)

    elif sys.argv[1] == 'download':
        fb_download_dir = sys.argv[2]
        api_url = sys.argv[3]
        api_key = sys.argv[4]
        user_slug = sys.argv[5]
        s3_bucket = sys.argv[6]
        s3_image_folder = sys.argv[7]
        upload_images = True

        posts = FacebookArchiveReader.read(fb_download_dir)
#        posts = FacebookArchiveReader.read_file(fb_download_dir, "/Users/wen/Downloads/facebook-chihpo/posts/test.json")
        posts = S3.upload_local_images_to_s3(s3_bucket, s3_image_folder, posts)

    # post to Ghost in 5 year increments

    posts_by_5years = GhostImporter.group_posts_by_5years(posts)

    gi = GhostImporter(api_url, api_key, user_slug)
    posts = gi.get_posts()
    # print(gi.get_post("5ef7d221495a755dbbcbe076"))

    for year, posts in posts_by_5years.items():
        slug = user_slug + "_" + str(year + 1) + "_" + str(year + 5)
        title = "The Years %d-%d, According to Facebook" % (year + 1, year + 5)
        gi.create_post(slug, title, posts)
