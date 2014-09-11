import flickrapi
import urllib
import unicodedata
import os

default_api_key = '2a738d26af4bf19c596490c1d422818e'
# output file
default_file_name = './met_metadata.txt'
# change this to get different picsets from a group
group_id = '2657549@N25'
default_pic_dir = './pics/' + group_id + '/'

flickr = flickrapi.FlickrAPI(default_api_key)
met_group_photo_data = {}

def write_metadata_and_download_photos(api_key=default_api_key, pic_dir=default_pic_dir):
    flickr = flickrapi.FlickrAPI(api_key)
    met_group_photo_data = {}
    sets = flickr.groups_pools_getPhotos(group_id=group_id)
    for child in sets[0]:

        title = child.attrib['title'] 
        photo_id = child.attrib['id']

        photo = flickr.photos_getInfo(photo_id=child.attrib['id'])
        username = photo[0].findall('owner')[0].get('username')
        description = photo[0].findall('description')[0].text
        date_posted = photo[0].findall('dates')[0].get('posted')
        date_taken = photo[0].findall('dates')[0].get('taken')
        views = photo[0].attrib['views']

        tags = ''
        for tag in flickr.tags_getListPhoto(photo_id=child.attrib['id'])[0][0]:
            tags = tags + tag.attrib['authorname'] + ': ' + tag.text + '\n'
        
        comments = ''
        for comment in flickr.photos_comments_getList(photo_id=child.attrib['id'])[0]:
            comments = comments + comment.attrib['authorname'] + ': ' + comment.text + '\n'
        fav_list = flickr.photos_getFavorites(photo_id=child.attrib['id'])
        
        favorites = ''
        try:
            favorites = 'Favorites: ' + fav_list[0].attrib['total'] + '\n'
            for favorite in fav_list[0]:
                favorites = favorites + favorite.attrib['username'] + '\n'
        except Exception, e:
            pass

        met_group_photo_data['Title'] = title
        met_group_photo_data['Photo ID'] = photo_id
        met_group_photo_data['Username'] = username
        met_group_photo_data['Description'] = description
        met_group_photo_data['Date Posted'] = date_posted
        met_group_photo_data['Date Taken'] = date_taken
        met_group_photo_data['Views'] = views
        met_group_photo_data['Tags'] = tags
        met_group_photo_data['Comments'] = comments
        met_group_photo_data['Favorites'] = favorites

        write_metadata_dict_to_file(met_group_photo_data)

        if not os.path.exists(pic_dir):
            os.makedirs(pic_dir)
        photo_sizes = flickr.photos_getSizes(photo_id=child.attrib['id'])
        urllib.urlretrieve(photo_sizes[0][-1].attrib['source'], pic_dir + child.attrib['id'] + '.jpg')

    return 0


def write_metadata_dict_to_file(metadata, file_name=default_file_name):
    output = ''
    for k, v in metadata.iteritems():
        if type(v) is not str and v is not None:
            v = unicodedata.normalize('NFKD', v).encode('ascii', 'ignore')
        if v == '':
            v = None
        output = output + str(k) + ': ' + str(v) + '\n'

    output = output + '---------------------' + '\n'

    with open(file_name, 'a') as f:
        f.write(output)

write_metadata_and_download_photos()
