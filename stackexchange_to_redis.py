# -*- coding: utf-8 -*-
"""
Imports stackexchange xml files into redis

Motivation:
-----------
This script is a part of redis-rdb-tools, 
and helps us generate realistic dump files to help in testing

Schema Documentation:
---------------------
See https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede

Pre-requisites:
---------------
1. Download any of the stackexchange website archive files 
    from https://archive.org/download/stackexchange
2. Extract the .7z files into a directory. 
    The directory should have Badges.xml, Comments.xml, PostHistory.xml,
    PostLinks.xml, Posts.xml, Tags.xml, Users.xml, and Votes.xml
3. Modify the path to the directory in the __main__ section of this script
4. Ensure redis is running on localhost:6379 without a password
    (Or, adjust the connection string in the __main__ section at the bottom)
5. Run this script without any command line arguments

Keyspace Details:
-----------------
After running this script, redis will have the following keys:

1.  questions:<id> -> a hash containing questions details
2.  questions:<id>:answers -> a set containing answer ids for this question
3.  questions:<id>:tags -> a set of tags assigned to this question
4.  questions:<id>:related_questions -> a set containing question ids that are related to this question
5.  answers:<id> -> a hash containing answer details
6.  post:<id>:comments -> a list of comment ids, sorted by time the comment was posted.
        A post can be a question or an answer. Post Id can be question id or answer id
        See comments:<id> for details on how comments are stored

7.  users:<id> -> a hash containing user details
8.  users:<id>:comments -> a sorted set, key = commentid, score = score of the comment
        this sorted set only contains comments created by this user.
9.  users:<id>:badges -> a set of badges assigned to this user
10. users:<id>:questions_by_score -> a sorted set, key = question id, score = score of the question
        this sorted set only contains questions created by this user
11. users:<id>:answers_by_score -> a sorted set, key = answer id, score = score of the answer
        this sorted set only contains answers created by this user
12. user:<id>:questions_by_views -> a sorted set, key = questionid, 
        score = number of views for this question
        this sorted set only contains questions created by this user
13. users_by_reputation -> a sorted set, key = userid, value is reputation of user
14. comments:<id> -> a hash containing comment details
15. tags:<id> -> a hash containing tag details
16. tags:<id>:questions_by_score -> a sorted set, key = question id, 
        score = score of the question
        this sorted set only contains questions that have the given tag id
17. tags:<id>:questions_by_views -> a sorted set, key = questionid, 
        score = number of views for this post
        this sorted set only contains questions that have the given tag id
18. tags:<id>:recent_questions -> a trimmed list of questions under this tag, 
        ordered by time
19. badges:<id>:users -> a set of users that have this badge
20. badges_by_popularity -> a sorted set, key = badge name, 
        value is number of users that have this badge


Implementation Details:
-----------------------
1. The script uses pipelining, but is single threaded. 
    It should be possible to increase throughput by making it multiple threaded

TODOs:
------
1. Accept command line parameters for csv file and redis connection parameters
2. We don't have a badges:<id> hash object. 
    Possible fields include description, level = (bronze, silver or gold),
    category = (answer badge, question badge, participation badge, tag badge, moderation badge, or others)
    See https://datascience.stackexchange.com/help/badges for more information

"""
import xml.etree.ElementTree as etree
import redis
import re

try:
    from itertools import izip_longest
except:
    from itertools import zip_longest as izip_longest


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def to_snake_case(name):
    if name.endswith(('Id', 'ID')) or name in ('UpVotes', 'DownVotes'):
        return name.lower()
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()

def parse_rows(xml_file, incl_attrs=None, excl_attrs=None):
    for _, elem in etree.iterparse(xml_file, events=('start', )):
        if elem.tag == 'row':
            if excl_attrs:
                req_keys = set(elem.attrib.keys()) - excl_attrs
            elif incl_attrs:
                req_keys = set(elem.attrib.keys()) & incl_attrs
            else:
                req_keys = elem.attrib.keys()
            obj = dict((to_snake_case(k), elem.attrib[k]) for k in req_keys)
            yield obj

def get_batches(iterable, batch_size, fillvalue=None):
    args = [iter(iterable)] * batch_size
    return izip_longest(fillvalue=fillvalue, *args)

def import_in_batches(red, objs, callback):
    for batch in get_batches(objs, 100):
        pipe = red.pipeline()
        for obj in batch:
            if obj:
                callback(obj, pipe)
        pipe.execute()

def _import_user(user, pipe):
    pipe.hmset("users:%s" % user['id'], user)
    pipe.zadd("users_by_reputation", user['reputation'], user['id'])

def import_users(users_file, red):
    users = parse_rows(users_file)
    import_in_batches(red, users, _import_user)

def _import_user_badge(badge, pipe):
    pipe.sadd("users:%s:badges" % badge['userid'], badge['name'])
    pipe.sadd("badges:%s:users" % badge['name'], badge['userid'])
    pipe.zincrby("badges_by_popularity", badge['name'], 1.0)

def import_user_badges(badges_file, red):
    badges = parse_rows(badges_file, incl_attrs=set(['UserId', 'Name']))
    import_in_batches(red, badges, _import_user_badge)
    
def _parse_tags(tag_str):
    tag_str = tag_str.replace("><", ",")
    tag_str = tag_str.replace(">", "")
    tag_str = tag_str.replace("<", "")
    if tag_str:
        tags = tag_str.split(",")
        return tags

def _post_type(posttypeid):
    posttypeid = int(posttypeid.strip())
    if posttypeid == 1:
        post_type = "questions"
    elif posttypeid == 2:
        post_type = "answers"
    else:
        post_type = None
    # elif posttypeid in (3,4,5):
    #     post_type = "tagwiki"
    # elif posttypeid == 6:
    #     post_type = "moderator-nomination"
    # elif posttypeid in (7,8):
    #     post_type = "wiki"
    # else:
    #     post_type = "generic-post"
    
    return post_type

def _import_post(post, pipe):
    post_type = _post_type(post['posttypeid'])

    # Only import questions and answers, ignore tag wiki
    if not post_type:
        return

    if 'tags' in post:
        tags = _parse_tags(post['tags'])
        del post['tags']
        if tags and post_type == 'questions':
            pipe.sadd("questions:%s:tags" % post['id'], *tags)
            for tag in tags:
                pipe.zadd("tags:%s:questions_by_score" % tag, post['score'], post['id'])
                if 'view_count' in post:
                    pipe.zadd("tags:%s:questions_by_views" % tag, post['view_count'], post['id'])
                pipe.lpush("tags:%s:recent_questions" % tag, post['id'])
                pipe.ltrim("tags:%s:recent_questions" % tag, 0, 200)
    
    if 'owneruserid' in post:
        pipe.zadd("users:%s:%s_by_score" % (post['owneruserid'], post_type), post['score'], post['id'])

        if 'view_count' in post and post_type == 'questions':
            pipe.zadd("users:%s:questions_by_views" % post['owneruserid'], post['view_count'], post['id'])

    # store questions / answers in separate key space
    pipe.hmset("%s:%s" % (post_type, post['id']), post)

    if post_type == 'answers' and 'parentid' in post:
        pipe.sadd("questions:%s:answers" % post['parentid'], post['id'])

def import_posts(posts_file, red):
    posts = parse_rows(posts_file)
    import_in_batches(red, posts, _import_post)

def _import_comment(comment, pipe):
    pipe.hmset("comments:%s" % comment['id'], comment)
    if 'userid' in comment:
        pipe.zadd("users:%s:comments" % comment['userid'], comment['score'], comment['id'])
    pipe.rpush("posts:%s:comments" % comment['postid'], comment['id'])

def import_comments(comments_file, red):
    comments = parse_rows(comments_file)
    import_in_batches(red, comments, _import_comment)

def _import_tag(tag, pipe):
    pipe.hmset("tags:%s" % tag['tag_name'], tag)

def import_tags(tags_file, red):
    tags = parse_rows(tags_file)
    import_in_batches(red, tags, _import_tag)

def _import_linked_post(linked_post, pipe):
    # Asssuming only questions are linked
    # technically wiki's and other posts can also be linked, 
    # but we will ignore those for now
    pipe.sadd("questions:%s:linked_questions" % linked_post['postid'], linked_post['relatedpostid'])
    pipe.sadd("questions:%s:linked_questions" % linked_post['relatedpostid'], linked_post['postid'])

def import_linked_posts(linked_posts_file, red):
    linked_posts = parse_rows(linked_posts_file)
    import_in_batches(red, linked_posts, _import_linked_post)

if __name__ == '__main__':
    _red = redis.StrictRedis()
    base_dir = "datascience.stackexchange.com"
    def _f(filename):
        return base_dir + "/" + filename

    import_users(_f("Users.xml"), _red)
    import_user_badges(_f("Badges.xml"), _red)
    import_posts(_f("Posts.xml"), _red)
    import_comments(_f("Comments.xml"), _red)
    import_tags(_f("Tags.xml"), _red)
    import_linked_posts(_f("PostLinks.xml"), _red)