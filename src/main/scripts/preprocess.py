import os.path

FILE_DIR_BASE = "../resources/"

# [0.5, 2.5] -> 0
# (2.5, 5.0] -> 1
# userId,movieId,rating,timestamp
# 1,2,3.5,1112486027
FILE_SRC_NAME = "ratings.csv"
FILE_DST_NAME = "ratings_simplify.txt"

dst_file = open(FILE_DIR_BASE + FILE_DST_NAME, "w")

print("start simplify the ratings.csv")
count = 0
first_line = True

MAX_COUNT = -1

# (userId, [[like_movie_list], [unlike_movie_list]])
user_rating_map = dict()
with open(FILE_DIR_BASE + FILE_SRC_NAME, "r") as f:
    for line in f:
        if (first_line) :
            first_line = False
            continue

        tokens = line.split(",")
        if len(tokens) != 4 or float(tokens[2]) < 0.5 or float(tokens[2]) > 5:
            continue

        if MAX_COUNT > 0 and count >= MAX_COUNT:
            break

        user_id = long(tokens[0])
        movie_id = long(tokens[1])
        if not user_rating_map.has_key(user_id):
            user_rating_map[user_id] = list()
            user_rating_map[user_id].append(list()) # like movie list
            user_rating_map[user_id].append(list()) # unlike movie list

        if float(tokens[2]) > 2.5:
            user_rating_map[user_id][0].append(movie_id)
        else:
            user_rating_map[user_id][1].append(movie_id)

        count += 1

for user_id in user_rating_map:
    line = str(user_id)
    line += ":"

    # like count
    line += str(len(user_rating_map[user_id][0]))
    line += ":"
    # unlike count
    line += str(len(user_rating_map[user_id][1]))
    line += ":"

    # like movie list
    for movie_id in user_rating_map[user_id][0]:
        line += str(movie_id) + ","
    if len(user_rating_map[user_id][0]) > 0:
        line = line[:-1]
    line += ":"

    # unlike movie list
    for movie_id in user_rating_map[user_id][1]:
        line += str(movie_id) + ","
    if len(user_rating_map[user_id][1]) > 0:
        line = line[:-1]

    dst_file.write(line + "\n")

print("simplify done")