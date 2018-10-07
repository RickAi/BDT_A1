
# split command
# sed -n '0,20000p' src > dst

FILE_DIR_BASE = "../resources/"

# [0.5, 2.5] -> 0
# (2.5, 5.0] -> 1
# userId,movieId,rating,timestamp
# 1,2,3.5,1112486027
DEBUG = True

PREFIX = "tiny" if DEBUG else "ratings"
FILE_SRC_NAME = PREFIX + ".csv"
FILE_DST_NAME = PREFIX + "_simplify.txt"

dst_file = open(FILE_DIR_BASE + FILE_DST_NAME, "w")

print("start simplify the " + FILE_SRC_NAME)

# parse user rating count
user_rating_map = dict()
first_line = True
with open(FILE_DIR_BASE + FILE_SRC_NAME, "r") as f:
    for line in f:
        if (first_line) :
            first_line = False
            continue

        tokens = line.split(",")
        if len(tokens) != 4 or float(tokens[2]) < 0.5 or float(tokens[2]) > 5:
            continue

        user_id = long(tokens[0])
        movie_id = long(tokens[1])
        if not user_rating_map.has_key(user_id):
            user_rating_map[user_id] = [0, 0]

        if float(tokens[2]) > 2.5:
            user_rating_map[user_id][0] += 1
        else:
            user_rating_map[user_id][1] += 1

# parse movie info
movie_info_map = dict()
first_line = True
with open(FILE_DIR_BASE + FILE_SRC_NAME, "r") as f:
    for line in f:
        if (first_line) :
            first_line = False
            continue

        tokens = line.split(",")
        if len(tokens) != 4 or float(tokens[2]) < 0.5 or float(tokens[2]) > 5:
            continue

        user_id = long(tokens[0])
        movie_id = long(tokens[1])
        if not movie_info_map.has_key(movie_id):
            movie_info_map[movie_id] = list()
            movie_info_map[movie_id].append(list()) # like user list
            movie_info_map[movie_id].append(list()) # unlike user list

        if float(tokens[2]) > 2.5:
            movie_info_map[movie_id][0].append(user_id)
        else:
            movie_info_map[movie_id][1].append(user_id)

for movie_id in movie_info_map:
    line = str(movie_id)
    line += ":"

    # like user list
    for user_id in movie_info_map[movie_id][0]:
        line += str(user_id) + "," + str(user_rating_map[user_id][0]) + "-"
    if len(movie_info_map[movie_id][0]) != 0:
        line = line[:-1]
    line += ":"

    # unlike user list
    for user_id in movie_info_map[movie_id][1]:
        line += str(user_id) + "," + str(user_rating_map[user_id][1]) + "-"
    if len(movie_info_map[movie_id][1]) != 0:
        line = line[:-1]

    dst_file.write(line + "\n")

# for user_id in user_rating_map:
#     line = str(user_id)
#     line += ":"
#
#     # like count
#     line += str(len(user_rating_map[user_id][0]))
#     line += ":"
#     # unlike count
#     line += str(len(user_rating_map[user_id][1]))
#     line += ":"
#
#     # like movie list
#     for movie_id in user_rating_map[user_id][0]:
#         line += str(movie_id) + ","
#     if len(user_rating_map[user_id][0]) > 0:
#         line = line[:-1]
#     line += ":"
#
#     # unlike movie list
#     for movie_id in user_rating_map[user_id][1]:
#         line += str(movie_id) + ","
#     if len(user_rating_map[user_id][1]) > 0:
#         line = line[:-1]
#
#     dst_file.write(line + "\n")

print("simplify done")