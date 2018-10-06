import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopKSimilarUserPair {

    /**
     * author:  RickAi
     * email:   yongbiaoai@gmail.com
     * <p>
     * Preprocessed data sample:
     * userId : like movieId1...like movieIdN : unlike movieId1...unlike movieIdN
     * <p>
     * Mapper:  line -> (movieId | UserInfo)
     * Reducer: (movieId | UserInfo1...UserInfoN) -> (UserPair | similarity)
     */

    private static class UserInfoMapper extends Mapper<LongWritable, Text, LongWritable, UserInfoWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // parse raw sample line to data bean
            String line = value.toString();
            String[] tokens = line.split(":");

            // parse userId
            UserInfoWritable userInfo = new UserInfoWritable();
            if (tokens.length < 1) return;
            LongWritable userId = new LongWritable(Long.valueOf(tokens[0]));
            userInfo.setUserId(userId);

            // parse like movie list
            if (tokens.length < 2) return;
            String[] moviesToken = tokens[1].split(",");
            MovieListWritable movieWritables = new MovieListWritable();
            for (String movieRaw : moviesToken) {
                movieWritables.add(new LongWritable(Long.valueOf(movieRaw)));
            }
            userInfo.setLikeMovies(movieWritables);

            // parse unlike movie list
            if (tokens.length < 3) return;
            moviesToken = tokens[2].split(",");
            movieWritables = new MovieListWritable();
            for (String movieRaw : moviesToken) {
                movieWritables.add(new LongWritable(Long.valueOf(movieRaw)));
            }
            userInfo.setUnlikeMovies(movieWritables);

            // emit
            for (LongWritable movieId : userInfo.getLikeMovies()) {
                context.write(movieId, userInfo);
            }

            for (LongWritable movieId : userInfo.getUnlikeMovies()) {
                context.write(movieId, userInfo);
            }
        }

    }

    private static class UserInfoReducer extends Reducer<LongWritable, UserInfoWritable, UserPairWritable, DoubleWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<UserInfoWritable> values, Context context)
                throws IOException, InterruptedException {
            List<UserInfoWritable> userInfoList = new ArrayList<UserInfoWritable>();
            for (UserInfoWritable user : values) {
                userInfoList.add(new UserInfoWritable(user));
            }

            UserInfoWritable curUserInfo;
            UserInfoWritable otherUserInfo;
            DoubleWritable jaccard;
            for (int i = 0; i < userInfoList.size(); i++) {
                curUserInfo = userInfoList.get(i);
                for (int j = i + 1; j < userInfoList.size(); j++) {
                    otherUserInfo = userInfoList.get(j);
                    UserPairWritable userPair = new UserPairWritable(curUserInfo.userId, otherUserInfo.userId);
                    if (curUserInfo.userId.get() == otherUserInfo.userId.get()) {
                        continue;
                    }

                    jaccard = curUserInfo.jaccardDistance(otherUserInfo);
                    if (jaccard.get() == 0.0f) {
                        continue;
                    }
                    context.write(userPair, jaccard);
                }
            }
        }

    }

    private static class RemoveDuplicateMapper extends Mapper<LongWritable, Text, UserPairWritable, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 2) {
                return;
            }

            String[] idTokens = tokens[0].split(",");
            if (idTokens.length < 2) {
                return;
            }

            LongWritable firstUserId = new LongWritable(Long.valueOf(idTokens[0]));
            LongWritable secondUserId = new LongWritable(Long.valueOf(idTokens[1]));
            DoubleWritable similarity = new DoubleWritable(Double.valueOf(tokens[1]));
            context.write(new UserPairWritable(firstUserId, secondUserId), similarity);
        }

    }

    private static class RemoveDuplicateReducer extends Reducer<UserPairWritable, DoubleWritable, UserPairWritable, DoubleWritable> {

        @Override
        protected void reduce(UserPairWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // only use one of values to remove duplicate
            DoubleWritable similarity = new DoubleWritable(values.iterator().next().get());
            context.write(new UserPairWritable(key), similarity);
        }
    }

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job userPairJob = Job.getInstance();
            userPairJob.setJarByClass(TopKSimilarUserPair.class);

            userPairJob.setMapperClass(UserInfoMapper.class);
            userPairJob.setReducerClass(UserInfoReducer.class);

            userPairJob.setMapOutputKeyClass(LongWritable.class);
            userPairJob.setMapOutputValueClass(UserInfoWritable.class);
            userPairJob.setOutputKeyClass(UserPairWritable.class);
            userPairJob.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(userPairJob, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(userPairJob, new Path(otherArgs[1]));

            if (userPairJob.waitForCompletion(true)) {
                System.out.println("userPairJob job success.");
            } else {
                System.out.println("userPairJob job failed.");
            }

            Job removeDupJob = Job.getInstance();
            removeDupJob.setJarByClass(TopKSimilarUserPair.class);

            removeDupJob.setMapperClass(RemoveDuplicateMapper.class);
            removeDupJob.setReducerClass(RemoveDuplicateReducer.class);

            removeDupJob.setMapOutputKeyClass(UserPairWritable.class);
            removeDupJob.setMapOutputValueClass(DoubleWritable.class);
            removeDupJob.setOutputKeyClass(UserPairWritable.class);
            removeDupJob.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(removeDupJob, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(removeDupJob, new Path(otherArgs[2]));

            if (removeDupJob.waitForCompletion(true)) {
                System.out.println("removeDupJob job success.");
            } else {
                System.out.println("removeDupJob job failed.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
