import com.jcraft.jsch.UserInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BetterSimilarUserPair {

    /**
     * author:  RickAi
     * email:   yongbiaoai@gmail.com
     * <p>
     * Preprocessed data:
     * userId : like movie list : like movie count : unlike movie list : unlike movie count
     * <p>
     * <p>
     * UserPairMapper
     * Mapper:
     * line
     * ->
     * movieId | userId, movie like count, movie unlike count, type
     * <p>
     * (UserInfoWritable, UserPrefWritable)
     * <p>
     * UserPairReducer
     * Reducer:
     * movieId | UserPrefWritable1...UserPrefWritableN
     * ->
     * UserInfoWritable1, UserInfoWritable2 | common like pref, common unlike pref (CommonPrefWritable)
     * <p>
     * (UserPairWritable, CommonPrefWritable)
     * <p>
     * <p>
     * CommonPrefMapper
     * Mapper:
     * line -> UserPairWritable, CommonPrefWritable
     * <p>
     * CommonPrefReducer
     * Reducer:
     * old.UserPairWritable, CommonPrefWritable1..N -> old.UserPairWritable, Jaccard Distance
     */

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job userPairJob = Job.getInstance();
            userPairJob.setJarByClass(BetterSimilarUserPair.class);

            userPairJob.setMapperClass(UserPairMapper.class);
            userPairJob.setReducerClass(UserPairReducer.class);

            userPairJob.setMapOutputKeyClass(LongWritable.class);
            userPairJob.setMapOutputValueClass(UserPrefWritable.class);
            userPairJob.setOutputKeyClass(UserPairWritable.class);
            userPairJob.setOutputValueClass(CommonPrefWritable.class);

            FileInputFormat.addInputPath(userPairJob, new Path(otherArgs[0])); // input
            FileOutputFormat.setOutputPath(userPairJob, new Path(otherArgs[1])); // tmp

            if (userPairJob.waitForCompletion(true)) {
                System.out.println("userPairJob job success.");
            } else {
                System.out.println("userPairJob job failed.");
            }

            Job commonPrefJob = Job.getInstance();
            commonPrefJob.setJarByClass(BetterSimilarUserPair.class);

            commonPrefJob.setMapperClass(CommonPrefMapper.class);
            commonPrefJob.setReducerClass(CommonPrefReducer.class);

            commonPrefJob.setMapOutputKeyClass(UserPairWritable.class);
            commonPrefJob.setMapOutputValueClass(CommonPrefWritable.class);
            commonPrefJob.setOutputKeyClass(UserPairWritable.class);
            commonPrefJob.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(commonPrefJob, new Path(otherArgs[1])); // tmp
            FileOutputFormat.setOutputPath(commonPrefJob, new Path(otherArgs[2])); // output

            if (commonPrefJob.waitForCompletion(true)) {
                System.out.println("commonPrefJob job success.");
            } else {
                System.out.println("commonPrefJob job failed.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class UserPairMapper extends Mapper<LongWritable, Text, LongWritable, UserPrefWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(":");

            if (tokens.length != 5) {
                return;
            }

            // parse UserInfo
            LongWritable userId = new LongWritable(Long.valueOf(tokens[0]));
            LongWritable ratingCount = new LongWritable(Long.valueOf(tokens[1]) + Long.valueOf(tokens[2]));
            UserInfoWritable userInfo = new UserInfoWritable(userId, ratingCount);

            // emit like movie with UserPref
            String[] likeMoviesToken = tokens[3].split(",");
            for (String movie : likeMoviesToken) {
                if (movie.length() == 0) continue;

                LongWritable movieId = new LongWritable(Long.valueOf(movie));
                BooleanWritable like = new BooleanWritable(true);
                UserPrefWritable userPref = new UserPrefWritable(userInfo, like);
                context.write(movieId, userPref);
            }

            // emit unlike movie with UserPref
            String[] unlikeMoviesToken = tokens[4].split(",");
            for (String movie : unlikeMoviesToken) {
                if (movie.length() == 0) continue;

                LongWritable movieId = new LongWritable(Long.valueOf(movie));
                BooleanWritable like = new BooleanWritable(false);
                UserPrefWritable userPref = new UserPrefWritable(userInfo, like);
                context.write(movieId, userPref);
            }
        }
    }

    private static class UserPairReducer extends Reducer<LongWritable, UserPrefWritable, UserPairWritable, CommonPrefWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<UserPrefWritable> values, Context context) throws IOException, InterruptedException {
            List<UserPrefWritable> userPrefs = new ArrayList<UserPrefWritable>();
            for (UserPrefWritable userPref : values) {
                userPrefs.add(new UserPrefWritable(userPref));
            }

            UserPrefWritable curUserPref;
            UserPrefWritable otherUserPref;
            for (int i = 0; i < userPrefs.size(); i++) {
                curUserPref = userPrefs.get(i);
                for (int j = i + 1; j < userPrefs.size(); j++) {
                    otherUserPref = userPrefs.get(j);

                    if (curUserPref.like.get() != otherUserPref.like.get()) {
                        continue;
                    }

                    UserPairWritable userPair = new UserPairWritable(curUserPref.userInfo, otherUserPref.userInfo);
                    if (curUserPref.like.get()) {
                        context.write(userPair, new CommonPrefWritable(1, 0));
                    } else {
                        context.write(userPair, new CommonPrefWritable(0, 1));
                    }
                }
            }
        }
    }

    private static class CommonPrefMapper extends Mapper<LongWritable, Text, UserPairWritable, CommonPrefWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // format: userId1, likeCount, unlikeCount | userId2, likeCount, unlikeCount
            String line = value.toString();
            String[] lineTokens = line.split("\t");

            String[] pairTokens = lineTokens[0].split("-");
            String[] userTokens = pairTokens[0].split(",");
            UserInfoWritable firstUser = new UserInfoWritable(
                    Long.valueOf(userTokens[0]), Long.valueOf(userTokens[1]));

            userTokens = pairTokens[1].split(",");
            UserInfoWritable secondUser = new UserInfoWritable(
                    Long.valueOf(userTokens[0]), Long.valueOf(userTokens[1]));
            UserPairWritable userPair = new UserPairWritable(firstUser, secondUser);

            String[] prefTokens = lineTokens[1].split(",");
            CommonPrefWritable commonPref = new CommonPrefWritable(Long.valueOf(prefTokens[0]), Long.valueOf(prefTokens[1]));
            context.write(userPair, commonPref);
        }
    }

    private static class CommonPrefReducer extends Reducer<UserPairWritable, CommonPrefWritable, UserPairWritable, DoubleWritable> {
        @Override
        protected void reduce(UserPairWritable key, Iterable<CommonPrefWritable> values, Context context) throws IOException, InterruptedException {
            long commonLikeCount = 0;
            long commonUnlikeCount = 0;

            for (CommonPrefWritable pref : values) {
                if (pref.likeCount.get() != 0) {
                    commonLikeCount += pref.likeCount.get();
                }

                if (pref.unlikeCount.get() != 0) {
                    commonUnlikeCount += pref.unlikeCount.get();
                }
            }

            double jaccard = key.jaccardDistance(commonLikeCount, commonUnlikeCount);
            if (jaccard > 0) {
                context.write(key, new DoubleWritable(jaccard));
            }
        }
    }

}
