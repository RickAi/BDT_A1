import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * author:  RickAi
 * email:   yongbiaoai@gmail.com
 * <p>
 * Preprocessed data format:
 * movieId : like (userId, totalRatingCount) list : dislike (userId, totalRatingCount) list
 * <p>
 * Mapper:
 * line -> UserInfo | CommonPref
 * <p>
 * Reducer:
 * UserInfo | CommonPref1...CommonPrefN -> UserInfo | Jaccard Distance
 */

public class SimilarUserPairMain {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job userPairJob = Job.getInstance();
            userPairJob.setJarByClass(SimilarUserPairMain.class);

            userPairJob.setMapperClass(UserPairMapper.class);
            userPairJob.setReducerClass(UserPairReducer.class);

            userPairJob.setMapOutputKeyClass(UserPairWritable.class);
            userPairJob.setMapOutputValueClass(CommonPrefWritable.class);
            userPairJob.setOutputKeyClass(UserPairWritable.class);
            userPairJob.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(userPairJob, new Path(otherArgs[0])); // input
            FileOutputFormat.setOutputPath(userPairJob, new Path(otherArgs[1])); // output

            if (userPairJob.waitForCompletion(true)) {
                System.out.println("userPairJob job success.");
            } else {
                System.out.println("userPairJob job failed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class UserPairMapper extends Mapper<LongWritable, Text, UserPairWritable, CommonPrefWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineTokens = line.split(":");

            if (lineTokens.length != 3) {
                return;
            }

            // parse two user id list
            List<UserInfoWritable> likeUserList = new ArrayList<UserInfoWritable>();
            List<UserInfoWritable> unlikeUserList = new ArrayList<UserInfoWritable>();
            for (int i = 1; i <= 2; i++) {
                String[] likeTokens = lineTokens[i].split("-");
                for (String likeToken : likeTokens) {
                    if (likeToken.length() == 0) {
                        continue;
                    }

                    String[] pairToken = likeToken.split(",");
                    if (pairToken.length != 2) {
                        continue;
                    }

                    LongWritable userId = new LongWritable(Long.valueOf(pairToken[0]));
                    LongWritable ratingCount = new LongWritable(Long.valueOf(pairToken[1]));
                    UserInfoWritable userInfo = new UserInfoWritable(userId, ratingCount);

                    if (i == 1) {
                        likeUserList.add(userInfo);
                    } else {
                        unlikeUserList.add(userInfo);
                    }
                }
            }

            // start to emit
            UserInfoWritable curUser;
            UserInfoWritable otherUser;
            for (int i = 0; i < likeUserList.size(); i++) {
                curUser = likeUserList.get(i);
                for (int j = i + 1; j < likeUserList.size(); j++) {
                    otherUser = likeUserList.get(j);
                    if (curUser.userId.get() == otherUser.userId.get()) {
                        continue;
                    }

                    UserPairWritable userPair = new UserPairWritable(curUser, otherUser);
                    CommonPrefWritable commonPref = new CommonPrefWritable(1, 0);
                    context.write(userPair, commonPref);
                }
            }

            for (int i = 0; i < unlikeUserList.size(); i++) {
                curUser = unlikeUserList.get(i);
                for (int j = i + 1; j < unlikeUserList.size(); j++) {
                    otherUser = unlikeUserList.get(j);
                    if (curUser.userId.get() == otherUser.userId.get()) {
                        continue;
                    }

                    UserPairWritable userPair = new UserPairWritable(curUser, otherUser);
                    CommonPrefWritable commonPref = new CommonPrefWritable(0, 1);
                    context.write(userPair, commonPref);
                }
            }
        }
    }

    private static class UserPairReducer extends Reducer<UserPairWritable, CommonPrefWritable, UserPairWritable, DoubleWritable> {
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
